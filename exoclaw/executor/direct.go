package executor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/standd/exoclaw-go/exoclaw/agent/tools"
	"github.com/standd/exoclaw-go/exoclaw/conversation"
	"github.com/standd/exoclaw-go/exoclaw/providers"
)

// turnStateKey is the context key under which per-turn message buffer state
// is bound. Equivalent to Python's per-task ContextVar on the executor — by
// putting state on the context, concurrent turns under the same executor
// instance don't trample each other.
type turnStateKey struct{}

// turnState holds the per-turn buffer for a DirectExecutor.
type turnState struct {
	mu     sync.Mutex
	prior  PriorSource
	delta  []map[string]any
	// scratch tracks scratch-file paths written by streaming tool results.
	// Cleaned up in PostTurn.
	scratch []string
}

func emptyPriorSource() []map[string]any { return nil }

// WithTurnState returns a context with a fresh per-turn state bound.
// AgentLoop calls this at the top of each turn so calls into the executor
// during the turn share the same buffer.
func WithTurnState(ctx context.Context) context.Context {
	return context.WithValue(ctx, turnStateKey{}, &turnState{prior: emptyPriorSource})
}

func stateFromContext(ctx context.Context) *turnState {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(turnStateKey{}).(*turnState)
	return s
}

// DirectExecutor is the pass-through executor — calls everything inline.
//
// The per-turn message buffer is split into prior (from build_prompt;
// read-only mid-turn, replaced on compaction) and delta (produced during the
// turn). LoadMessages concatenates prior + delta for the LLM request body.
//
// Concurrency: per-turn state lives on the context, not the executor, so
// concurrent turns each see their own buffer.
type DirectExecutor struct {
	// stateFallback is used by callers that ignore the context-bound state
	// entirely (typically simple unit tests). Keep it nil in production.
	stateFallback *turnState
	stateMu       sync.Mutex

	// uuid7 high-water mark + lock.
	uuidMu     sync.Mutex
	uuidLastMS int64
}

// NewDirectExecutor constructs a fresh DirectExecutor.
func NewDirectExecutor() *DirectExecutor { return &DirectExecutor{} }

func (e *DirectExecutor) HandlesResponseSend() bool   { return false }
func (e *DirectExecutor) HandlesInboundEnqueue() bool { return false }

func (e *DirectExecutor) MonotonicMS() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

// state returns the per-turn buffer for ctx, falling back to the
// executor-scoped buffer (allocated lazily) for callers that didn't seed a
// turn state. Production code goes through WithTurnState; the fallback is a
// convenience for tests that don't care about isolation.
func (e *DirectExecutor) state(ctx context.Context) *turnState {
	if s := stateFromContext(ctx); s != nil {
		return s
	}
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	if e.stateFallback == nil {
		e.stateFallback = &turnState{prior: emptyPriorSource}
	}
	return e.stateFallback
}

// AppendMessages buffers new messages on the per-turn delta.
//
// Implementation note: AppendMessages on the Executor interface is
// context-less for back-compat with the Python signature. In Go we cheat
// by using the fallback buffer when no turn state is bound — production
// callers thread context via the methods that take it (Chat, ExecuteTool,
// etc.).
func (e *DirectExecutor) AppendMessages(messages []map[string]any) {
	s := e.state(context.Background())
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delta = append(s.delta, messages...)
}

// AppendMessagesCtx is the context-aware variant used internally so peers
// see the same buffer as the caller.
func (e *DirectExecutor) AppendMessagesCtx(ctx context.Context, messages []map[string]any) {
	s := e.state(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delta = append(s.delta, messages...)
}

// LoadMessages concatenates prior + delta into a fresh slice the caller owns.
func (e *DirectExecutor) LoadMessages() []map[string]any {
	return e.LoadMessagesCtx(context.Background())
}

// LoadMessagesCtx is the context-aware variant.
func (e *DirectExecutor) LoadMessagesCtx(ctx context.Context) []map[string]any {
	s := e.state(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	prior := []map[string]any{}
	if s.prior != nil {
		prior = s.prior()
	}
	out := make([]map[string]any, 0, len(prior)+len(s.delta))
	out = append(out, prior...)
	out = append(out, s.delta...)
	return out
}

// SetMessages replaces the prior buffer with a snapshot of messages and
// clears delta.
func (e *DirectExecutor) SetMessages(messages []map[string]any) {
	e.SetMessagesCtx(context.Background(), messages)
}

// SetMessagesCtx is the context-aware variant.
func (e *DirectExecutor) SetMessagesCtx(ctx context.Context, messages []map[string]any) {
	snapshot := make([]map[string]any, len(messages))
	copy(snapshot, messages)
	e.SetPriorSourceCtx(ctx, func() []map[string]any { return snapshot })
}

// SetPriorSource installs a prior-history source. Each LoadMessages call
// invokes source() to materialise the prior list, then concatenates with
// the live delta.
func (e *DirectExecutor) SetPriorSource(source PriorSource) {
	e.SetPriorSourceCtx(context.Background(), source)
}

// SetPriorSourceCtx is the context-aware variant.
func (e *DirectExecutor) SetPriorSourceCtx(ctx context.Context, source PriorSource) {
	s := e.state(ctx)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.prior = source
	s.delta = nil
}

// RecoverFromOverflow forwards to conversation.RecoverFromOverflow when
// the conversation opts in. Returns nil when the conversation can't recover.
func (e *DirectExecutor) RecoverFromOverflow(ctx context.Context, conv conversation.Conversation, sessionID string) ([]map[string]any, error) {
	r, ok := conv.(conversation.OverflowRecoverable)
	if !ok {
		return nil, nil
	}
	return r.RecoverFromOverflow(ctx, sessionID)
}

func (e *DirectExecutor) Chat(ctx context.Context, provider providers.LLMProvider, messages []map[string]any, params ChatParams) (*providers.LLMResponse, error) {
	return provider.Chat(ctx, messages, params)
}

func (e *DirectExecutor) ExecuteTool(ctx context.Context, registry *tools.ToolRegistry, name string, params map[string]any, tctx *tools.ToolContext, _ string) (string, error) {
	return registry.Execute(ctx, name, params, tctx)
}

// ExecuteToolWithHandle is the Step-D opt-in: detect StreamingTool, drain
// chunks into a scratch file as they arrive, and return a file-backed
// ToolResult. Falls back to the inline ExecuteTool path for tools that
// don't opt in.
func (e *DirectExecutor) ExecuteToolWithHandle(ctx context.Context, registry *tools.ToolRegistry, name string, params map[string]any, tctx *tools.ToolContext, toolCallID string) (*ToolResult, error) {
	tool, validated, errStr := registry.StreamDispatch(name, params)
	if errStr != "" {
		return &ToolResult{Content: errStr}, nil
	}
	streamer, ok := tool.(tools.StreamingTool)
	if !ok {
		inline, err := e.ExecuteTool(ctx, registry, name, params, tctx, toolCallID)
		if err != nil {
			return nil, err
		}
		return &ToolResult{Content: inline}, nil
	}

	// Sanitize the tool_call_id before letting it near a filesystem path.
	safe := sanitizeToolCallID(toolCallID)
	suffix := ""
	if safe != "" {
		suffix = "-" + safe
	}

	path := makeScratchPath("exoclaw-tool-", suffix+".txt")

	dispatchCtx := tools.WithDispatchRegistry(ctx, registry)
	ch, err := streamer.ExecuteStreaming(dispatchCtx, validated)
	if err != nil {
		_ = os.Remove(path)
		return &ToolResult{Content: fmt.Sprintf("Error executing %s: %s", name, err.Error())}, nil
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	var (
		bytesWritten   int
		previewBudget  = 256
		previewBuilder strings.Builder
	)
	for chunk := range ch {
		if _, err := f.WriteString(chunk); err != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return nil, err
		}
		bytesWritten += len(chunk)
		if previewBudget > 0 {
			take := len(chunk)
			if take > previewBudget {
				take = previewBudget
			}
			previewBuilder.WriteString(chunk[:take])
			previewBudget -= take
		}
	}
	if err := f.Close(); err != nil {
		return nil, err
	}

	s := e.state(ctx)
	s.mu.Lock()
	s.scratch = append(s.scratch, path)
	s.mu.Unlock()

	preview := previewBuilder.String()
	if bytesWritten > len(preview) {
		preview = fmt.Sprintf("%s…\n[streamed %d bytes to %s]", preview, bytesWritten, filepath.Base(path))
	}
	return &ToolResult{Content: preview, ContentFile: path}, nil
}

func sanitizeToolCallID(id string) string {
	if id == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
		if b.Len() >= 64 {
			break
		}
	}
	return b.String()
}

func makeScratchPath(prefix, suffix string) string {
	dir := os.TempDir()
	buf := make([]byte, 8)
	_, _ = rand.Read(buf)
	name := prefix + hex.EncodeToString(buf) + suffix
	return filepath.Join(dir, name)
}

// BuildPrompt calls conversation.BuildPrompt and wires the lazy-prior
// source when the conversation supports PersistedHistoryLoader.
func (e *DirectExecutor) BuildPrompt(ctx context.Context, conv conversation.Conversation, sessionID, message string, opts conversation.BuildPromptOptions) ([]map[string]any, error) {
	messages, err := conv.BuildPrompt(ctx, sessionID, message, opts)
	if err != nil {
		return nil, err
	}
	if loader, ok := conv.(conversation.PersistedHistoryLoader); ok {
		snapshot := loader.LoadPersistedHistory(sessionID)
		if source := buildLazyPriorSource(messages, snapshot, func() []map[string]any {
			return loader.LoadPersistedHistory(sessionID)
		}); source != nil {
			e.SetPriorSourceCtx(ctx, source)
			return messages, nil
		}
	}
	e.SetMessagesCtx(ctx, messages)
	return messages, nil
}

// buildLazyPriorSource locates historySnapshot inside full as a contiguous
// sublist (dict-equality match), and returns a closure that materialises
// prefix + reloadHistory() + suffix on each call.
//
// Returns nil when no contiguous match is found.
func buildLazyPriorSource(full, historySnapshot []map[string]any, reloadHistory func() []map[string]any) PriorSource {
	if len(historySnapshot) == 0 {
		return nil
	}
	if len(historySnapshot) > len(full) {
		return nil
	}
	first := historySnapshot[0]
	firstIdx := -1
	for i := 0; i <= len(full)-len(historySnapshot); i++ {
		if !reflect.DeepEqual(full[i], first) {
			continue
		}
		match := true
		for j := 1; j < len(historySnapshot); j++ {
			if !reflect.DeepEqual(full[i+j], historySnapshot[j]) {
				match = false
				break
			}
		}
		if match {
			firstIdx = i
			break
		}
	}
	if firstIdx == -1 {
		return nil
	}
	lastIdx := firstIdx + len(historySnapshot) - 1
	prefix := make([]map[string]any, firstIdx)
	copy(prefix, full[:firstIdx])
	suffix := make([]map[string]any, len(full)-lastIdx-1)
	copy(suffix, full[lastIdx+1:])

	return func() []map[string]any {
		hist := reloadHistory()
		out := make([]map[string]any, 0, len(prefix)+len(hist)+len(suffix))
		out = append(out, prefix...)
		out = append(out, hist...)
		out = append(out, suffix...)
		return out
	}
}

func (e *DirectExecutor) AppendMessage(ctx context.Context, conv conversation.Conversation, sessionID string, message map[string]any) error {
	a, ok := conv.(conversation.AppendableConversation)
	if !ok {
		return nil
	}
	return a.Append(ctx, sessionID, message)
}

func (e *DirectExecutor) PostTurn(ctx context.Context, conv conversation.Conversation, sessionID string) error {
	if a, ok := conv.(conversation.AppendableConversation); ok {
		if err := a.PostTurn(ctx, sessionID); err != nil {
			return err
		}
	}
	// Clean up scratch files written during the turn.
	s := stateFromContext(ctx)
	if s == nil {
		return nil
	}
	s.mu.Lock()
	paths := s.scratch
	s.scratch = nil
	s.mu.Unlock()
	for _, p := range paths {
		_ = os.Remove(p)
	}
	return nil
}

func (e *DirectExecutor) Record(ctx context.Context, conv conversation.Conversation, sessionID string, newMessages []map[string]any) error {
	return conv.Record(ctx, sessionID, newMessages)
}

func (e *DirectExecutor) Clear(ctx context.Context, conv conversation.Conversation, sessionID string) (bool, error) {
	return conv.Clear(ctx, sessionID)
}

func (e *DirectExecutor) RunHook(ctx context.Context, fn HookFn, args ...any) (any, error) {
	if fn == nil {
		return nil, nil
	}
	return fn(ctx, args...)
}

// MintTurnID produces an inline uuidv7. RFC 9562. Clamped against a per-process
// non-decreasing high-water mark so successive calls always see a non-decreasing
// ms value — keeps ids monotonic across clock regressions.
func (e *DirectExecutor) MintTurnID(ctx context.Context) (string, error) {
	nowMS := time.Now().UnixNano() / int64(time.Millisecond)
	e.uuidMu.Lock()
	if nowMS <= e.uuidLastMS {
		nowMS = e.uuidLastMS
	}
	e.uuidLastMS = nowMS
	e.uuidMu.Unlock()

	var b [16]byte
	b[0] = byte(nowMS >> 40)
	b[1] = byte(nowMS >> 32)
	b[2] = byte(nowMS >> 24)
	b[3] = byte(nowMS >> 16)
	b[4] = byte(nowMS >> 8)
	b[5] = byte(nowMS)

	var rnd [10]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return "", err
	}
	b[6] = 0x70 | (rnd[0] & 0x0F)
	b[7] = rnd[1]
	b[8] = 0x80 | (rnd[2] & 0x3F)
	copy(b[9:16], rnd[3:10])

	hx := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", hx[0:8], hx[8:12], hx[12:16], hx[16:20], hx[20:32]), nil
}
