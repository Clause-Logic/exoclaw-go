package conversationfile

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Clause-Logic/exoclaw-go/conversation-file/session"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/bus"
	coreconv "github.com/Clause-Logic/exoclaw-go/exoclaw/conversation"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/providers"
)

// Ported from exoclaw_conversation/conversation.py.
//
// DefaultConversation — file-backed implementation of the
// conversation.Conversation protocol from the core. Composes
// HistoryStore + MemoryBackend + PromptBuilder + ConsolidationPolicy.

const toolResultMaxChars = 500

// noopPolicy is the inline policy used when a caller doesn't configure
// one. Streams the session log unchanged and runs no consolidation.
type noopPolicy struct{}

func (noopPolicy) Transform(ctx context.Context, reader SessionReader, _ int) <-chan StreamMessage {
	return reader.Stream(ctx, 0, -1)
}
func (noopPolicy) OnTurnComplete(_ context.Context, _ SessionReader) error { return nil }

// DefaultConversation is the file-backed conversation state manager.
//
// Implements the conversation.Conversation protocol without inheriting
// from any exoclaw type.
//
// Accepts HistoryStore, MemoryBackend, PromptBuilder, and
// ConsolidationPolicy as constructor arguments so each layer can be
// replaced independently.
//
// The session log is append-only — this type never rewrites or truncates
// message data on disk. The ConsolidationPolicy owns the *view* the LLM
// sees: it transforms a streaming reader over the log into the message
// list, persisting its own state in a sidecar next to the session file.
type DefaultConversation struct {
	History       HistoryStore
	Memory        MemoryBackend
	Prompt        PromptBuilder
	MemoryWindow  int

	policy ConsolidationPolicy
	bus    bus.Bus
	log    *slog.Logger

	mu              sync.Mutex
	consolidating   map[string]struct{}
	consolidationWg sync.WaitGroup

	// Turn context set by BuildPrompt, read by Record for hook firing.
	turnChannel   string
	turnChatID    string
	turnSessionID string
}

// DefaultConversationOptions bundles construction options for
// NewDefaultConversation.
type DefaultConversationOptions struct {
	MemoryWindow        int
	ConsolidationPolicy ConsolidationPolicy
	Bus                 bus.Bus
	Log                 *slog.Logger
}

// NewDefaultConversation composes a DefaultConversation from the given
// pluggable layers. If ConsolidationPolicy is nil, a no-op pass-through
// policy is used.
func NewDefaultConversation(history HistoryStore, memory MemoryBackend, prompt PromptBuilder, opts DefaultConversationOptions) *DefaultConversation {
	mw := opts.MemoryWindow
	if mw <= 0 {
		mw = 100
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	policy := opts.ConsolidationPolicy
	if policy == nil {
		policy = noopPolicy{}
	}
	return &DefaultConversation{
		History:       history,
		Memory:        memory,
		Prompt:        prompt,
		MemoryWindow:  mw,
		policy:        policy,
		bus:           opts.Bus,
		log:           log,
		consolidating: map[string]struct{}{},
	}
}

// CreateOptions bundles the optional inputs to Create.
type CreateOptions struct {
	MemoryWindow        int
	SkillPackages       map[string]string
	ConsolidationPolicy ConsolidationPolicy
	BuiltinSkillsDir    string
	AllowedSkills       []string
	StreamingHistory    bool
	Bus                 bus.Bus
	Log                 *slog.Logger
}

// Create constructs a DefaultConversation with the standard file-backed
// implementations.
//
// BuiltinSkillsDir is the deployment-bundled skills root — intrinsic-to-
// this-deployment skills that ship with the binary and sit alongside
// workspace (agent-managed) skills. Leave empty to expose only workspace
// + entry-point package skills.
//
// AllowedSkills, when non-nil, forwards to SkillsLoader.AllowedNames to
// restrict the visible surface to a known whitelist.
func Create(workspace string, provider providers.LLMProvider, model string, opts CreateOptions) (*DefaultConversation, error) {
	mem, err := NewMemoryStore(workspace, provider, model, opts.Log)
	if err != nil {
		return nil, err
	}
	mgr, err := session.NewSessionManager(workspace, opts.StreamingHistory, opts.Log)
	if err != nil {
		return nil, err
	}
	skills := NewSkillsLoader(workspace, SkillsLoaderOptions{
		BuiltinSkillsDir: opts.BuiltinSkillsDir,
		PackageSkills:    opts.SkillPackages,
		AllowedNames:     opts.AllowedSkills,
	})
	ctx := NewContextBuilder(workspace, mem, skills, 0)
	return NewDefaultConversation(
		&sessionStoreAdapter{mgr: mgr},
		mem,
		ctx,
		DefaultConversationOptions{
			MemoryWindow:        opts.MemoryWindow,
			ConsolidationPolicy: opts.ConsolidationPolicy,
			Bus:                 opts.Bus,
			Log:                 opts.Log,
		},
	), nil
}

// BuildPrompt returns the full messages list to send to the LLM.
//
// Signature matches the core conversation.Conversation interface; the
// extended fields the Python original passed via **kwargs (Skills,
// Isolated, TurnContext) are read out of opts.Extra:
//
//   - "skills"       — []string of skill names
//   - "isolated"     — bool (must actually be a bool, not a stringly value)
//   - "turn_context" — []string of turn-volatile context lines
func (c *DefaultConversation) BuildPrompt(ctx context.Context, sessionID, message string, opts coreconv.BuildPromptOptions) ([]map[string]any, error) {
	c.mu.Lock()
	c.turnChannel = opts.Channel
	c.turnChatID = opts.ChatID
	c.turnSessionID = sessionID
	consolidating := false
	if _, ok := c.consolidating[sessionID]; ok {
		consolidating = true
	}
	c.mu.Unlock()

	skills := extraStringSlice(opts.Extra, "skills")
	isolated := false
	if v, ok := opts.Extra["isolated"].(bool); ok {
		isolated = v
	}
	turnContext := extraStringSlice(opts.Extra, "turn_context")

	c.log.Info("build_prompt",
		"memory.window", c.MemoryWindow,
		"consolidation.active", consolidating,
		"skill.requested", strings.Join(skills, ","),
		"hook.active", opts.Channel == "hook",
		"isolated", isolated,
	)

	var history []map[string]any
	if !isolated {
		reader := c.History.Reader(sessionID)
		for item := range c.policy.Transform(ctx, reader, 0) {
			if item.Err != nil {
				return nil, item.Err
			}
			history = append(history, item.Message)
		}
		history = session.RepairAndProject(history)
	}

	extraContext := ""
	if len(opts.PluginContext) > 0 {
		extraContext = strings.Join(opts.PluginContext, "\n\n")
	}

	if len(turnContext) == 0 {
		turnContext = nil
	}

	messages := c.Prompt.BuildMessages(history, message, BuildMessagesOptions{
		SkillNames:   skills,
		Media:        opts.Media,
		Channel:      opts.Channel,
		ChatID:       opts.ChatID,
		ExtraContext: extraContext,
		TurnContext:  turnContext,
		Isolated:     isolated,
	})

	return messages, nil
}

// extraStringSlice pulls a []string out of opts.Extra. Accepts either a
// raw []string or a []any whose entries are strings (the agent loop
// passes plain Go types, but other entry points may marshal through any).
func extraStringSlice(extra map[string]any, key string) []string {
	if extra == nil {
		return nil
	}
	switch v := extra[key].(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// Append persists a single message as it is produced during a turn.
//
// Implements AppendableConversation from the core. The agent loop calls
// this after each assistant response, tool result, and the incoming user
// message. PrepareTurn runs per-message (tool-result truncation,
// runtime-context tag stripping, etc.).
//
// No hooks fire here — PostTurn owns the end-of-turn hook trigger so
// consolidation / agent_end callbacks only run once per turn, not per
// message.
func (c *DefaultConversation) Append(ctx context.Context, sessionID string, message map[string]any) error {
	sess := c.History.GetOrCreate(sessionID)
	prepared := c.prepareTurn(sess, []map[string]any{message})
	if len(prepared) == 0 {
		return nil
	}
	return c.History.SaveAppend(sess, prepared)
}

// PostTurnOptions bundles the optional inputs to PostTurn.
type PostTurnOptions struct {
	Channel          string
	ChatID           string
	AwaitMaintenance bool
}

// PostTurn is the end-of-turn callback: schedule policy maintenance and
// fire agent_end hooks.
//
// By default policy.OnTurnComplete runs in a background goroutine. Hook
// turns (channel="hook") skip both maintenance and hook firing to prevent
// recursion.
func (c *DefaultConversation) PostTurn(ctx context.Context, sessionID string) error {
	return c.PostTurnWith(ctx, sessionID, PostTurnOptions{})
}

// PostTurnWith is the option-bearing variant of PostTurn.
func (c *DefaultConversation) PostTurnWith(ctx context.Context, sessionID string, opts PostTurnOptions) error {
	effectiveChannel := opts.Channel
	if effectiveChannel == "" {
		effectiveChannel = c.turnChannel
	}
	if effectiveChannel == "hook" {
		return nil
	}

	if opts.AwaitMaintenance {
		c.runMaintenance(ctx, sessionID)
	} else {
		c.scheduleMaintenance(sessionID)
	}

	if c.bus != nil {
		if err := c.fireAgentHooks(ctx, sessionID, opts.ChatID); err != nil {
			return err
		}
	}
	return nil
}

// Record is the legacy end-of-turn batch path. The agent loop calls this
// only when the Conversation doesn't satisfy AppendableConversation
// (which DefaultConversation does — so under a current-version agent
// loop this method isn't called during normal turns).
func (c *DefaultConversation) Record(ctx context.Context, sessionID string, newMessages []map[string]any) error {
	return c.RecordWith(ctx, sessionID, newMessages, PostTurnOptions{})
}

// RecordWith is the option-bearing variant of Record.
func (c *DefaultConversation) RecordWith(ctx context.Context, sessionID string, newMessages []map[string]any, opts PostTurnOptions) error {
	sess := c.History.GetOrCreate(sessionID)
	prepared := c.prepareTurn(sess, newMessages)
	if err := c.History.SaveAppend(sess, prepared); err != nil {
		return err
	}

	effectiveChannel := opts.Channel
	if effectiveChannel == "" {
		effectiveChannel = c.turnChannel
	}
	if effectiveChannel != "hook" {
		if opts.AwaitMaintenance {
			c.runMaintenance(ctx, sessionID)
		} else {
			c.scheduleMaintenance(sessionID)
		}
		if c.bus != nil {
			if err := c.fireAgentHooks(ctx, sessionID, opts.ChatID); err != nil {
				return err
			}
		}
	}
	return nil
}

// Clear resets the session log to a metadata-only header and removes the
// policy sidecar. Returns true on success.
//
// Note: the JSONL file is rewritten (not unlinked) — file-backed
// HistoryStore implementations preserve the file so ListSessions still
// surfaces the empty session.
//
// No automatic archival — if a caller wants the session summarised to
// long-term memory before it disappears, they must drive that explicitly
// via the MemoryBackend before Clear.
func (c *DefaultConversation) Clear(ctx context.Context, sessionID string) (bool, error) {
	sess := c.History.GetOrCreate(sessionID)
	sess.(clearable).Clear()
	if err := c.History.Save(sess); err != nil {
		c.log.Error("session_clear_failed", "session.id", sessionID, "err", err)
		return false, err
	}
	c.History.Invalidate(sessionID)
	if adapter, ok := c.History.(*sessionStoreAdapter); ok {
		DeleteState(adapter.mgr.SessionsDir, sessionID, c.log)
	}
	return true, nil
}

// ListSessions returns metadata for all known sessions.
func (c *DefaultConversation) ListSessions() []map[string]any {
	return c.History.ListSessions()
}

// ActiveTools returns optional tool names activated by the current turn's
// skills. Implements the conversation.ActiveToolsProvider opt-in
// capability that the agent loop checks at runtime.
func (c *DefaultConversation) ActiveTools() map[string]struct{} {
	return c.Prompt.GetActiveOptionalTools()
}

// RecoverFromOverflow is the reactive overflow-recovery seam consumed by
// AgentLoop (via Executor.RecoverFromOverflow) on
// ContextWindowExceededError.
//
// Asks the consolidation policy to advance its sidecar by one chunk,
// then re-assembles the prompt from the post-recovery view. Returns the
// new message list (caller passes to executor.SetMessages and retries)
// or nil when the policy can't make progress.
//
// The returned list is [system_prompt, *recovered_view] — no new user
// message is appended. The in-flight turn's messages are already in the
// active log and surface naturally through the policy's transform.
func (c *DefaultConversation) RecoverFromOverflow(ctx context.Context, sessionID string) ([]map[string]any, error) {
	recoverable, ok := c.policy.(interface {
		RecoverFromOverflow(ctx context.Context, reader SessionReader) (bool, error)
	})
	if !ok {
		return nil, nil
	}
	reader := c.History.Reader(sessionID)
	advanced, err := recoverable.RecoverFromOverflow(ctx, reader)
	if err != nil {
		return nil, err
	}
	if !advanced {
		return nil, nil
	}

	var recovered []map[string]any
	for item := range c.policy.Transform(ctx, reader, 0) {
		if item.Err != nil {
			return nil, item.Err
		}
		recovered = append(recovered, item.Message)
	}
	recovered = session.RepairAndProject(recovered)

	ctxBuilder, ok := c.Prompt.(*ContextBuilder)
	if !ok {
		return recovered, nil
	}
	sysContent := ctxBuilder.BuildSystemPrompt(BuildSystemPromptOptions{})
	out := append([]map[string]any{{"role": "system", "content": sysContent}}, recovered...)
	return out, nil
}

// ─── Internal: maintenance + hook plumbing ───

func (c *DefaultConversation) runMaintenance(ctx context.Context, sessionID string) {
	c.mu.Lock()
	if _, in := c.consolidating[sessionID]; in {
		c.mu.Unlock()
		return
	}
	c.consolidating[sessionID] = struct{}{}
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.consolidating, sessionID)
		c.mu.Unlock()
	}()

	reader := c.History.Reader(sessionID)
	if err := c.policy.OnTurnComplete(ctx, reader); err != nil {
		c.log.Error("policy_on_turn_complete_failed", "session.id", sessionID, "err", err)
	}
}

// scheduleMaintenance spawns a background goroutine that runs
// runMaintenance. Used by the default PostTurn path. Fast-paths repeated
// calls for the same session.
func (c *DefaultConversation) scheduleMaintenance(sessionID string) {
	c.mu.Lock()
	if _, in := c.consolidating[sessionID]; in {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	c.consolidationWg.Add(1)
	go func() {
		defer c.consolidationWg.Done()
		c.runMaintenance(context.Background(), sessionID)
	}()
}

// WaitForMaintenance blocks until all in-flight background maintenance
// goroutines complete. Useful for tests and shutdown paths.
func (c *DefaultConversation) WaitForMaintenance() {
	c.consolidationWg.Wait()
}

func (c *DefaultConversation) fireAgentHooks(ctx context.Context, sessionID, chatIDOverride string) error {
	ctxBuilder, ok := c.Prompt.(*ContextBuilder)
	if !ok {
		return nil
	}
	loader, ok := ctxBuilder.Skills.(*SkillsLoader)
	if !ok {
		return nil
	}
	hooks := loader.GetAgentHooks("agent_end")
	if len(hooks) == 0 {
		return nil
	}
	chatID := chatIDOverride
	if chatID == "" {
		chatID = c.turnChatID
	}
	if chatID == "" {
		chatID = sessionID
	}
	for _, hook := range hooks {
		msg := &bus.InboundMessage{
			Channel:   "hook",
			SenderID:  "hook:" + hook.SkillName + ":agent_end",
			ChatID:    chatID,
			Content:   hook.Prompt,
			Timestamp: time.Now(),
			Metadata: map[string]any{
				"_hook_turn":        true,
				"hook_name":         "agent_end",
				"hook_skill":        hook.SkillName,
				"hook_tools":        hook.Tools,
				"hook_skills":       hook.Skills,
				"source_session_id": sessionID,
			},
		}
		if err := c.bus.PublishInbound(ctx, msg); err != nil {
			c.log.Warn("agent_hook_publish_failed", "hook_skill", hook.SkillName, "err", err)
		}
	}
	return nil
}

// prepareTurn prepares turn messages, truncating large tool results and
// stripping runtime-context tags. Returns the list of prepared entries for
// disk persistence. Does not mutate session.MessagesSlice — the session
// log is append-only on disk; in-memory caching is a back-compat artefact
// maintained by SessionManager for non-streaming deployments.
func (c *DefaultConversation) prepareTurn(sess Session, messages []map[string]any) []map[string]any {
	prepared := make([]map[string]any, 0, len(messages))

	for _, m := range messages {
		entry := make(map[string]any, len(m))
		for k, v := range m {
			entry[k] = v
		}
		role, _ := entry["role"].(string)
		content := entry["content"]

		// Skip empty assistant messages.
		if role == "assistant" {
			contentStr, _ := content.(string)
			tcs, _ := entry["tool_calls"].([]any)
			if contentStr == "" && len(tcs) == 0 {
				continue
			}
		}

		// Tool result truncation.
		if role == "tool" {
			if s, ok := content.(string); ok && len(s) > toolResultMaxChars {
				entry["content"] = s[:toolResultMaxChars] + "\n... (truncated)"
			}
		}

		// User runtime-context stripping.
		if role == "user" {
			switch cv := content.(type) {
			case string:
				if strings.HasPrefix(cv, runtimeContextTag) {
					parts := strings.SplitN(cv, "\n\n", 2)
					if len(parts) > 1 && strings.TrimSpace(parts[1]) != "" {
						entry["content"] = parts[1]
					} else {
						continue
					}
				}
			case []any:
				var filtered []any
				for _, item := range cv {
					im, ok := item.(map[string]any)
					if !ok {
						filtered = append(filtered, item)
						continue
					}
					t, _ := im["type"].(string)
					if t == "text" {
						if text, _ := im["text"].(string); strings.HasPrefix(text, runtimeContextTag) {
							continue
						}
					}
					if t == "image_url" {
						if iu, ok := im["image_url"].(map[string]any); ok {
							if url, _ := iu["url"].(string); strings.HasPrefix(url, "data:image/") {
								filtered = append(filtered, map[string]any{"type": "text", "text": "[image]"})
								continue
							}
						}
					}
					filtered = append(filtered, item)
				}
				if len(filtered) == 0 {
					continue
				}
				entry["content"] = filtered
			}
		}

		if _, ok := entry["timestamp"]; !ok {
			entry["timestamp"] = time.Now().Format(time.RFC3339)
		}
		prepared = append(prepared, entry)
	}
	return prepared
}

// ─── HistoryStore adapter for *session.SessionManager ───

type clearable interface{ Clear() }

// sessionStoreAdapter wraps *session.SessionManager to satisfy HistoryStore.
type sessionStoreAdapter struct {
	mgr *session.SessionManager
}

func (a *sessionStoreAdapter) GetOrCreate(key string) Session {
	return a.mgr.GetOrCreate(key)
}

func (a *sessionStoreAdapter) Save(s Session) error {
	sess, ok := s.(*session.Session)
	if !ok {
		return errors.New("Save: session is not *session.Session")
	}
	return a.mgr.Save(sess)
}

func (a *sessionStoreAdapter) Invalidate(key string) { a.mgr.Invalidate(key) }

func (a *sessionStoreAdapter) ListSessions() []map[string]any { return a.mgr.ListSessions() }

func (a *sessionStoreAdapter) SaveAppend(s Session, msgs []map[string]any) error {
	sess, ok := s.(*session.Session)
	if !ok {
		return errors.New("SaveAppend: session is not *session.Session")
	}
	return a.mgr.SaveAppend(sess, msgs)
}

func (a *sessionStoreAdapter) LoadRange(key string, start, end int) ([]map[string]any, error) {
	return a.mgr.LoadRange(key, start, end)
}

func (a *sessionStoreAdapter) Reader(key string) SessionReader {
	return &jsonlReaderAdapter{r: a.mgr.Reader(key)}
}

func (a *sessionStoreAdapter) ReadHistory(key string, maxMessages int) []map[string]any {
	return a.mgr.ReadHistory(key, maxMessages)
}

// jsonlReaderAdapter wraps *session.JSONLSessionReader to satisfy SessionReader.
type jsonlReaderAdapter struct {
	r *session.JSONLSessionReader
}

func (a *jsonlReaderAdapter) Key() string { return a.r.Key() }
func (a *jsonlReaderAdapter) Count(_ context.Context) (int, error) {
	return a.r.Count()
}
func (a *jsonlReaderAdapter) Stream(ctx context.Context, start, end int) <-chan StreamMessage {
	out := make(chan StreamMessage)
	go func() {
		defer close(out)
		for item := range a.r.Stream(start, end) {
			select {
			case out <- StreamMessage{Message: item.Message, Err: item.Err}:
				if item.Err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
func (a *jsonlReaderAdapter) At(_ context.Context, index int) (map[string]any, error) {
	return a.r.At(index)
}

