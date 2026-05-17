// Package agent contains the AgentLoop — the core processing engine.
//
// Ported from exoclaw/agent/loop.py. The Conversation interface lives in
// exoclaw/conversation to avoid an import cycle with executor.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/standd/exoclaw-go/exoclaw/agent/tools"
	"github.com/standd/exoclaw-go/exoclaw/bus"
	"github.com/standd/exoclaw-go/exoclaw/conversation"
	"github.com/standd/exoclaw-go/exoclaw/executor"
	"github.com/standd/exoclaw-go/exoclaw/iterationpolicy"
	"github.com/standd/exoclaw-go/exoclaw/providers"
)

// AgentLoop is the core processing engine.
//
// It:
//  1. Receives messages from the bus
//  2. Asks the Conversation for the prompt
//  3. Calls the LLM
//  4. Executes tool calls
//  5. Records the turn and sends the response back
type AgentLoop struct {
	Bus              bus.Bus
	Provider         providers.LLMProvider
	Conversation     conversation.Conversation
	Model            string
	MaxIterations    int
	Temperature      float64
	MaxTokens        int
	ReasoningEffort  string
	Tools            *tools.ToolRegistry
	IterationPolicy  iterationpolicy.IterationPolicy
	Executor         executor.Executor
	Log              *slog.Logger

	// Optional lifecycle callbacks — all default to nil.
	OnPreContext     func(ctx context.Context, content, sessionKey, channel, chatID string) (string, error)
	OnPreTool        func(ctx context.Context, name string, params map[string]any, sessionKey string) (string, error)
	OnPostTurn       func(ctx context.Context, newMsgs []map[string]any, sessionKey, channel, chatID string) error
	OnMaxIterations  func(ctx context.Context, sessionKey, channel, chatID string) error
	OnToolCalls      func(ctx context.Context, calls []providers.ToolCallRequest) error
	OnToolResult     func(ctx context.Context, call providers.ToolCallRequest, result string) error
	// OnContextOverflow is the deprecated callback path; prefer
	// Conversation.RecoverFromOverflow.
	OnContextOverflow    func(ctx context.Context, messages []map[string]any) ([]map[string]any, error)
	MaxRecoveryAttempts  int

	extraTools  []tools.Tool

	running       bool
	runMu         sync.Mutex
	activeTasks   map[string][]context.CancelFunc // session_key -> cancellation handles
	tasksMu       sync.Mutex
	processingMu  sync.Mutex
	currentTCtxMu sync.Mutex
	currentTCtx   *tools.ToolContext
}

// Options bundles AgentLoop construction parameters.
type Options struct {
	Bus              bus.Bus
	Provider         providers.LLMProvider
	Conversation     conversation.Conversation
	Model            string
	MaxIterations    int
	Temperature      float64
	MaxTokens        int
	ReasoningEffort  string
	Tools            []tools.Tool
	Registry         *tools.ToolRegistry
	IterationPolicy  iterationpolicy.IterationPolicy
	Executor         executor.Executor
	Log              *slog.Logger

	OnPreContext         func(ctx context.Context, content, sessionKey, channel, chatID string) (string, error)
	OnPreTool            func(ctx context.Context, name string, params map[string]any, sessionKey string) (string, error)
	OnPostTurn           func(ctx context.Context, newMsgs []map[string]any, sessionKey, channel, chatID string) error
	OnMaxIterations      func(ctx context.Context, sessionKey, channel, chatID string) error
	OnToolCalls          func(ctx context.Context, calls []providers.ToolCallRequest) error
	OnToolResult         func(ctx context.Context, call providers.ToolCallRequest, result string) error
	OnContextOverflow    func(ctx context.Context, messages []map[string]any) ([]map[string]any, error)
	MaxRecoveryAttempts  int
}

// New constructs an AgentLoop from Options. Mirrors AgentLoop.__init__ from
// the Python original.
func New(opts Options) *AgentLoop {
	exe := opts.Executor
	if exe == nil {
		exe = executor.NewDirectExecutor()
	}
	model := opts.Model
	if model == "" {
		model = opts.Provider.GetDefaultModel()
	}
	registry := opts.Registry
	if registry == nil {
		registry = tools.NewToolRegistry()
	}
	for _, t := range opts.Tools {
		registry.Register(t, false)
		if ba, ok := t.(tools.BusAware); ok {
			ba.SetBus(opts.Bus)
		}
		if ra, ok := t.(tools.RegistryAware); ok {
			ra.SetRegistry(registry)
		}
	}
	maxIt := opts.MaxIterations
	if maxIt <= 0 {
		maxIt = 40
	}
	maxTok := opts.MaxTokens
	if maxTok <= 0 {
		maxTok = 4096
	}
	temp := opts.Temperature
	if temp == 0 {
		// Match Python default of 0.1 here (the package-level default).
		// The Executor's Chat default is 0.7 — that's intentional.
		temp = 0.1
	}
	maxRec := opts.MaxRecoveryAttempts
	if maxRec < 1 {
		maxRec = 3
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	a := &AgentLoop{
		Bus:                 opts.Bus,
		Provider:            opts.Provider,
		Conversation:        opts.Conversation,
		Model:               model,
		MaxIterations:       maxIt,
		Temperature:         temp,
		MaxTokens:           maxTok,
		ReasoningEffort:     opts.ReasoningEffort,
		Tools:               registry,
		IterationPolicy:     opts.IterationPolicy,
		Executor:            exe,
		Log:                 log,
		OnPreContext:        opts.OnPreContext,
		OnPreTool:           opts.OnPreTool,
		OnPostTurn:          opts.OnPostTurn,
		OnMaxIterations:     opts.OnMaxIterations,
		OnToolCalls:         opts.OnToolCalls,
		OnToolResult:        opts.OnToolResult,
		OnContextOverflow:   opts.OnContextOverflow,
		MaxRecoveryAttempts: maxRec,
		extraTools:          opts.Tools,
		activeTasks:         map[string][]context.CancelFunc{},
	}

	// Durable inbound enqueue handoff.
	if a.Executor.HandlesInboundEnqueue() {
		enqueuer, ok := a.Executor.(executor.InboundEnqueuer)
		hookBus, hookOK := a.Bus.(bus.InboundHookBus)
		var missing []string
		if !ok {
			missing = append(missing, "executor.EnqueueInbound")
		}
		if !hookOK {
			missing = append(missing, "bus.SetInboundHook")
		}
		if len(missing) > 0 {
			panic(fmt.Sprintf("Executor opted into durable inbound enqueue via "+
				"HandlesInboundEnqueue=true, but required wiring is missing: %s",
				strings.Join(missing, ", ")))
		}
		hookBus.SetInboundHook(func(ctx context.Context, msg *bus.InboundMessage) error {
			return enqueuer.EnqueueInbound(ctx, msg)
		})
	}

	return a
}

func (a *AgentLoop) monotonicMS() int64 { return a.Executor.MonotonicMS() }

// notifyToolsInbound lets tools that care about inbound messages update state.
func (a *AgentLoop) notifyToolsInbound(msg *bus.InboundMessage) {
	for _, t := range a.Tools.All() {
		if inbound, ok := t.(tools.InboundAware); ok {
			inbound.OnInbound(msg)
		}
	}
}

// collectPluginContext gathers SystemContext strings from tools that provide them.
func (a *AgentLoop) collectPluginContext() []string {
	var ctx []string
	for _, t := range a.Tools.All() {
		if sysc, ok := t.(tools.SystemContextual); ok {
			func() {
				defer func() {
					if r := recover(); r != nil {
						a.Log.Error("system_context_error", "tool.name", t.Name(), "panic", r)
					}
				}()
				if s := sysc.SystemContext(); s != "" {
					ctx = append(ctx, s)
				}
			}()
		}
	}
	return ctx
}

// invokeTool dispatches a tool, preferring ExecuteToolWithHandle when available.
// Returns (content, content_file_path).
func (a *AgentLoop) invokeTool(ctx context.Context, call providers.ToolCallRequest) (string, string, error) {
	a.currentTCtxMu.Lock()
	tctx := a.currentTCtx
	a.currentTCtxMu.Unlock()
	res, err := a.Executor.ExecuteToolWithHandle(ctx, a.Tools, call.Name, call.Arguments, tctx, call.ID)
	if err != nil {
		// Fall back to ExecuteTool for executors that don't implement the
		// streaming surface meaningfully (or that return a nil result).
		legacy, lerr := a.Executor.ExecuteTool(ctx, a.Tools, call.Name, call.Arguments, tctx, call.ID)
		if lerr != nil {
			return "", "", lerr
		}
		return legacy, "", nil
	}
	if res == nil {
		legacy, lerr := a.Executor.ExecuteTool(ctx, a.Tools, call.Name, call.Arguments, tctx, call.ID)
		if lerr != nil {
			return "", "", lerr
		}
		return legacy, "", nil
	}
	return res.Content, res.ContentFile, nil
}

var thinkRE = regexp.MustCompile(`(?s)<think>.*?</think>`)

func stripThink(text string) string {
	if text == "" {
		return ""
	}
	return strings.TrimSpace(thinkRE.ReplaceAllString(text, ""))
}

func toolHint(calls []providers.ToolCallRequest) string {
	parts := make([]string, 0, len(calls))
	for _, tc := range calls {
		args := tc.Arguments
		var val any
		for _, v := range args {
			val = v
			break
		}
		s, ok := val.(string)
		if !ok {
			parts = append(parts, tc.Name)
			continue
		}
		if len(s) > 40 {
			parts = append(parts, fmt.Sprintf(`%s("%s…")`, tc.Name, s[:40]))
		} else {
			parts = append(parts, fmt.Sprintf(`%s("%s")`, tc.Name, s))
		}
	}
	return strings.Join(parts, ", ")
}

func (a *AgentLoop) shouldContinue(ctx context.Context, iteration int, toolsUsed []string) (bool, error) {
	if a.IterationPolicy != nil {
		return a.IterationPolicy.ShouldContinue(ctx, iteration, toolsUsed)
	}
	return iteration < a.MaxIterations, nil
}

func (a *AgentLoop) buildLimitMessage(ctx context.Context, iteration int, toolsUsed []string) (string, error) {
	if a.IterationPolicy != nil {
		return a.IterationPolicy.OnLimitReached(ctx, iteration, toolsUsed)
	}
	return fmt.Sprintf("I reached the maximum number of tool call iterations (%d) "+
		"without completing the task. You can try breaking the task into smaller steps.",
		a.MaxIterations), nil
}

// progressFn is the callable used for streaming progress updates.
type progressFn = executor.ProgressFn

// runAgentLoop runs the iteration loop. Returns (final_content, tools_used,
// messages).
//
// Does NOT seed the executor with initialMessages — BuildPrompt already did
// that (and may have installed a lazy PriorSource that SetMessages would
// overwrite).
func (a *AgentLoop) runAgentLoop(ctx context.Context, initialMessages []map[string]any, onProgress progressFn, model, sessionID string) (string, []string, []map[string]any, error) {
	iteration := 0
	var finalContent string
	var finalContentSet bool
	var toolsUsed []string
	effectiveModel := model
	if effectiveModel == "" {
		effectiveModel = a.Model
	}
	recoveryAttempts := 0

	preferAppend := sessionID != ""
	if _, ok := a.Conversation.(conversation.AppendableConversation); !ok {
		preferAppend = false
	}

	flush := func(msg map[string]any) error {
		if !preferAppend {
			return nil
		}
		return a.Executor.AppendMessage(ctx, a.Conversation, sessionID, msg)
	}

	for {
		cont, err := a.shouldContinue(ctx, iteration, toolsUsed)
		if err != nil {
			return "", toolsUsed, a.Executor.LoadMessages(), err
		}
		if !cont {
			break
		}
		iteration++

		var include map[string]struct{}
		if ap, ok := a.Conversation.(conversation.ActiveToolsProvider); ok {
			include = ap.ActiveTools()
		}
		messages := a.Executor.LoadMessages()
		response, err := a.Executor.Chat(ctx, a.Provider, messages, providers.ChatParams{
			Tools:           a.Tools.GetDefinitions(include),
			Model:           effectiveModel,
			Temperature:     a.Temperature,
			MaxTokens:       a.MaxTokens,
			ReasoningEffort: a.ReasoningEffort,
		})
		if err != nil {
			var ce *providers.ContextWindowExceededError
			if errors.As(err, &ce) {
				if recoveryAttempts >= a.MaxRecoveryAttempts {
					a.Log.Error("context_overflow_recovery_capped",
						"iteration", iteration,
						"attempts", recoveryAttempts,
						"cap", a.MaxRecoveryAttempts,
					)
					finalContent = "The conversation exceeded the model's context window " +
						"and I couldn't recover. Try starting a new session."
					finalContentSet = true
					break
				}
				recoveryAttempts++

				var compacted []map[string]any
				if a.OnContextOverflow != nil {
					compacted, err = a.OnContextOverflow(ctx, messages)
					if err != nil {
						return "", toolsUsed, a.Executor.LoadMessages(), err
					}
				} else if rec, ok := a.Executor.(executor.OverflowRecoverer); ok && sessionID != "" {
					compacted, err = rec.RecoverFromOverflow(ctx, a.Conversation, sessionID)
					if err != nil {
						return "", toolsUsed, a.Executor.LoadMessages(), err
					}
				}
				if compacted != nil {
					a.Log.Info("context_compact", "iteration", iteration, "attempt", recoveryAttempts)
					a.Executor.SetMessages(compacted)
					continue
				}
				a.Log.Error("context_overflow", "iteration", iteration)
				finalContent = "The conversation exceeded the model's context window " +
					"and I couldn't recover. Try starting a new session."
				finalContentSet = true
				break
			}
			return "", toolsUsed, a.Executor.LoadMessages(), err
		}

		if response.HasToolCalls() {
			if onProgress != nil {
				if response.Content != nil {
					if thought := stripThink(*response.Content); thought != "" {
						_ = onProgress(ctx, thought, false)
					}
				}
				_ = onProgress(ctx, toolHint(response.ToolCalls), true)
			}
			if a.OnToolCalls != nil {
				if _, err := a.Executor.RunHook(ctx, func(_ context.Context, _ ...any) (any, error) {
					return nil, a.OnToolCalls(ctx, response.ToolCalls)
				}); err != nil {
					return "", toolsUsed, a.Executor.LoadMessages(), err
				}
			}

			toolCallDicts := make([]map[string]any, 0, len(response.ToolCalls))
			for _, tc := range response.ToolCalls {
				argsJSON, _ := json.Marshal(tc.Arguments)
				toolCallDicts = append(toolCallDicts, map[string]any{
					"id":   tc.ID,
					"type": "function",
					"function": map[string]any{
						"name":      tc.Name,
						"arguments": string(argsJSON),
					},
				})
			}
			content := ""
			if response.Content != nil {
				content = *response.Content
			}
			msg := map[string]any{"role": "assistant", "content": content}
			if len(toolCallDicts) > 0 {
				msg["tool_calls"] = toolCallDicts
			}
			if response.ReasoningContent != nil {
				msg["reasoning_content"] = *response.ReasoningContent
			}
			if len(response.ThinkingBlocks) > 0 {
				msg["thinking_blocks"] = response.ThinkingBlocks
			}
			a.Executor.AppendMessages([]map[string]any{msg})
			if err := flush(msg); err != nil {
				return "", toolsUsed, a.Executor.LoadMessages(), err
			}

			for _, tc := range response.ToolCalls {
				toolsUsed = append(toolsUsed, tc.Name)
				argsStr, _ := json.Marshal(tc.Arguments)
				argsPreview := string(argsStr)
				if len(argsPreview) > 200 {
					argsPreview = argsPreview[:200]
				}
				a.Log.Info("tool_call",
					"tool.name", tc.Name,
					"tool.call_id", tc.ID,
					"args", argsPreview,
				)
				t0 := a.monotonicMS()
				status := "ok"
				var execErr error
				result := ""
				contentFile := ""

				if a.OnPreTool != nil {
					sk := ""
					a.currentTCtxMu.Lock()
					if a.currentTCtx != nil {
						sk = a.currentTCtx.SessionKey
					}
					a.currentTCtxMu.Unlock()
					rejection, rerr := a.OnPreTool(ctx, tc.Name, tc.Arguments, sk)
					if rerr != nil {
						execErr = rerr
					} else if rejection != "" {
						status = "rejected"
						rejPreview := rejection
						if len(rejPreview) > 100 {
							rejPreview = rejPreview[:100]
						}
						a.Log.Info("tool_reject",
							"tool.name", tc.Name,
							"tool.call_id", tc.ID,
							"reason", rejPreview,
						)
						result = rejection
					} else {
						result, contentFile, execErr = a.invokeTool(ctx, tc)
					}
				} else {
					result, contentFile, execErr = a.invokeTool(ctx, tc)
				}
				if execErr != nil {
					status = "error"
					detail := execErr.Error()
					if detail == "" {
						detail = fmt.Sprintf("%T", execErr)
					}
					result = fmt.Sprintf("Error executing %s: %s\n\n[Analyze the error above and try a different approach.]", tc.Name, detail)
				}

				durationMS := a.monotonicMS() - t0
				logArgs := []any{
					"tool.name", tc.Name,
					"tool.call_id", tc.ID,
					"tool.status", status,
					"tool.duration_ms", durationMS,
				}
				if execErr != nil {
					a.Log.Error("tool_result", append(logArgs, "err", execErr)...)
				} else {
					a.Log.Info("tool_result", logArgs...)
				}

				if a.OnToolResult != nil {
					_ = a.OnToolResult(ctx, tc, result)
				}

				toolMsg := map[string]any{
					"role":         "tool",
					"tool_call_id": tc.ID,
					"name":         tc.Name,
					"content":      result,
				}
				if contentFile != "" {
					toolMsg["_content_file"] = contentFile
				}
				a.Executor.AppendMessages([]map[string]any{toolMsg})

				// Persistence path strips underscore-prefixed transport
				// metadata so tmp paths don't end up in the session JSONL.
				persisted := map[string]any{}
				for k, v := range toolMsg {
					if !strings.HasPrefix(k, "_") {
						persisted[k] = v
					}
				}
				if err := flush(persisted); err != nil {
					return "", toolsUsed, a.Executor.LoadMessages(), err
				}
			}
		} else {
			contentPtr := response.Content
			content := ""
			if contentPtr != nil {
				content = *contentPtr
			}
			clean := stripThink(content)
			if response.FinishReason == "error" {
				preview := clean
				if len(preview) > 200 {
					preview = preview[:200]
				}
				a.Log.Error("llm_error", "error.message", preview)
				if clean != "" {
					finalContent = clean
				} else {
					finalContent = "Sorry, I encountered an error calling the AI model."
				}
				finalContentSet = true
				break
			}
			msg2 := map[string]any{"role": "assistant", "content": clean}
			if response.ReasoningContent != nil {
				msg2["reasoning_content"] = *response.ReasoningContent
			}
			if len(response.ThinkingBlocks) > 0 {
				msg2["thinking_blocks"] = response.ThinkingBlocks
			}
			a.Executor.AppendMessages([]map[string]any{msg2})
			if err := flush(msg2); err != nil {
				return "", toolsUsed, a.Executor.LoadMessages(), err
			}
			finalContent = clean
			finalContentSet = true
			break
		}
	}

	if !finalContentSet {
		cont, err := a.shouldContinue(ctx, iteration, toolsUsed)
		if err != nil {
			return "", toolsUsed, a.Executor.LoadMessages(), err
		}
		if !cont {
			a.Log.Warn("iteration_limit", "iteration.max", a.MaxIterations)
			finalContent, err = a.buildLimitMessage(ctx, iteration, toolsUsed)
			if err != nil {
				return "", toolsUsed, a.Executor.LoadMessages(), err
			}
			if a.OnMaxIterations != nil {
				a.currentTCtxMu.Lock()
				tctx := a.currentTCtx
				a.currentTCtxMu.Unlock()
				if tctx != nil {
					go func() {
						_ = a.OnMaxIterations(context.Background(), tctx.SessionKey, tctx.Channel, tctx.ChatID)
					}()
				}
			}
		}
	}

	return finalContent, toolsUsed, a.Executor.LoadMessages(), nil
}

// ProcessTurn executes a single turn: build prompt, run agent loop, record.
//
// If the executor provides RunTurner, the call is delegated to it (durable
// executors). Otherwise the turn runs inline.
func (a *AgentLoop) ProcessTurn(ctx context.Context, sessionID, message string, opts executor.RunTurnOptions) (string, []map[string]any, error) {
	if runner, ok := a.Executor.(executor.RunTurner); ok {
		res, err := runner.RunTurn(ctx, a, sessionID, message, opts)
		if err != nil {
			return "", nil, err
		}
		if res != nil {
			return res.FinalContent, res.NewMessages, nil
		}
	}
	return a.processTurnInline(ctx, sessionID, message, opts)
}

func (a *AgentLoop) processTurnInline(ctx context.Context, sessionID, message string, opts executor.RunTurnOptions) (string, []map[string]any, error) {
	ctx = executor.WithTurnState(ctx)
	turnID, err := a.Executor.MintTurnID(ctx)
	if err != nil {
		return "", nil, err
	}
	a.Log.Info("turn_start", "turn.id", turnID)
	turnStart := a.monotonicMS()
	defer func() {
		a.Log.Info("turn_end",
			"turn.id", turnID,
			"turn.duration_ms", a.monotonicMS()-turnStart,
		)
	}()

	bpOpts := conversation.BuildPromptOptions{
		Channel:       opts.Channel,
		ChatID:        opts.ChatID,
		Media:         opts.Media,
		PluginContext: opts.PluginContext,
		Extra:         opts.Extra,
	}
	initial, err := a.Executor.BuildPrompt(ctx, a.Conversation, sessionID, message, bpOpts)
	if err != nil {
		return "", nil, err
	}

	_, appendable := a.Conversation.(conversation.AppendableConversation)
	if appendable && len(initial) > 0 {
		if err := a.Executor.AppendMessage(ctx, a.Conversation, sessionID, initial[len(initial)-1]); err != nil {
			return "", nil, err
		}
	}

	finalContent, _, allMsgs, err := a.runAgentLoop(ctx, initial, opts.OnProgress, opts.Model, sessionID)
	if err != nil {
		return "", nil, err
	}

	var newMsgs []map[string]any
	if len(initial) > 0 {
		newMsgs = allMsgs[len(initial)-1:]
	} else {
		newMsgs = allMsgs
	}

	if appendable {
		if err := a.Executor.PostTurn(ctx, a.Conversation, sessionID); err != nil {
			return "", nil, err
		}
	} else {
		if err := a.Executor.Record(ctx, a.Conversation, sessionID, newMsgs); err != nil {
			return "", nil, err
		}
	}

	return finalContent, newMsgs, nil
}

// Run dispatches inbound messages until ctx is cancelled or Stop is called.
func (a *AgentLoop) Run(ctx context.Context) error {
	a.runMu.Lock()
	a.running = true
	a.runMu.Unlock()
	a.Log.Info("agent_loop_start")

	defer func() {
		a.tasksMu.Lock()
		all := a.activeTasks
		a.activeTasks = map[string][]context.CancelFunc{}
		a.tasksMu.Unlock()
		for _, cancels := range all {
			for _, c := range cancels {
				c()
			}
		}
		a.Log.Info("agent_loop_stop")
	}()

	for {
		a.runMu.Lock()
		running := a.running
		a.runMu.Unlock()
		if !running {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		consumeCtx, cancel := context.WithTimeout(ctx, time.Second)
		msg, err := a.Bus.ConsumeInbound(consumeCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			if errors.Is(err, context.Canceled) {
				return nil
			}
			a.Log.Error("consume_inbound_error", "err", err)
			continue
		}

		if strings.EqualFold(strings.TrimSpace(msg.Content), "/stop") {
			a.handleStop(ctx, msg)
			continue
		}

		taskCtx, taskCancel := context.WithCancel(ctx)
		a.tasksMu.Lock()
		a.activeTasks[msg.SessionKey()] = append(a.activeTasks[msg.SessionKey()], taskCancel)
		a.tasksMu.Unlock()

		go func(m *bus.InboundMessage, cancel context.CancelFunc) {
			defer cancel()
			a.dispatch(taskCtx, m)
			a.tasksMu.Lock()
			defer a.tasksMu.Unlock()
			sk := m.SessionKey()
			tasks := a.activeTasks[sk]
			for i, c := range tasks {
				// Reflexively remove our own cancel from the slice.
				if &c == &taskCancel {
					a.activeTasks[sk] = append(tasks[:i], tasks[i+1:]...)
					break
				}
			}
		}(msg, taskCancel)
	}
}

func (a *AgentLoop) handleStop(ctx context.Context, msg *bus.InboundMessage) {
	sk := msg.SessionKey()
	a.tasksMu.Lock()
	cancels := a.activeTasks[sk]
	delete(a.activeTasks, sk)
	a.tasksMu.Unlock()

	cancelled := 0
	for _, c := range cancels {
		c()
		cancelled++
	}
	subCancelled := 0
	for _, t := range a.Tools.All() {
		if sc, ok := t.(tools.SessionCancellable); ok {
			subCancelled += sc.CancelBySession(sk)
		}
	}
	total := cancelled + subCancelled
	content := "No active task to stop."
	if total > 0 {
		content = fmt.Sprintf("⏹ Stopped %d task(s).", total)
	}
	_ = a.Bus.PublishOutbound(ctx, &bus.OutboundMessage{
		Channel: msg.Channel,
		ChatID:  msg.ChatID,
		Content: content,
	})
}

func (a *AgentLoop) dispatch(ctx context.Context, msg *bus.InboundMessage) {
	a.processingMu.Lock()
	defer a.processingMu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			a.Log.Error("message_error", "session.key", msg.SessionKey(), "panic", r)
			_ = a.Bus.PublishOutbound(ctx, &bus.OutboundMessage{
				Channel: msg.Channel,
				ChatID:  msg.ChatID,
				Content: "Sorry, I encountered an error.",
			})
		}
	}()
	response, err := a.processMessage(ctx, msg, "", nil, "", true)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			a.Log.Info("task_cancel", "session.key", msg.SessionKey())
			return
		}
		a.Log.Error("message_error", "session.key", msg.SessionKey(), "err", err)
		_ = a.Bus.PublishOutbound(ctx, &bus.OutboundMessage{
			Channel: msg.Channel,
			ChatID:  msg.ChatID,
			Content: "Sorry, I encountered an error.",
		})
		return
	}
	if response != nil {
		_ = a.Bus.PublishOutbound(ctx, response)
		return
	}
	if msg.Channel == "cli" {
		_ = a.Bus.PublishOutbound(ctx, &bus.OutboundMessage{
			Channel:  msg.Channel,
			ChatID:   msg.ChatID,
			Content:  "",
			Metadata: msg.Metadata,
		})
	}
}

// Stop signals the run loop to terminate after the current message.
func (a *AgentLoop) Stop() {
	a.runMu.Lock()
	defer a.runMu.Unlock()
	a.running = false
}

func copyMeta(in map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

// processMessage handles a single inbound message and returns the response.
//
// onProgress is the optional progress callback; pass nil to inherit the
// default bus-based progress publisher. publishResponse forwards to
// ProcessTurn so executors that own the send can take it; when they do,
// processMessage returns (nil, nil).
func (a *AgentLoop) processMessage(ctx context.Context, msg *bus.InboundMessage, sessionKey string, onProgress progressFn, model string, publishResponse bool) (*bus.OutboundMessage, error) {
	// System messages: parse origin from chat_id ("channel:chat_id").
	if msg.Channel == "system" {
		channel := "cli"
		chatID := msg.ChatID
		if idx := strings.Index(msg.ChatID, ":"); idx != -1 {
			channel = msg.ChatID[:idx]
			chatID = msg.ChatID[idx+1:]
		}
		sid := msg.SessionKeyOverride
		if sid == "" {
			sid = channel + ":" + chatID
		}
		a.Log.Info("system_message", "session.key", sid, "channel", channel, "chat.id", chatID, "sender.id", msg.SenderID)
		pluginCtx := a.collectPluginContext()
		effModel := model
		if effModel == "" {
			effModel = msg.ModelOverride
		}
		finalContent, _, err := a.ProcessTurn(ctx, sid, msg.Content, executor.RunTurnOptions{
			Channel:         channel,
			ChatID:          chatID,
			PluginContext:   pluginCtx,
			Model:           effModel,
			PublishResponse: publishResponse,
		})
		if err != nil {
			return nil, err
		}
		if publishResponse && a.Executor.HandlesResponseSend() {
			return nil, nil
		}
		meta := copyMeta(msg.Metadata)
		if _, ok := meta["session_key"]; !ok {
			meta["session_key"] = sid
		}
		content := finalContent
		if content == "" {
			content = "Background task completed."
		}
		return &bus.OutboundMessage{
			Channel:  channel,
			ChatID:   chatID,
			Content:  content,
			Metadata: meta,
		}, nil
	}

	preview := msg.Content
	if len(preview) > 80 {
		preview = preview[:80] + "..."
	}
	sid := sessionKey
	if sid == "" {
		sid = msg.SessionKey()
	}
	a.Log.Info("message_receive",
		"preview", preview,
		"session.key", sid,
		"channel", msg.Channel,
		"chat.id", msg.ChatID,
		"sender.id", msg.SenderID,
	)

	// Slash commands.
	cmd := strings.ToLower(strings.TrimSpace(msg.Content))
	if cmd == "/new" {
		success, err := a.Executor.Clear(ctx, a.Conversation, sid)
		if err != nil {
			return nil, err
		}
		content := "Memory archival failed, session not cleared. Please try again."
		if success {
			content = "New session started."
		}
		return &bus.OutboundMessage{Channel: msg.Channel, ChatID: msg.ChatID, Content: content}, nil
	}

	if cmd == "/help" {
		return &bus.OutboundMessage{
			Channel: msg.Channel,
			ChatID:  msg.ChatID,
			Content: "🦀 exoclaw commands:\n/new — Start a new conversation\n/stop — Stop the current task\n/help — Show available commands",
		}, nil
	}

	a.notifyToolsInbound(msg)
	a.currentTCtxMu.Lock()
	a.currentTCtx = &tools.ToolContext{
		SessionKey: sid,
		Channel:    msg.Channel,
		ChatID:     msg.ChatID,
		Executor:   a.Executor,
	}
	a.currentTCtxMu.Unlock()

	pluginCtx := a.collectPluginContext()
	if a.OnPreContext != nil {
		extra, err := a.OnPreContext(ctx, msg.Content, sid, msg.Channel, msg.ChatID)
		if err != nil {
			return nil, err
		}
		if extra != "" {
			pluginCtx = append(pluginCtx, extra)
		}
	}

	defaultProgress := func(progressCtx context.Context, content string, toolHint bool) error {
		meta := copyMeta(msg.Metadata)
		meta["_progress"] = true
		meta["_tool_hint"] = toolHint
		return a.Bus.PublishOutbound(progressCtx, &bus.OutboundMessage{
			Channel:  msg.Channel,
			ChatID:   msg.ChatID,
			Content:  content,
			Metadata: meta,
		})
	}
	effectiveProgress := onProgress
	if effectiveProgress == nil {
		effectiveProgress = defaultProgress
	}

	effModel := model
	if effModel == "" {
		effModel = msg.ModelOverride
	}

	media := msg.Media
	if len(media) == 0 {
		media = nil
	}
	finalContent, newMsgs, err := a.ProcessTurn(ctx, sid, msg.Content, executor.RunTurnOptions{
		Channel:         msg.Channel,
		ChatID:          msg.ChatID,
		Media:           media,
		PluginContext:   pluginCtx,
		OnProgress:      effectiveProgress,
		Model:           effModel,
		PublishResponse: publishResponse,
	})
	if err != nil {
		return nil, err
	}

	if finalContent == "" {
		finalContent = "I've completed processing but have no response to give."
	}
	if a.OnPostTurn != nil && len(newMsgs) > 0 {
		go func() {
			_ = a.OnPostTurn(context.Background(), newMsgs, sid, msg.Channel, msg.ChatID)
		}()
	}

	if publishResponse && a.Executor.HandlesResponseSend() {
		return nil, nil
	}

	for _, t := range a.Tools.All() {
		if s, ok := t.(tools.SentInTurnTool); ok && s.SentInTurn() {
			return nil, nil
		}
	}

	previewOut := finalContent
	if len(previewOut) > 120 {
		previewOut = previewOut[:120] + "..."
	}
	a.Log.Info("response_send", "preview", previewOut)

	meta := copyMeta(msg.Metadata)
	if _, ok := meta["session_key"]; !ok {
		meta["session_key"] = sid
	}
	return &bus.OutboundMessage{
		Channel:  msg.Channel,
		ChatID:   msg.ChatID,
		Content:  finalContent,
		Metadata: meta,
	}, nil
}

// ProcessDirect processes a message directly (for CLI or cron usage).
//
// model overrides the loop's default for this turn only; pass "" to inherit.
func (a *AgentLoop) ProcessDirect(ctx context.Context, content, sessionKey, channel, chatID string, onProgress progressFn, model string) (string, error) {
	msg := bus.NewInboundMessage(channel, "user", chatID, content)
	resp, err := a.processMessage(ctx, msg, sessionKey, onProgress, model, false)
	if err != nil {
		return "", err
	}
	if resp == nil {
		return "", nil
	}
	return resp.Content, nil
}
