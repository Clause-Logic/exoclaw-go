package executor

import (
	"context"

	"github.com/Clause-Logic/exoclaw-go/exoclaw/agent/tools"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/conversation"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/providers"
)

// ChatParams is the bundle of optional knobs forwarded to the provider.
// Mirrors providers.ChatParams so the executor can decorate without
// re-declaring the surface.
type ChatParams = providers.ChatParams

// Executor is the pluggable execution layer for agent loop I/O.
//
// The default (DirectExecutor) calls everything inline. Alternative
// implementations can wrap each operation differently (different timeout
// policies, retry strategies, replay-safe sandboxes, etc.).
type Executor interface {
	// HandlesResponseSend — when true, the executor takes responsibility
	// for publishing the final turn reply to the bus itself.
	HandlesResponseSend() bool

	// HandlesInboundEnqueue — when true, the executor takes responsibility
	// for persisting each inbound message the moment a channel hands it
	// over. AgentLoop will wire bus.SetInboundHook to forward to
	// EnqueueInbound (which durable executors implement on their concrete
	// type, not on this interface).
	HandlesInboundEnqueue() bool

	Chat(ctx context.Context, provider providers.LLMProvider, messages []map[string]any, params ChatParams) (*providers.LLMResponse, error)

	ExecuteTool(ctx context.Context, registry *tools.ToolRegistry, name string, params map[string]any, tctx *tools.ToolContext, toolCallID string) (string, error)

	// ExecuteToolWithHandle returns a possibly file-backed result. Default
	// implementations may delegate to ExecuteTool and wrap the string in a
	// ToolResult with empty ContentFile.
	ExecuteToolWithHandle(ctx context.Context, registry *tools.ToolRegistry, name string, params map[string]any, tctx *tools.ToolContext, toolCallID string) (*ToolResult, error)

	BuildPrompt(ctx context.Context, conv conversation.Conversation, sessionID, message string, opts conversation.BuildPromptOptions) ([]map[string]any, error)

	// AppendMessage persists a single message mid-turn via the Conversation.
	AppendMessage(ctx context.Context, conv conversation.Conversation, sessionID string, message map[string]any) error

	// PostTurn fires end-of-turn hooks after all messages are persisted.
	PostTurn(ctx context.Context, conv conversation.Conversation, sessionID string) error

	// Record is the deprecated end-of-turn batched-persist path; AgentLoop
	// falls back to this when the conversation doesn't support Append.
	Record(ctx context.Context, conv conversation.Conversation, sessionID string, newMessages []map[string]any) error

	Clear(ctx context.Context, conv conversation.Conversation, sessionID string) (bool, error)

	// RunHook invokes an optional callback. Durable executors override to
	// wrap the call in a step/activity.
	RunHook(ctx context.Context, fn HookFn, args ...any) (any, error)

	// MintTurnID produces a replay-safe unique id for one turn.
	MintTurnID(ctx context.Context) (string, error)

	// AppendMessages buffers messages for the current turn.
	AppendMessages(messages []map[string]any)

	// LoadMessages returns the current message list (prior + delta).
	LoadMessages() []map[string]any

	// SetMessages replaces the current message list (e.g. after compaction).
	SetMessages(messages []map[string]any)

	// MonotonicMS returns the per-runtime monotonic clock in milliseconds.
	// Durable executors substitute a deterministic clock for replay safety.
	MonotonicMS() int64
}

// HookFn is the signature of an optional callback the executor runs.
// The Python original supports arbitrary positional+keyword args; in Go we
// keep it as a variadic any.
type HookFn func(ctx context.Context, args ...any) (any, error)

// RunTurner is the optional capability for executors that want to wrap a
// whole turn in a workflow (Temporal/DBOS). AgentLoop calls RunTurn first
// and falls back to inline processing when the executor doesn't implement
// the interface or returns nil result with nil error.
type RunTurner interface {
	RunTurn(ctx context.Context, loop any, sessionID, message string, opts RunTurnOptions) (*RunTurnResult, error)
}

// RunTurnOptions bundles the optional kwargs for RunTurn.
type RunTurnOptions struct {
	Channel         string
	ChatID          string
	Media           []string
	PluginContext   []string
	OnProgress      ProgressFn
	Model           string
	PublishResponse bool
	Extra           map[string]any
}

// RunTurnResult is the (final_content, new_messages) tuple from Python.
type RunTurnResult struct {
	FinalContent string
	NewMessages  []map[string]any
	// FinalContentSet distinguishes "" from "no content returned".
	FinalContentSet bool
}

// ProgressFn is the progress callback signature. tool_hint=true signals a
// tool-hint marker rather than reasoning content.
type ProgressFn func(ctx context.Context, content string, toolHint bool) error

// OverflowRecoverer is the optional capability for executors that route the
// ContextWindowExceededError recovery path through a durable step. Default
// impls forward to conversation.RecoverFromOverflow.
type OverflowRecoverer interface {
	RecoverFromOverflow(ctx context.Context, conv conversation.Conversation, sessionID string) ([]map[string]any, error)
}

// PriorSourceSetter is the optional Phase-2b lazy-prior capability.
type PriorSourceSetter interface {
	SetPriorSource(source PriorSource)
}

// PriorSource is the per-turn callback that returns the prior-history
// messages. Setting one allows the executor to keep prior off-heap between
// LLM iterations.
type PriorSource func() []map[string]any

// InboundEnqueuer is the optional durable-inbound-handoff capability.
type InboundEnqueuer interface {
	EnqueueInbound(ctx context.Context, msg any) error
}
