// Ported from exoclaw/agent/hooks.py.
//
// The agent loop fires hooks at lifecycle seams (currently before_tool and
// before_finish). At each seam it builds a HookContext and asks the
// Conversation for a single decision — the same way it already asks
// ActiveToolsProvider which optional tools to advertise. Core defines only the
// context handed to a hook, the decision shapes a hook returns, and the seams
// the loop calls. It has no opinion on what produces those decisions, how many
// hooks there are, or how they compose — a consumer decides activation and
// collapses any number of hooks into the one result core applies.
//
// Python reaches these seams via getattr(conversation, "before_tool", None);
// the Go-idiomatic translation is an optional interface the loop detects with a
// type assertion (BeforeToolDecider / BeforeFinishDecider / RunContexter),
// exactly like conversation.ActiveToolsProvider and executor.OverflowRecoverer.
package agent

import "context"

// Event names. Plain strings so callers key off them without importing a
// symbol — mirrors how the Python original keys hooks by name.
const (
	BeforeToolEvent   = "before_tool"
	BeforeFinishEvent = "before_finish"
)

// BeforeToolResult is the decision a before_tool consumer returns.
//
// Params (when non-nil) replaces the tool's arguments — how a consumer stamps
// an authoritative value. Block refuses the call; BlockReason becomes the tool
// result the model sees (a positive nudge, not an error).
type BeforeToolResult struct {
	Params      map[string]any
	Block       bool
	BlockReason string
}

// BeforeFinishResult is the decision a before_finish consumer returns.
//
// A non-empty ContinueMessage is appended as a user turn and the loop continues
// (the model gets another turn); empty lets the turn end.
type BeforeFinishResult struct {
	ContinueMessage string
}

// HookContext is read-only context the loop passes to a hook decision.
//
// RunContext is a per-run bag the host seeds (e.g. a cycle id) so a hook can
// read authoritative values instead of trusting the model's tool args.
// Messages is the current transcript (a read-only copy). Both are copies — a
// decider is pure, deterministic logic that reads this context and returns a
// decision, run inline in the turn; durable I/O belongs in an activity/step the
// consumer's hook dispatches, the one primitive every executor implements
// natively. The remaining fields are event-specific, populated per seam.
type HookContext struct {
	Event      string
	RunContext map[string]any
	Messages   []map[string]any

	ToolName     string
	Params       map[string]any
	FinalContent string
	ToolsUsed    []string
}

// BeforeToolDecider is the optional Conversation capability the loop invokes
// before each tool dispatch. Return a BeforeToolResult to replace the tool's
// args (Params) or veto the call (Block + BlockReason); return nil to leave it
// unchanged. The consumer owns whether anything fires and how multiple hooks
// compose into this one result — the loop just applies it.
type BeforeToolDecider interface {
	BeforeTool(ctx context.Context, hc *HookContext) (*BeforeToolResult, error)
}

// BeforeFinishDecider is the optional Conversation capability the loop invokes
// when the model ends a turn with no tool calls. Return a BeforeFinishResult
// with a non-empty ContinueMessage to re-prompt (the loop appends it as a user
// turn and continues), or nil to let the turn end.
type BeforeFinishDecider interface {
	BeforeFinish(ctx context.Context, hc *HookContext) (*BeforeFinishResult, error)
}

// RunContexter is the optional Conversation capability that returns the per-run
// context bag exposed to hooks via HookContext.RunContext (e.g. a cycle id
// minted before the turn). Defaults to empty when not implemented.
type RunContexter interface {
	RunContext() map[string]any
}
