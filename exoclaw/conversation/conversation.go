// Package conversation contains the Conversation protocol and related types.
//
// Ported from exoclaw/agent/conversation.py.
//
// Lives in its own package (rather than under agent) so the executor package
// can import it without creating an import cycle through agent.
package conversation

import "context"

// BuildPromptOptions bundles the optional kwargs from Python's build_prompt.
type BuildPromptOptions struct {
	Channel        string
	ChatID         string
	Media          []string
	PluginContext  []string
	// Extra carries domain-specific kwargs (skill_names, turn_context, …)
	// that the conversation backend understands.
	Extra map[string]any
}

// Conversation is the structural protocol for conversation state management.
//
// External packages implement this without inheriting from any exoclaw type.
type Conversation interface {
	// BuildPrompt returns the full messages list to send to the LLM.
	BuildPrompt(ctx context.Context, sessionID, message string, opts BuildPromptOptions) ([]map[string]any, error)

	// Record persists the messages produced during one turn.
	//
	// The agent loop calls this at end-of-turn as the default persistence
	// path. Implementations that support per-message persistence should
	// additionally implement AppendableConversation; the loop will flush
	// each message as it's produced and skip the end-of-turn Record call.
	Record(ctx context.Context, sessionID string, newMessages []map[string]any) error

	// Clear archives the current session and starts fresh. Returns true on success.
	Clear(ctx context.Context, sessionID string) (bool, error)

	// ListSessions returns metadata for all known sessions.
	ListSessions() []map[string]any
}

// ActiveToolsProvider is an optional capability: implementations can return
// the set of optional tool names to surface for the current turn. The agent
// loop calls this after BuildPrompt; an empty set suppresses all optional
// tools.
type ActiveToolsProvider interface {
	ActiveTools() map[string]struct{}
}

// Lifecycle-hook decider seams (before_tool / before_finish / run_context) are
// further optional Conversation capabilities, kept OFF this interface — same
// reasoning as ActiveToolsProvider — so adding them doesn't force every
// structural impl to define them. They live as separate interfaces in the
// agent package (agent.BeforeToolDecider / agent.BeforeFinishDecider /
// agent.RunContexter) because they reference agent.HookContext; the loop
// type-asserts each with a no-op fallback. See exoclaw/agent/hooks.go.

// AppendableConversation is the opt-in extension for implementations that
// can persist one message at a time as the turn progresses.
//
// When the agent loop sees a Conversation that satisfies this protocol, it
// calls Append after each new assistant response, tool result, and the
// incoming user message — keeping crash recovery from losing mid-turn work
// and keeping the in-memory buffer from being the sole holder of turn
// state. PostTurn then runs end-of-turn hooks; Record is skipped entirely
// on this path.
//
// Do NOT implement Append as a no-op just to satisfy the type — the loop's
// capability check sees its presence and skips Record, so a no-op Append
// would drop persistence entirely.
type AppendableConversation interface {
	Conversation
	Append(ctx context.Context, sessionID string, message map[string]any) error
	PostTurn(ctx context.Context, sessionID string) error
}

// OverflowRecoverable is an optional capability layered on Conversation:
// when the provider raises a context-window-exceeded error, the agent loop
// calls the executor's recovery hook, which by default forwards to this
// method. Returning a non-nil slice signals "retry with this compacted
// message list"; returning nil signals "couldn't recover".
type OverflowRecoverable interface {
	RecoverFromOverflow(ctx context.Context, sessionID string) ([]map[string]any, error)
}

// PersistedHistoryLoader is the Phase-2b opt-in: when the conversation can
// cheaply re-read its history slice from disk, exposing this method lets
// the executor install a lazy PriorSource so the history bytes are not
// heap-resident between LLM iterations within a turn.
type PersistedHistoryLoader interface {
	LoadPersistedHistory(sessionID string) []map[string]any
}
