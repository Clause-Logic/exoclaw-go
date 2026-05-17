package conversationfile

import "context"

// Ported from exoclaw_conversation/protocols.py.
//
// SessionReader, HistoryStore, MemoryBackend, ConsolidationPolicy,
// PromptBuilder — the internal protocols DefaultConversation composes.

// StreamMessage is a single emitted message from a SessionReader.Stream
// channel. err is non-nil only on the final emission; receivers stop
// reading as soon as err is non-nil OR the channel closes.
type StreamMessage struct {
	Message map[string]any
	Err     error
}

// SessionReader is the read-only, streaming view of a session's
// append-only message log.
//
// All access is lazy — implementations stream from disk and never
// materialise the full log in RAM. Restartable: Stream may be called
// multiple times.
type SessionReader interface {
	// Key returns the session key this reader is bound to.
	Key() string

	// Count returns the total messages currently in the log. Cheap —
	// backed by the store's index, not a full scan.
	Count(ctx context.Context) (int, error)

	// Stream emits messages in [start, end) one at a time. end < 0 streams
	// to the current tail. Restartable — call again to re-read. The
	// returned channel is closed when iteration completes.
	Stream(ctx context.Context, start, end int) <-chan StreamMessage

	// At does a random-access read of one message. Returns nil if out of
	// range. For peek/lookahead — not for bulk reads.
	At(ctx context.Context, index int) (map[string]any, error)
}

// Session is the in-memory view of a single session — produced by
// HistoryStore.GetOrCreate.
//
// Methods on Session are defined in session/manager.go. Forward-declared
// here as an empty interface so protocols can reference it without an
// import cycle; concrete code casts to *session.Session as needed.
type Session interface {
	GetHistory(maxMessages int) []map[string]any
	Append(messages []map[string]any)
	Messages() []map[string]any
	Key() string
}

// HistoryStore is the protocol for session history persistence.
type HistoryStore interface {
	GetOrCreate(key string) Session
	Save(session Session) error
	Invalidate(key string)
	ListSessions() []map[string]any

	// SaveAppend appends new messages to disk. Default impls fall back to Save.
	SaveAppend(session Session, newMessages []map[string]any) error

	// LoadRange loads a range of messages from disk by index.
	LoadRange(key string, start, end int) ([]map[string]any, error)

	// Reader returns a streaming reader for the session's append-only log.
	Reader(key string) SessionReader

	// ReadHistory returns the unconsolidated tail for LLM input, applying
	// orphan repair. maxMessages < 0 means "no cap — return the full tail".
	ReadHistory(key string, maxMessages int) []map[string]any
}

// MemoryBackend is the protocol for long-term memory storage and
// summarization.
//
// The backend's job is to produce two artefacts from a list of messages: a
// long-term memory document (e.g. MEMORY.md) and a grep-searchable
// history-log entry (e.g. HISTORY.md). It does not own session state —
// boundaries, summaries, and view assembly belong to ConsolidationPolicy.
type MemoryBackend interface {
	// GetMemoryContext returns text to inject into the system prompt as
	// long-term memory context. Empty when no memory has been accumulated.
	GetMemoryContext() string

	// Summarize summarises the given messages and persists the result.
	// Returns the new history-log entry text on success, or "" + nil on
	// "no-op" (e.g. nothing worth summarising).
	Summarize(ctx context.Context, messages []map[string]any) (string, error)
}

// ConsolidationPolicy is the pluggable consolidation strategy.
//
// A policy owns the view the LLM sees: it transforms the append-only
// message log into the message list sent to the model. It may drop,
// replace, prepend (e.g. with a summary), or truncate messages — and it
// persists its own state in a sidecar next to the session file. The
// session log itself is append-only and never mutated by the policy.
//
// Transform is the read seam: DefaultConversation calls it every turn to
// materialise the LLM input from a SessionReader. OnTurnComplete is the
// write seam: called once per turn so the policy can run any deferred
// work (token-estimate maintenance, background summarisation) and persist
// its sidecar.
//
// Policies receive no Session handle. They are constructed with whatever
// state-store they need; the only runtime input is a streaming reader
// over the session's append-only log.
type ConsolidationPolicy interface {
	// Transform turns a streaming view of the session log into the
	// message list to send to the LLM.
	//
	// If budget > 0, the emitted stream should aim to fit within budget
	// tokens (best-effort, for overflow recovery). Otherwise normal
	// consolidation rules apply.
	Transform(ctx context.Context, reader SessionReader, budget int) <-chan StreamMessage

	// OnTurnComplete notifies the policy a turn finished. Lets it run
	// background work and persist its sidecar.
	OnTurnComplete(ctx context.Context, reader SessionReader) error
}

// BuildMessagesOptions bundles the optional knobs for PromptBuilder.BuildMessages.
type BuildMessagesOptions struct {
	SkillNames    []string
	Media         []string
	Channel       string
	ChatID        string
	ExtraContext  string
	TurnContext   []string
	Isolated      bool
}

// PromptBuilder is the protocol for assembling the LLM message list.
type PromptBuilder interface {
	BuildMessages(history []map[string]any, currentMessage string, opts BuildMessagesOptions) []map[string]any

	// GetActiveOptionalTools returns optional tool names activated by the
	// current turn's skills. Implementations that don't need skill-scoped
	// tools may return an empty set.
	GetActiveOptionalTools() map[string]struct{}
}
