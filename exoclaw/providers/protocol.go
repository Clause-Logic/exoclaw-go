package providers

import "context"

// ChatParams bundles the optional knobs for LLMProvider.Chat.
//
// The Python original used keyword args; Go gets a struct so callers don't
// have to thread positional args through. Zero values mean "not set" for
// pointer fields, and defaults for non-pointer scalars (MaxTokens=4096,
// Temperature=0.7) are applied by callers (matching Python defaults).
type ChatParams struct {
	Tools           []map[string]any
	Model           string
	MaxTokens       int
	Temperature     float64
	ReasoningEffort string
	ResponseFormat  *ResponseFormat
	// Stream chooses between streamed SSE and one-shot JSON. nil → use
	// the provider's default (streaming for production, since it cuts
	// TTFT and peak memory); explicit false swaps to a single POST →
	// single JSON response. Tests + callers that want deterministic
	// fixture replay set this false so the wire format is a single
	// object instead of a chunk stream.
	Stream *bool
}

// LLMProvider is the structural protocol any LLM backend must satisfy.
type LLMProvider interface {
	Chat(ctx context.Context, messages []map[string]any, params ChatParams) (*LLMResponse, error)
	GetDefaultModel() string
}
