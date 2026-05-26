// Package providers defines the LLMProvider interface and shared types.
//
// Ported from exoclaw/providers/types.py and providers/protocol.py.
package providers

import "strings"

// ContextWindowExceededError is returned by providers when the prompt exceeds
// the model's context window.
type ContextWindowExceededError struct {
	Message string
}

func (e *ContextWindowExceededError) Error() string {
	if e.Message == "" {
		return "context window exceeded"
	}
	return e.Message
}

// ToolCallRequest is a tool call request from the LLM.
type ToolCallRequest struct {
	ID        string
	Name      string
	Arguments map[string]any
}

// LLMResponse is a response from an LLM provider.
type LLMResponse struct {
	Content          *string
	ToolCalls        []ToolCallRequest
	FinishReason     string
	Usage            map[string]int
	ReasoningContent *string
	ThinkingBlocks   []map[string]any
}

// HasToolCalls reports whether the response includes any tool calls.
func (r *LLMResponse) HasToolCalls() bool {
	return len(r.ToolCalls) > 0
}

// DropMalformedToolCalls removes tool calls with a blank function name and
// returns how many were dropped. Some models (notably gemini-flash) degenerate
// into emitting finish_reason=tool_calls with a null-named call. Left in,
// HasToolCalls stays true and the agent loop tries to dispatch a nameless tool,
// which never resolves — so the loop neither makes progress nor reaches the
// no-tool-call finish path, and spins to the iteration cap with no output.
// Dropping them lets an all-malformed response fall through to the normal
// finish (firing the before-finish nudge), while a mixed response keeps its
// valid calls.
func (r *LLMResponse) DropMalformedToolCalls() int {
	if len(r.ToolCalls) == 0 {
		return 0
	}
	kept := r.ToolCalls[:0]
	dropped := 0
	for _, tc := range r.ToolCalls {
		if strings.TrimSpace(tc.Name) == "" {
			dropped++
			continue
		}
		kept = append(kept, tc)
	}
	r.ToolCalls = kept
	return dropped
}

// JSONSchema configures structured outputs, including a JSON Schema.
type JSONSchema struct {
	Name        string         // required, must match [a-zA-Z0-9_-]{1,64}
	Description string         // optional
	Schema      map[string]any // optional
	Strict      *bool          // optional
}

// ResponseFormat is the union of supported response-format kinds.
// One of the fields must be set.
type ResponseFormat struct {
	Text       *ResponseFormatText
	JSONSchema *ResponseFormatJSONSchema
	JSONObject *ResponseFormatJSONObject
}

// ResponseFormatText is the default response format used to generate text.
type ResponseFormatText struct {
	Type string // "text"
}

// ResponseFormatJSONSchema is the JSON-schema response format for structured outputs.
type ResponseFormatJSONSchema struct {
	JSONSchema JSONSchema
	Type       string // "json_schema"
}

// ResponseFormatJSONObject is the JSON-object response format.
// Prefer JSONSchema for models that support it.
type ResponseFormatJSONObject struct {
	Type string // "json_object"
}
