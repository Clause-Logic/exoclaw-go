package providers

import (
	"reflect"
	"testing"
)

// Ported from tests/test_provider_types.py.

func TestToolCallRequest_Constructor(t *testing.T) {
	req := ToolCallRequest{ID: "call-7", Name: "search", Arguments: map[string]any{"q": "hi"}}
	if req.ID != "call-7" || req.Name != "search" {
		t.Fatalf("got %+v", req)
	}
	if !reflect.DeepEqual(req.Arguments, map[string]any{"q": "hi"}) {
		t.Fatalf("args: %+v", req.Arguments)
	}
}

func TestLLMResponse_RequiredOnly(t *testing.T) {
	content := "hello"
	resp := LLMResponse{Content: &content}
	if *resp.Content != "hello" {
		t.Fatal("content")
	}
	if len(resp.ToolCalls) != 0 {
		t.Fatal("tool_calls default")
	}
	if resp.HasToolCalls() {
		t.Fatal("HasToolCalls should be false")
	}
}

func TestLLMResponse_Full(t *testing.T) {
	tc := ToolCallRequest{ID: "x", Name: "t", Arguments: map[string]any{}}
	rc := "thinking..."
	resp := LLMResponse{
		Content:          nil,
		ToolCalls:        []ToolCallRequest{tc},
		FinishReason:     "tool_calls",
		Usage:            map[string]int{"input_tokens": 100, "output_tokens": 50},
		ReasoningContent: &rc,
		ThinkingBlocks:   []map[string]any{{"type": "thinking", "text": "..."}},
	}
	if resp.Content != nil {
		t.Fatal("content")
	}
	if !resp.HasToolCalls() {
		t.Fatal("HasToolCalls should be true")
	}
	if resp.FinishReason != "tool_calls" {
		t.Fatal("finish_reason")
	}
	if resp.Usage["input_tokens"] != 100 {
		t.Fatal("usage")
	}
}

func TestContextWindowExceededError(t *testing.T) {
	var err error = &ContextWindowExceededError{Message: "too big"}
	if err.Error() != "too big" {
		t.Fatalf("err: %q", err.Error())
	}
	err = &ContextWindowExceededError{}
	if err.Error() == "" {
		t.Fatal("default message empty")
	}
}
