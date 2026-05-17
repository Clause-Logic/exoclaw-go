package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xhttp "github.com/standd/exoclaw-go/exoclaw/http"
	"github.com/standd/exoclaw-go/exoclaw/providers"
)

// Ported from exoclaw-plugins/packages/exoclaw-provider-openai/tests/test_provider_openai.py.

// ----------------------------------------------------------------------
// Streaming-body tests
// ----------------------------------------------------------------------

func collectBytes(r io.Reader) []byte {
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.Bytes()
}

func TestStreamBody_RoundTripSingleMessage(t *testing.T) {
	head := map[string]any{"model": "m1", "temperature": 0.5}
	messages := []map[string]any{{"role": "user", "content": "hi"}}

	body := collectBytes(streamBody(head, messages))
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("invalid json: %v\nbody=%s", err, body)
	}
	if parsed["model"] != "m1" {
		t.Fatalf("model: %v", parsed["model"])
	}
	if parsed["temperature"].(float64) != 0.5 {
		t.Fatalf("temperature: %v", parsed["temperature"])
	}
	msgs, _ := parsed["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("messages len: %d", len(msgs))
	}
	first := msgs[0].(map[string]any)
	if first["role"] != "user" || first["content"] != "hi" {
		t.Fatalf("got %v", first)
	}
}

func TestStreamBody_RoundTripManyMessages(t *testing.T) {
	head := map[string]any{"model": "m1"}
	var messages []map[string]any
	for i := 0; i < 20; i++ {
		messages = append(messages, map[string]any{"role": "user", "content": fmt.Sprintf("msg-%d", i)})
	}
	body := collectBytes(streamBody(head, messages))
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("invalid json: %v\nbody=%s", err, body)
	}
	msgs, _ := parsed["messages"].([]any)
	if len(msgs) != 20 {
		t.Fatalf("len %d", len(msgs))
	}
	last := msgs[19].(map[string]any)
	if last["content"] != "msg-19" {
		t.Fatalf("got %v", last)
	}
}

func TestStreamBody_NestedTypesAndQuotes(t *testing.T) {
	head := map[string]any{
		"model": "m1",
		"tools": []map[string]any{
			{
				"type": "function",
				"function": map[string]any{
					"name":       "lookup",
					"parameters": map[string]any{"type": "object", "properties": map[string]any{}},
				},
			},
		},
		"stream": true,
	}
	messages := []map[string]any{{"role": "assistant", "content": "he said \"hi\"\nthen left"}}
	body := collectBytes(streamBody(head, messages))
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("invalid json: %v\nbody=%s", err, body)
	}
	msgs, _ := parsed["messages"].([]any)
	if msgs[0].(map[string]any)["content"] != "he said \"hi\"\nthen left" {
		t.Fatalf("escaping broke: %v", msgs[0])
	}
}

func TestStreamBody_EmptyMessages(t *testing.T) {
	head := map[string]any{"model": "m1"}
	body := collectBytes(streamBody(head, nil))
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("invalid json: %v\nbody=%s", err, body)
	}
	msgs, _ := parsed["messages"].([]any)
	if len(msgs) != 0 {
		t.Fatalf("got %v", msgs)
	}
}

func TestStreamBody_UnderscoreKeysStripped(t *testing.T) {
	messages := []map[string]any{
		{
			"role":          "tool",
			"tool_call_id":  "tc1",
			"name":          "exec",
			"content":       "preview",
			"_content_file": "/nonexistent/path",
		},
	}
	body := collectBytes(streamBody(map[string]any{"model": "m1"}, messages))
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("invalid json: %v\nbody=%s", err, body)
	}
	msg := parsed["messages"].([]any)[0].(map[string]any)
	if _, ok := msg["_content_file"]; ok {
		t.Fatal("_content_file leaked to wire")
	}
}

func TestStreamBody_FileBackedStreamedFromDisk(t *testing.T) {
	tmp := t.TempDir()
	scratch := filepath.Join(tmp, "tool-output.txt")
	expected := "line one\nline two\nspecial: \"quote\" and \\backslash\n"
	if err := os.WriteFile(scratch, []byte(expected), 0o644); err != nil {
		t.Fatal(err)
	}

	messages := []map[string]any{
		{
			"role":          "tool",
			"tool_call_id":  "tc1",
			"name":          "exec",
			"content":       "preview-only",
			"_content_file": scratch,
		},
	}
	body := collectBytes(streamBody(map[string]any{"model": "m1"}, messages))
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("invalid json: %v\nbody=%s", err, body)
	}
	msg := parsed["messages"].([]any)[0].(map[string]any)
	if msg["content"] != expected {
		t.Fatalf("content mismatch:\n  got: %q\n want: %q", msg["content"], expected)
	}
	if _, ok := msg["_content_file"]; ok {
		t.Fatal("_content_file leaked")
	}
}

func TestStreamBody_FileBackedFallsBackToPreviewWhenMissing(t *testing.T) {
	messages := []map[string]any{
		{
			"role":          "tool",
			"tool_call_id":  "tc1",
			"name":          "exec",
			"content":       "fallback-preview",
			"_content_file": "/no/such/file.txt",
		},
	}
	body := collectBytes(streamBody(map[string]any{"model": "m1"}, messages))
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("invalid json: %v\nbody=%s", err, body)
	}
	msg := parsed["messages"].([]any)[0].(map[string]any)
	if msg["content"] != "fallback-preview" {
		t.Fatalf("fallback content not used: %v", msg["content"])
	}
}

func TestStreamBody_FileBackedDoesNotMaterialise(t *testing.T) {
	tmp := t.TempDir()
	scratch := filepath.Join(tmp, "big.txt")
	expected := strings.Repeat("x", 100_000)
	if err := os.WriteFile(scratch, []byte(expected), 0o644); err != nil {
		t.Fatal(err)
	}

	messages := []map[string]any{
		{
			"role":          "tool",
			"tool_call_id":  "tc1",
			"name":          "exec",
			"content":       "preview",
			"_content_file": scratch,
		},
	}
	// Read in fixed-size chunks; no single read returns the whole body.
	r := streamBody(map[string]any{"model": "m1"}, messages)
	var assembled bytes.Buffer
	chunk := make([]byte, 4096)
	for {
		n, err := r.Read(chunk)
		if n > 0 {
			if n >= 100_000 {
				t.Fatal("a single read returned the entire body — streaming broken")
			}
			assembled.Write(chunk[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read err: %v", err)
		}
	}
	var parsed map[string]any
	if err := json.Unmarshal(assembled.Bytes(), &parsed); err != nil {
		t.Fatalf("invalid json: %v\nbody=%s", err, assembled.Bytes())
	}
	if parsed["messages"].([]any)[0].(map[string]any)["content"] != expected {
		t.Fatal("content lost")
	}
}

// ----------------------------------------------------------------------
// SSE helper
// ----------------------------------------------------------------------

func sseCompletion(content string, toolCalls []map[string]any, finishReason string) []byte {
	delta := map[string]any{}
	if content != "" {
		delta["content"] = content
	}
	if len(toolCalls) > 0 {
		delta["tool_calls"] = toolCalls
	}
	var lines []string
	first, _ := json.Marshal(map[string]any{
		"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": nil}},
	})
	lines = append(lines, "data: "+string(first))
	second, _ := json.Marshal(map[string]any{
		"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": finishReason}},
		"usage":   map[string]any{"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12},
	})
	lines = append(lines, "data: "+string(second))
	lines = append(lines, "data: [DONE]")
	return []byte(strings.Join(lines, "\n\n") + "\n\n")
}

// ----------------------------------------------------------------------
// Provider routing tests
// ----------------------------------------------------------------------

func newTestProvider(t *testing.T, primaryHandler, backupHandler http.HandlerFunc, fallbacks map[string][]string) (*StreamingProvider, *httptest.Server, *httptest.Server) {
	t.Helper()
	primary := httptest.NewServer(primaryHandler)
	t.Cleanup(primary.Close)
	var backup *httptest.Server
	deployments := map[string]Deployment{
		"primary": {BaseURL: primary.URL + "/v1", APIKey: "k-a"},
	}
	if backupHandler != nil {
		backup = httptest.NewServer(backupHandler)
		t.Cleanup(backup.Close)
		deployments["backup"] = Deployment{BaseURL: backup.URL + "/v1", APIKey: "k-b"}
	}
	prov, err := NewStreamingProvider(Options{
		DefaultModel:      "primary",
		Deployments:       deployments,
		Fallbacks:         fallbacks,
		Client:            xhttp.New(5 * time.Second),
		RequestTimeout:    5 * time.Second,
		StreamTTFTTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prov.Close() })
	return prov, primary, backup
}

func TestProvider_RoutesToDeploymentBaseURLAndKey(t *testing.T) {
	var seenAuth atomic.Value
	prov, primary, _ := newTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		seenAuth.Store(r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(sseCompletion("hello", nil, "stop"))
	}, nil, nil)

	resp, err := prov.Chat(context.Background(), []map[string]any{{"role": "user", "content": "hi"}}, providers.ChatParams{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Content == nil || *resp.Content != "hello" {
		t.Fatalf("content: %v", resp.Content)
	}
	if got, _ := seenAuth.Load().(string); got != "Bearer k-a" {
		t.Fatalf("auth: %q", got)
	}
	if !strings.HasPrefix(primary.URL, "http://") {
		t.Fatal("test server URL")
	}
}

func TestProvider_BodyIsStreamedAndJSON(t *testing.T) {
	var capturedBody []byte
	var capturedCT string
	prov, _, _ := newTestProvider(t, func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		capturedCT = r.Header.Get("Content-Type")
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(sseCompletion("ok", nil, "stop"))
	}, nil, nil)

	msgs := []map[string]any{{"role": "user", "content": "hello"}}
	_, err := prov.Chat(context.Background(), msgs, providers.ChatParams{})
	if err != nil {
		t.Fatal(err)
	}
	if capturedCT != "application/json" {
		t.Fatalf("content-type: %q", capturedCT)
	}
	var parsed map[string]any
	if err := json.Unmarshal(capturedBody, &parsed); err != nil {
		t.Fatalf("body parse: %v body=%s", err, capturedBody)
	}
	if parsed["stream"] != true {
		t.Fatal("stream flag missing")
	}
	if m := parsed["messages"].([]any); len(m) != 1 || m[0].(map[string]any)["content"] != "hello" {
		t.Fatalf("messages: %v", parsed["messages"])
	}
}

func TestProvider_FallbackOn503(t *testing.T) {
	primaryHits := atomic.Int64{}
	backupHits := atomic.Int64{}
	prov, _, _ := newTestProvider(t,
		func(w http.ResponseWriter, _ *http.Request) {
			primaryHits.Add(1)
			http.Error(w, `{"error":"busy"}`, 503)
		},
		func(w http.ResponseWriter, _ *http.Request) {
			backupHits.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write(sseCompletion("from-backup", nil, "stop"))
		},
		map[string][]string{"primary": {"backup"}},
	)

	resp, err := prov.Chat(context.Background(), []map[string]any{{"role": "user", "content": "x"}}, providers.ChatParams{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Content == nil || *resp.Content != "from-backup" {
		t.Fatalf("content: %v", resp.Content)
	}
	if primaryHits.Load() != 1 || backupHits.Load() != 1 {
		t.Fatalf("primary=%d backup=%d", primaryHits.Load(), backupHits.Load())
	}
}

func TestProvider_NoFallbackOn401(t *testing.T) {
	prov, _, _ := newTestProvider(t, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"bad key"}`, 401)
	}, func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("backup must not be called for 401")
	}, map[string][]string{"primary": {"backup"}})

	_, err := prov.Chat(context.Background(), []map[string]any{{"role": "user", "content": "x"}}, providers.ChatParams{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Fatalf("err: %v", err)
	}
}

func TestProvider_ContextWindowErrorDoesNotFallback(t *testing.T) {
	primaryHits := atomic.Int64{}
	prov, _, _ := newTestProvider(t,
		func(w http.ResponseWriter, _ *http.Request) {
			primaryHits.Add(1)
			http.Error(w, `{"error":{"code":"context_length_exceeded"}}`, 400)
		},
		func(w http.ResponseWriter, _ *http.Request) {
			t.Fatal("backup must not be called for context-window error")
		},
		map[string][]string{"primary": {"backup"}},
	)
	_, err := prov.Chat(context.Background(), []map[string]any{{"role": "user", "content": "x"}}, providers.ChatParams{})
	var ce *providers.ContextWindowExceededError
	if !errors.As(err, &ce) {
		t.Fatalf("expected ContextWindowExceededError, got %T: %v", err, err)
	}
	if primaryHits.Load() != 1 {
		t.Fatalf("primary hits: %d", primaryHits.Load())
	}
}

func TestProvider_FallbackExhaustedRaisesLastError(t *testing.T) {
	prov, _, _ := newTestProvider(t,
		func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "busy", 503) },
		func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "busy", 503) },
		map[string][]string{"primary": {"backup"}},
	)
	_, err := prov.Chat(context.Background(), []map[string]any{{"role": "user", "content": "x"}}, providers.ChatParams{})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Fatalf("err: %v", err)
	}
}

func TestProvider_ParsesToolCalls(t *testing.T) {
	prov, _, _ := newTestProvider(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(sseCompletion("", []map[string]any{
			{
				"index": 0,
				"id":    "call_abc",
				"function": map[string]any{
					"name":      "lookup",
					"arguments": `{"q":"weather"}`,
				},
			},
		}, "tool_calls"))
	}, nil, nil)

	resp, err := prov.Chat(context.Background(), []map[string]any{{"role": "user", "content": "x"}}, providers.ChatParams{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.FinishReason != "tool_calls" {
		t.Fatalf("finish: %q", resp.FinishReason)
	}
	if len(resp.ToolCalls) != 1 {
		t.Fatalf("calls: %d", len(resp.ToolCalls))
	}
	tc := resp.ToolCalls[0]
	if tc.Name != "lookup" || tc.ID != "call_abc" {
		t.Fatalf("got %+v", tc)
	}
	if tc.Arguments["q"] != "weather" {
		t.Fatalf("args: %v", tc.Arguments)
	}
}

func TestProvider_ExtraBodyAndHeadersApplied(t *testing.T) {
	var capturedHeader string
	var capturedBody map[string]any
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeader = r.Header.Get("HTTP-Referer")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &capturedBody)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(sseCompletion("ok", nil, "stop"))
	}))
	defer primary.Close()

	prov, err := NewStreamingProvider(Options{
		DefaultModel: "primary",
		Deployments: map[string]Deployment{
			"primary": {
				BaseURL:      primary.URL + "/v1",
				APIKey:       "k",
				ExtraHeaders: map[string]string{"HTTP-Referer": "https://openclaw"},
				ExtraBody:    map[string]any{"provider": map[string]any{"order": []any{"deepinfra"}}},
			},
		},
		Client:            xhttp.New(5 * time.Second),
		RequestTimeout:    5 * time.Second,
		StreamTTFTTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer prov.Close()

	if _, err := prov.Chat(context.Background(), []map[string]any{{"role": "user", "content": "x"}}, providers.ChatParams{}); err != nil {
		t.Fatal(err)
	}
	if capturedHeader != "https://openclaw" {
		t.Fatalf("HTTP-Referer: %q", capturedHeader)
	}
	prov_, _ := capturedBody["provider"].(map[string]any)
	order, _ := prov_["order"].([]any)
	if len(order) != 1 || order[0] != "deepinfra" {
		t.Fatalf("provider extra body: %v", capturedBody["provider"])
	}
}

func TestProvider_UnknownFallbackRejectedAtInit(t *testing.T) {
	_, err := NewStreamingProvider(Options{
		DefaultModel: "primary",
		Deployments: map[string]Deployment{
			"primary": {BaseURL: "https://a.example/v1", APIKey: "k"},
		},
		Fallbacks: map[string][]string{"primary": {"nonexistent"}},
	})
	if err == nil || !strings.Contains(err.Error(), "fallback") {
		t.Fatalf("expected fallback err, got %v", err)
	}
}

func TestProvider_DefaultModelMustBeDeclared(t *testing.T) {
	_, err := NewStreamingProvider(Options{
		DefaultModel: "not-there",
		Deployments: map[string]Deployment{
			"primary": {BaseURL: "https://a.example/v1", APIKey: "k"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "default_model") {
		t.Fatalf("expected default_model err, got %v", err)
	}
}

func TestProvider_NonSSE200FailsOver(t *testing.T) {
	prov, _, _ := newTestProvider(t,
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"error":"wrong endpoint"}`))
		},
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write(sseCompletion("ok", nil, "stop"))
		},
		map[string][]string{"primary": {"backup"}},
	)
	resp, err := prov.Chat(context.Background(), []map[string]any{{"role": "user", "content": "x"}}, providers.ChatParams{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Content == nil || *resp.Content != "ok" {
		t.Fatalf("content: %v", resp.Content)
	}
}
