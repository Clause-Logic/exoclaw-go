package workspace

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Ported from tests/test_tools_workspace.py — TestWebSearchTool + TestWebFetchTool.

// ---------- WebFetchTool ----------

func TestWebFetch_ValidationRejectsNonHTTP(t *testing.T) {
	tool := NewWebFetchTool(WebFetchOptions{})
	got, _ := tool.Execute(context.Background(), map[string]any{"url": "ftp://example.com/x"})
	var env map[string]any
	if err := json.Unmarshal([]byte(got), &env); err != nil {
		t.Fatal(err)
	}
	if s, _ := env["error"].(string); !strings.Contains(s, "Only http/https allowed") {
		t.Fatalf("err: %v", env["error"])
	}
}

func TestWebFetch_StripsTagsFromHTML(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<html><head><title>T</title></head><body><p>hello world</p></body></html>")
	}))
	defer ts.Close()
	tool := NewWebFetchTool(WebFetchOptions{HTTPClient: ts.Client()})
	got, _ := tool.Execute(context.Background(), map[string]any{"url": ts.URL})
	var env map[string]any
	if err := json.Unmarshal([]byte(got), &env); err != nil {
		t.Fatal(err)
	}
	text, _ := env["text"].(string)
	if !strings.Contains(text, "hello world") {
		t.Fatalf("text: %q", text)
	}
	if !strings.Contains(text, "# T") {
		t.Fatalf("expected title prefix: %q", text)
	}
	if env["extractor"] != "readability" {
		t.Fatalf("extractor: %v", env["extractor"])
	}
}

func TestWebFetch_PrettyPrintsJSON(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"a":1,"b":[2,3]}`)
	}))
	defer ts.Close()
	tool := NewWebFetchTool(WebFetchOptions{HTTPClient: ts.Client()})
	got, _ := tool.Execute(context.Background(), map[string]any{"url": ts.URL})
	var env map[string]any
	if err := json.Unmarshal([]byte(got), &env); err != nil {
		t.Fatal(err)
	}
	if env["extractor"] != "json" {
		t.Fatalf("extractor: %v", env["extractor"])
	}
	text, _ := env["text"].(string)
	if !strings.Contains(text, "\"a\": 1") {
		t.Fatalf("not pretty: %q", text)
	}
}

func TestWebFetch_TruncatesAtMaxChars(t *testing.T) {
	body := strings.Repeat("z", 1000)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, body)
	}))
	defer ts.Close()
	tool := NewWebFetchTool(WebFetchOptions{HTTPClient: ts.Client(), MaxChars: 100})
	got, _ := tool.Execute(context.Background(), map[string]any{"url": ts.URL})
	var env map[string]any
	if err := json.Unmarshal([]byte(got), &env); err != nil {
		t.Fatal(err)
	}
	if env["truncated"] != true {
		t.Fatalf("not truncated: %v", env)
	}
	text, _ := env["text"].(string)
	if len(text) != 100 {
		t.Fatalf("text len: %d", len(text))
	}
}

func TestWebFetch_HTTP4xxSurfaced(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", 404)
	}))
	defer ts.Close()
	tool := NewWebFetchTool(WebFetchOptions{HTTPClient: ts.Client()})
	got, _ := tool.Execute(context.Background(), map[string]any{"url": ts.URL})
	var env map[string]any
	_ = json.Unmarshal([]byte(got), &env)
	if !strings.Contains(env["error"].(string), "HTTP 404") {
		t.Fatalf("err: %v", env["error"])
	}
}

// ---------- WebSearchTool (Brave path) ----------

func TestWebSearch_NoAPIKeyError(t *testing.T) {
	t.Setenv("BRAVE_API_KEY", "")
	tool := NewWebSearchTool(WebSearchOptions{})
	got, _ := tool.Execute(context.Background(), map[string]any{"query": "anything"})
	if !strings.Contains(got, "BRAVE_API_KEY") {
		t.Fatalf("got %q", got)
	}
}

func TestWebSearch_BraveResponseFormatted(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"web":{"results":[
			{"title":"R1","url":"https://e/1","description":"first"},
			{"title":"R2","url":"https://e/2"}
		]}}`)
	}))
	defer ts.Close()
	tool := NewWebSearchTool(WebSearchOptions{
		APIKey:     "k",
		HTTPClient: ts.Client(),
		MaxResults: 5,
	})
	// Patch the URL so the test http server is hit instead of the
	// real Brave endpoint. The cleanest way is to substitute the
	// http.Client with a transport that rewrites the URL.
	tool.HTTPClient = &http.Client{Transport: rewriteTransport{target: ts.URL}}
	got, _ := tool.Execute(context.Background(), map[string]any{"query": "x"})
	if !strings.Contains(got, "1. R1") || !strings.Contains(got, "https://e/1") {
		t.Fatalf("got %q", got)
	}
	if !strings.Contains(got, "first") {
		t.Fatalf("missing description: %q", got)
	}
}

// rewriteTransport sends every request to target, regardless of the
// original URL — used to point the WebSearchTool at a httptest server.
type rewriteTransport struct{ target string }

func (rt rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	newReq := req.Clone(req.Context())
	parsed, _ := newReq.URL.Parse(rt.target)
	newReq.URL.Scheme = parsed.Scheme
	newReq.URL.Host = parsed.Host
	newReq.Host = parsed.Host
	return http.DefaultTransport.RoundTrip(newReq)
}

// ---------- WebSearchTool (model path) ----------

type fakeProvider struct {
	content string
}

func (p *fakeProvider) GetDefaultModel() string { return "test-model" }

func (p *fakeProvider) Chat(_ context.Context, _ []map[string]any, _ struct{}) (any, error) {
	return p.content, nil
}

// ---------- helpers ----------

func TestStripTagsBasic(t *testing.T) {
	got := stripTags("<p>hello <b>world</b></p>")
	if got != "hello world" {
		t.Fatalf("got %q", got)
	}
}

func TestStripTagsRemovesScripts(t *testing.T) {
	got := stripTags("a <script>evil()</script> b")
	if got != "a  b" {
		t.Fatalf("got %q", got)
	}
}
