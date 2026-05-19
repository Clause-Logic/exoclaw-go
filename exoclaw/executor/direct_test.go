package executor

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/Clause-Logic/exoclaw-go/exoclaw/agent/tools"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/conversation"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/providers"
)

// Ported from tests/test_executor.py.

func TestDirect_MintTurnIDMonotonic(t *testing.T) {
	e := NewDirectExecutor()
	ctx := context.Background()
	// uuidv7 only guarantees the timestamp prefix (first 12 hex chars,
	// before the version nibble) is non-decreasing — the random tail is
	// random, so two ids minted in the same ms aren't lexically ordered.
	// Compare timestamp prefixes only, as in the Python suite.
	prevTS := ""
	for i := 0; i < 200; i++ {
		id, err := e.MintTurnID(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(id) != 36 {
			t.Fatalf("bad len %d", len(id))
		}
		// "xxxxxxxx-xxxx-7xxx-..." — strip dashes from first 13 chars.
		ts := id[0:8] + id[9:13]
		if ts < prevTS {
			t.Fatalf("timestamp regression %s < %s", ts, prevTS)
		}
		prevTS = ts
	}
}

func TestDirect_MessageBufferIsolatedPerCtx(t *testing.T) {
	e := NewDirectExecutor()
	ctx1 := WithTurnState(context.Background())
	ctx2 := WithTurnState(context.Background())

	e.SetMessagesCtx(ctx1, []map[string]any{{"role": "user", "content": "ctx1"}})
	e.SetMessagesCtx(ctx2, []map[string]any{{"role": "user", "content": "ctx2"}})

	a := e.LoadMessagesCtx(ctx1)
	b := e.LoadMessagesCtx(ctx2)
	if len(a) != 1 || a[0]["content"] != "ctx1" {
		t.Fatalf("ctx1: %v", a)
	}
	if len(b) != 1 || b[0]["content"] != "ctx2" {
		t.Fatalf("ctx2: %v", b)
	}
}

func TestDirect_AppendThenLoadConcatsPriorAndDelta(t *testing.T) {
	e := NewDirectExecutor()
	ctx := WithTurnState(context.Background())
	e.SetMessagesCtx(ctx, []map[string]any{{"role": "system", "content": "sys"}})
	e.AppendMessagesCtx(ctx, []map[string]any{{"role": "assistant", "content": "hi"}})
	msgs := e.LoadMessagesCtx(ctx)
	if len(msgs) != 2 {
		t.Fatalf("want 2 got %d", len(msgs))
	}
	if msgs[0]["content"] != "sys" || msgs[1]["content"] != "hi" {
		t.Fatalf("order: %v", msgs)
	}
}

func TestDirect_SetMessagesClearsDelta(t *testing.T) {
	e := NewDirectExecutor()
	ctx := WithTurnState(context.Background())
	e.AppendMessagesCtx(ctx, []map[string]any{{"role": "x"}})
	e.SetMessagesCtx(ctx, []map[string]any{{"role": "system"}})
	msgs := e.LoadMessagesCtx(ctx)
	if len(msgs) != 1 || msgs[0]["role"] != "system" {
		t.Fatalf("delta not cleared: %v", msgs)
	}
}

func TestDirect_SetPriorSourceReReadsEachCall(t *testing.T) {
	e := NewDirectExecutor()
	ctx := WithTurnState(context.Background())
	call := 0
	e.SetPriorSourceCtx(ctx, func() []map[string]any {
		call++
		return []map[string]any{{"role": "p", "call": call}}
	})
	a := e.LoadMessagesCtx(ctx)
	b := e.LoadMessagesCtx(ctx)
	if a[0]["call"] != 1 || b[0]["call"] != 2 {
		t.Fatalf("source not re-read: %v %v", a, b)
	}
}

// ----------------------------------------------------------------------
// BuildLazyPriorSource locator
// ----------------------------------------------------------------------

func TestBuildLazyPriorSource_FindsContiguousMatch(t *testing.T) {
	full := []map[string]any{
		{"role": "system", "content": "s"},
		{"role": "user", "content": "u1"},
		{"role": "assistant", "content": "a1"},
		{"role": "user", "content": "current"},
	}
	hist := []map[string]any{
		{"role": "user", "content": "u1"},
		{"role": "assistant", "content": "a1"},
	}
	calls := 0
	src := buildLazyPriorSource(full, hist, func() []map[string]any {
		calls++
		return hist
	})
	if src == nil {
		t.Fatal("source nil")
	}
	out := src()
	if len(out) != 4 {
		t.Fatalf("want 4, got %d", len(out))
	}
	if calls != 1 {
		t.Fatalf("calls: %d", calls)
	}
}

func TestBuildLazyPriorSource_NoMatchReturnsNil(t *testing.T) {
	src := buildLazyPriorSource(
		[]map[string]any{{"role": "system"}},
		[]map[string]any{{"role": "doesnt-match"}},
		func() []map[string]any { return nil },
	)
	if src != nil {
		t.Fatal("expected nil")
	}
}

func TestBuildLazyPriorSource_EmptyHistory(t *testing.T) {
	src := buildLazyPriorSource(
		[]map[string]any{{"role": "system"}},
		nil,
		func() []map[string]any { return nil },
	)
	if src != nil {
		t.Fatal("expected nil")
	}
}

// ----------------------------------------------------------------------
// Streaming tool path
// ----------------------------------------------------------------------

type streamingTool struct{ tools.ToolBase }

func newStreamingTool() *streamingTool {
	t := &streamingTool{}
	t.NameField = "stream"
	t.ParametersField = map[string]any{"type": "object", "properties": map[string]any{}}
	return t
}

func (t *streamingTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	return "should-not-be-called", nil
}

func (t *streamingTool) ExecuteStreaming(ctx context.Context, params map[string]any) (<-chan string, error) {
	ch := make(chan string, 3)
	go func() {
		defer close(ch)
		ch <- "chunk-a"
		ch <- "chunk-b"
		ch <- "chunk-c"
	}()
	return ch, nil
}

func TestDirect_ExecuteToolWithHandle_StreamingPath(t *testing.T) {
	e := NewDirectExecutor()
	reg := tools.NewToolRegistry()
	reg.Register(newStreamingTool(), false)
	ctx := WithTurnState(context.Background())
	res, err := e.ExecuteToolWithHandle(ctx, reg, "stream", nil, nil, "call-1")
	if err != nil {
		t.Fatal(err)
	}
	if res.ContentFile == "" {
		t.Fatal("expected file-backed result")
	}
	defer os.Remove(res.ContentFile)
	body, err := os.ReadFile(res.ContentFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "chunk-achunk-bchunk-c" {
		t.Fatalf("body: %q", string(body))
	}
	if !strings.Contains(res.Content, "chunk-a") {
		t.Fatalf("preview missing prefix: %q", res.Content)
	}
}

type inlineTool struct{ tools.ToolBase }

func newInlineTool() *inlineTool {
	t := &inlineTool{}
	t.NameField = "inline"
	t.ParametersField = map[string]any{"type": "object", "properties": map[string]any{}}
	return t
}
func (t *inlineTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	return "plain-result", nil
}

func TestDirect_ExecuteToolWithHandle_InlineFallback(t *testing.T) {
	e := NewDirectExecutor()
	reg := tools.NewToolRegistry()
	reg.Register(newInlineTool(), false)
	res, err := e.ExecuteToolWithHandle(context.Background(), reg, "inline", nil, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if res.ContentFile != "" {
		t.Fatal("expected no scratch file")
	}
	if res.Content != "plain-result" {
		t.Fatalf("content: %s", res.Content)
	}
}

// ----------------------------------------------------------------------
// AppendMessage / Record / Clear forwarding
// ----------------------------------------------------------------------

type fakeConv struct {
	appended  []map[string]any
	postedID  string
	cleared   bool
	recorded  []map[string]any
}

func (f *fakeConv) BuildPrompt(ctx context.Context, sid, msg string, opts conversation.BuildPromptOptions) ([]map[string]any, error) {
	return []map[string]any{{"role": "user", "content": msg}}, nil
}
func (f *fakeConv) Record(ctx context.Context, sid string, msgs []map[string]any) error {
	f.recorded = append(f.recorded, msgs...)
	return nil
}
func (f *fakeConv) Clear(ctx context.Context, sid string) (bool, error) {
	f.cleared = true
	return true, nil
}
func (f *fakeConv) ListSessions() []map[string]any { return nil }

type appendableConv struct {
	fakeConv
}

func (f *appendableConv) Append(ctx context.Context, sid string, msg map[string]any) error {
	f.appended = append(f.appended, msg)
	return nil
}
func (f *appendableConv) PostTurn(ctx context.Context, sid string) error {
	f.postedID = sid
	return nil
}

func TestDirect_AppendForwardsToAppendable(t *testing.T) {
	e := NewDirectExecutor()
	c := &appendableConv{}
	if err := e.AppendMessage(context.Background(), c, "s", map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
	if len(c.appended) != 1 || c.appended[0]["x"] != 1 {
		t.Fatalf("appended: %v", c.appended)
	}
}

func TestDirect_AppendNoOpForNonAppendable(t *testing.T) {
	e := NewDirectExecutor()
	c := &fakeConv{}
	if err := e.AppendMessage(context.Background(), c, "s", map[string]any{"x": 1}); err != nil {
		t.Fatal(err)
	}
}

func TestDirect_ClearAndRecord(t *testing.T) {
	e := NewDirectExecutor()
	c := &fakeConv{}
	ok, err := e.Clear(context.Background(), c, "s")
	if err != nil || !ok || !c.cleared {
		t.Fatal("clear")
	}
	if err := e.Record(context.Background(), c, "s", []map[string]any{{"x": 1}}); err != nil {
		t.Fatal(err)
	}
	if len(c.recorded) != 1 {
		t.Fatal("record")
	}
}

// ----------------------------------------------------------------------
// Chat / Tool execution forwarding
// ----------------------------------------------------------------------

type fakeProvider struct {
	called int
	resp   *providers.LLMResponse
	err    error
}

func (p *fakeProvider) Chat(ctx context.Context, msgs []map[string]any, params providers.ChatParams) (*providers.LLMResponse, error) {
	p.called++
	return p.resp, p.err
}
func (p *fakeProvider) GetDefaultModel() string { return "fake-model" }

func TestDirect_ChatForwardsToProvider(t *testing.T) {
	e := NewDirectExecutor()
	content := "hi"
	p := &fakeProvider{resp: &providers.LLMResponse{Content: &content}}
	resp, err := e.Chat(context.Background(), p, []map[string]any{{"role": "user", "content": "hello"}}, providers.ChatParams{})
	if err != nil {
		t.Fatal(err)
	}
	if p.called != 1 || *resp.Content != "hi" {
		t.Fatalf("got %+v", resp)
	}
}

func TestDirect_ChatPropagatesContextWindowError(t *testing.T) {
	e := NewDirectExecutor()
	p := &fakeProvider{err: &providers.ContextWindowExceededError{Message: "too big"}}
	_, err := e.Chat(context.Background(), p, nil, providers.ChatParams{})
	var ce *providers.ContextWindowExceededError
	if !errors.As(err, &ce) {
		t.Fatalf("expected context-window err, got %T: %v", err, err)
	}
}
