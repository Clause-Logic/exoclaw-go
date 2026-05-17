package agent

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/standd/exoclaw-go/exoclaw/agent/tools"
	"github.com/standd/exoclaw-go/exoclaw/bus"
	"github.com/standd/exoclaw-go/exoclaw/conversation"
	"github.com/standd/exoclaw-go/exoclaw/providers"
)

// Ported from tests/test_loop_coverage.py + tests/test_loop_extension_points.py.

// ----------------------------------------------------------------------
// fakes
// ----------------------------------------------------------------------

type stubConversation struct {
	prior     []map[string]any
	recorded  []map[string]any
	cleared   bool
	mu        sync.Mutex
}

func newStubConv() *stubConversation {
	return &stubConversation{prior: []map[string]any{{"role": "system", "content": "sys"}}}
}

func (c *stubConversation) BuildPrompt(ctx context.Context, sid, msg string, opts conversation.BuildPromptOptions) ([]map[string]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]map[string]any, 0, len(c.prior)+1)
	out = append(out, c.prior...)
	out = append(out, map[string]any{"role": "user", "content": msg})
	return out, nil
}
func (c *stubConversation) Record(ctx context.Context, sid string, msgs []map[string]any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recorded = append(c.recorded, msgs...)
	return nil
}
func (c *stubConversation) Clear(ctx context.Context, sid string) (bool, error) {
	c.cleared = true
	return true, nil
}
func (c *stubConversation) ListSessions() []map[string]any { return nil }

type stubProvider struct {
	calls int
	responses []*providers.LLMResponse
}

func (p *stubProvider) Chat(ctx context.Context, msgs []map[string]any, params providers.ChatParams) (*providers.LLMResponse, error) {
	r := p.responses[p.calls]
	p.calls++
	return r, nil
}
func (p *stubProvider) GetDefaultModel() string { return "test-model" }

func plainResponse(content string) *providers.LLMResponse {
	return &providers.LLMResponse{Content: &content, FinishReason: "stop"}
}

func toolCallResponse(callID, name string, args map[string]any) *providers.LLMResponse {
	empty := ""
	return &providers.LLMResponse{
		Content:      &empty,
		FinishReason: "tool_calls",
		ToolCalls: []providers.ToolCallRequest{
			{ID: callID, Name: name, Arguments: args},
		},
	}
}

type fixedTool struct{ tools.ToolBase }

func newFixedTool(name, out string) *fixedTool {
	t := &fixedTool{}
	t.NameField = name
	t.ParametersField = map[string]any{"type": "object", "properties": map[string]any{}}
	return t
}

type recordingTool struct {
	tools.ToolBase
	called int
	result string
}

func newRecordingTool(name, result string) *recordingTool {
	t := &recordingTool{result: result}
	t.NameField = name
	t.ParametersField = map[string]any{"type": "object", "properties": map[string]any{}}
	return t
}

func (t *recordingTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	t.called++
	return t.result, nil
}

// ----------------------------------------------------------------------
// tests
// ----------------------------------------------------------------------

func TestLoop_PlainResponse(t *testing.T) {
	conv := newStubConv()
	prov := &stubProvider{responses: []*providers.LLMResponse{plainResponse("hello")}}
	loop := New(Options{
		Bus:          bus.NewMessageBus(),
		Provider:     prov,
		Conversation: conv,
	})
	out, err := loop.ProcessDirect(context.Background(), "hi", "cli:direct", "cli", "direct", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if out != "hello" {
		t.Fatalf("out: %q", out)
	}
	if prov.calls != 1 {
		t.Fatalf("calls: %d", prov.calls)
	}
}

func TestLoop_ToolCallThenAssistantResponse(t *testing.T) {
	conv := newStubConv()
	tool := newRecordingTool("greeter", "world")
	prov := &stubProvider{responses: []*providers.LLMResponse{
		toolCallResponse("c1", "greeter", map[string]any{"q": "x"}),
		plainResponse("done"),
	}}
	loop := New(Options{
		Bus:          bus.NewMessageBus(),
		Provider:     prov,
		Conversation: conv,
		Tools:        []tools.Tool{tool},
	})
	out, err := loop.ProcessDirect(context.Background(), "hi", "cli:direct", "cli", "direct", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if out != "done" {
		t.Fatalf("out: %q", out)
	}
	if tool.called != 1 {
		t.Fatalf("tool called %d", tool.called)
	}
	if prov.calls != 2 {
		t.Fatalf("provider calls: %d", prov.calls)
	}
}

func TestLoop_MaxIterationsReached(t *testing.T) {
	// Always return tool calls so the loop never finalizes naturally.
	conv := newStubConv()
	tool := newRecordingTool("loopy", "still going")
	responses := make([]*providers.LLMResponse, 5)
	for i := range responses {
		responses[i] = toolCallResponse("c1", "loopy", nil)
	}
	prov := &stubProvider{responses: responses}
	loop := New(Options{
		Bus:           bus.NewMessageBus(),
		Provider:      prov,
		Conversation:  conv,
		Tools:         []tools.Tool{tool},
		MaxIterations: 3,
	})
	out, _ := loop.ProcessDirect(context.Background(), "hi", "cli:direct", "cli", "direct", nil, "")
	if !strings.Contains(out, "maximum number of tool call iterations") {
		t.Fatalf("expected limit message, got: %s", out)
	}
}

func TestLoop_NewSlashCommand(t *testing.T) {
	conv := newStubConv()
	prov := &stubProvider{responses: []*providers.LLMResponse{plainResponse("ignored")}}
	loop := New(Options{
		Bus:          bus.NewMessageBus(),
		Provider:     prov,
		Conversation: conv,
	})
	out, err := loop.ProcessDirect(context.Background(), "/new", "cli:direct", "cli", "direct", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if out != "New session started." {
		t.Fatalf("out: %q", out)
	}
	if !conv.cleared {
		t.Fatal("conv not cleared")
	}
}

func TestLoop_HelpSlashCommand(t *testing.T) {
	conv := newStubConv()
	prov := &stubProvider{}
	loop := New(Options{
		Bus:          bus.NewMessageBus(),
		Provider:     prov,
		Conversation: conv,
	})
	out, err := loop.ProcessDirect(context.Background(), "/help", "cli:direct", "cli", "direct", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "exoclaw commands") {
		t.Fatalf("out: %q", out)
	}
}

func TestLoop_StripThinkTags(t *testing.T) {
	conv := newStubConv()
	prov := &stubProvider{responses: []*providers.LLMResponse{
		plainResponse("<think>internal</think>visible"),
	}}
	loop := New(Options{Bus: bus.NewMessageBus(), Provider: prov, Conversation: conv})
	out, _ := loop.ProcessDirect(context.Background(), "hi", "cli:direct", "cli", "direct", nil, "")
	if out != "visible" {
		t.Fatalf("think not stripped: %q", out)
	}
}

func TestLoop_ToolHintFormatting(t *testing.T) {
	calls := []providers.ToolCallRequest{
		{Name: "search", Arguments: map[string]any{"q": "hello"}},
	}
	if h := toolHint(calls); h != `search("hello")` {
		t.Fatalf("hint: %q", h)
	}
	// Long arg gets truncated with ellipsis.
	long := strings.Repeat("a", 60)
	calls[0].Arguments["q"] = long
	if h := toolHint(calls); !strings.Contains(h, "…") {
		t.Fatalf("expected ellipsis: %q", h)
	}
}

// ----------------------------------------------------------------------
// Bus integration: Run / Stop
// ----------------------------------------------------------------------

func TestLoop_RunDispatchesInboundMessage(t *testing.T) {
	conv := newStubConv()
	prov := &stubProvider{responses: []*providers.LLMResponse{plainResponse("hello")}}
	b := bus.NewMessageBus()
	loop := New(Options{
		Bus:          b,
		Provider:     prov,
		Conversation: conv,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go loop.Run(ctx)
	defer loop.Stop()

	in := bus.NewInboundMessage("cli", "u", "direct", "hi")
	if err := b.PublishInbound(ctx, in); err != nil {
		t.Fatal(err)
	}
	out, err := b.ConsumeOutbound(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out.Content != "hello" {
		t.Fatalf("out: %s", out.Content)
	}
}
