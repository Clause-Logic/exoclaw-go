package agent

import (
	"context"
	"reflect"
	"testing"

	"github.com/Clause-Logic/exoclaw-go/exoclaw/agent/tools"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/bus"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/conversation"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/providers"
)

// Ported from tests/test_hooks.py. The loop asks the Conversation for one
// decision per seam (before_tool / before_finish) and applies it; it has no
// opinion on what produces that decision. These tests use a decider-bearing
// conversation that returns a single decision directly, proving the loop
// applies mutate/veto/inject, exposes a read-only run_context, and that a
// conversation WITHOUT the decider interfaces is unaffected (the common case).

// hookConv is a stubConversation that also implements the optional decider
// interfaces, delegating to the funcs it was handed.
type hookConv struct {
	*stubConversation
	beforeTool   func(ctx context.Context, hc *HookContext) (*BeforeToolResult, error)
	beforeFinish func(ctx context.Context, hc *HookContext) (*BeforeFinishResult, error)
	runCtx       map[string]any
}

func (c *hookConv) BeforeTool(ctx context.Context, hc *HookContext) (*BeforeToolResult, error) {
	if c.beforeTool != nil {
		return c.beforeTool(ctx, hc)
	}
	return nil, nil
}

func (c *hookConv) BeforeFinish(ctx context.Context, hc *HookContext) (*BeforeFinishResult, error) {
	if c.beforeFinish != nil {
		return c.beforeFinish(ctx, hc)
	}
	return nil, nil
}

func (c *hookConv) RunContext() map[string]any { return c.runCtx }

// argCapturingTool records the params it was actually invoked with.
type argCapturingTool struct {
	tools.ToolBase
	called    int
	gotParams map[string]any
}

func newArgCapturingTool(name string) *argCapturingTool {
	t := &argCapturingTool{}
	t.NameField = name
	t.ParametersField = map[string]any{"type": "object", "properties": map[string]any{}}
	return t
}

func (t *argCapturingTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	t.called++
	t.gotParams = params
	return "tool-ok", nil
}

func runLoop(t *testing.T, conv conversation.Conversation, responses []*providers.LLMResponse, tool tools.Tool) string {
	t.Helper()
	prov := &stubProvider{responses: responses}
	opts := Options{Bus: bus.NewMessageBus(), Provider: prov, Conversation: conv}
	if tool != nil {
		opts.Tools = []tools.Tool{tool}
	}
	loop := New(opts)
	out, err := loop.ProcessDirect(context.Background(), "go", "cli:direct", "cli", "direct", nil, "")
	if err != nil {
		t.Fatalf("ProcessDirect: %v", err)
	}
	return out
}

func TestLoop_BeforeTool_StampsFromRunContext(t *testing.T) {
	// A before_tool decider reads the authoritative cycle id from run_context
	// and stamps it onto the tool args — the tool runs with the stamped args,
	// not whatever the model passed.
	tool := newArgCapturingTool("do")
	conv := &hookConv{
		stubConversation: newStubConv(),
		runCtx:           map[string]any{"cycle_id": "C1"},
		beforeTool: func(ctx context.Context, hc *HookContext) (*BeforeToolResult, error) {
			p := map[string]any{}
			for k, v := range hc.Params {
				p[k] = v
			}
			p["cycle_id"] = hc.RunContext["cycle_id"]
			return &BeforeToolResult{Params: p}, nil
		},
	}
	out := runLoop(t, conv, []*providers.LLMResponse{
		toolCallResponse("tc1", "do", map[string]any{"q": "x"}),
		plainResponse("final"),
	}, tool)
	if out != "final" {
		t.Fatalf("out = %q, want final", out)
	}
	want := map[string]any{"q": "x", "cycle_id": "C1"}
	if !reflect.DeepEqual(tool.gotParams, want) {
		t.Fatalf("tool params = %v, want %v", tool.gotParams, want)
	}
}

func TestLoop_BeforeTool_Vetoes(t *testing.T) {
	// Block=true refuses the tool; the tool never runs and the model sees the
	// reason as the result.
	tool := newArgCapturingTool("web_search")
	conv := &hookConv{
		stubConversation: newStubConv(),
		beforeTool: func(ctx context.Context, hc *HookContext) (*BeforeToolResult, error) {
			return &BeforeToolResult{Block: true, BlockReason: "budget spent — write up findings"}, nil
		},
	}
	out := runLoop(t, conv, []*providers.LLMResponse{
		toolCallResponse("tc1", "web_search", map[string]any{}),
		plainResponse("final"),
	}, tool)
	if out != "final" {
		t.Fatalf("out = %q, want final", out)
	}
	if tool.called != 0 {
		t.Fatalf("tool called %d times, want 0 (vetoed)", tool.called)
	}
}

func TestLoop_BeforeFinish_InjectsAndContinues(t *testing.T) {
	// A before_finish decider re-prompts a model that stopped; the loop
	// continues and ends on the next response.
	var seen int
	conv := &hookConv{
		stubConversation: newStubConv(),
		beforeFinish: func(ctx context.Context, hc *HookContext) (*BeforeFinishResult, error) {
			seen++
			if seen == 1 {
				return &BeforeFinishResult{ContinueMessage: "call finish first"}, nil
			}
			return nil, nil
		},
	}
	out := runLoop(t, conv, []*providers.LLMResponse{
		plainResponse("partial"),
		plainResponse("done"),
	}, nil)
	if out != "done" {
		t.Fatalf("out = %q, want done", out)
	}
	if seen != 2 {
		t.Fatalf("before_finish fired %d times, want 2 (continued after the first)", seen)
	}
}

func TestLoop_HookCannotCorruptRunContext(t *testing.T) {
	// A decider mutating hc.RunContext must not touch the conversation's own
	// bag — the loop hands it a shallow copy, not the live map.
	src := map[string]any{"cycle_id": "C1"}
	conv := &hookConv{
		stubConversation: newStubConv(),
		runCtx:           src,
		beforeTool: func(ctx context.Context, hc *HookContext) (*BeforeToolResult, error) {
			hc.RunContext["evil"] = "injected" // in-place
			return nil, nil
		},
	}
	runLoop(t, conv, []*providers.LLMResponse{
		toolCallResponse("tc1", "do", map[string]any{}),
		plainResponse("final"),
	}, newArgCapturingTool("do"))
	if _, leaked := src["evil"]; leaked {
		t.Fatalf("decider mutation leaked into the conversation's run_context bag: %v", src)
	}
}

func TestLoop_NoDecider_NoOp(t *testing.T) {
	// A conversation WITHOUT the decider interfaces (the common case) is
	// unaffected — the tool runs with the model's args, unstamped.
	tool := newArgCapturingTool("do")
	out := runLoop(t, newStubConv(), []*providers.LLMResponse{
		toolCallResponse("tc1", "do", map[string]any{"q": "x"}),
		plainResponse("final"),
	}, tool)
	if out != "final" {
		t.Fatalf("out = %q, want final", out)
	}
	want := map[string]any{"q": "x"}
	if !reflect.DeepEqual(tool.gotParams, want) {
		t.Fatalf("tool params = %v, want %v (unstamped)", tool.gotParams, want)
	}
}

func TestLoop_BeforeTool_InPlaceMutationDoesNotChangeCall(t *testing.T) {
	// Mutating hc.Params in place must NOT change the call — only an explicit
	// BeforeToolResult.Params does. The loop hands the decider a copy.
	tool := newArgCapturingTool("do")
	conv := &hookConv{
		stubConversation: newStubConv(),
		beforeTool: func(ctx context.Context, hc *HookContext) (*BeforeToolResult, error) {
			hc.Params["evil"] = "injected" // in-place, no result returned
			return nil, nil
		},
	}
	runLoop(t, conv, []*providers.LLMResponse{
		toolCallResponse("tc1", "do", map[string]any{"q": "x"}),
		plainResponse("final"),
	}, tool)
	want := map[string]any{"q": "x"}
	if !reflect.DeepEqual(tool.gotParams, want) {
		t.Fatalf("tool params = %v, want %v (in-place mutation must not leak)", tool.gotParams, want)
	}
}
