package tools

import (
	"context"
	"strings"
	"testing"
)

// Ported from tests/test_registry_coverage.py.

type echoTool struct {
	ToolBase
	called int
}

func newEchoTool(name string) *echoTool {
	t := &echoTool{}
	t.NameField = name
	t.DescriptionField = "echoes"
	t.ParametersField = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"msg": map[string]any{"type": "string"},
		},
	}
	return t
}

func (t *echoTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	t.called++
	s, _ := params["msg"].(string)
	return "echo:" + s, nil
}

func TestRegistry_RegisterGetHas(t *testing.T) {
	r := NewToolRegistry()
	tool := newEchoTool("e1")
	r.Register(tool, false)
	if !r.Has("e1") {
		t.Fatal("Has")
	}
	if got := r.Get("e1"); got == nil {
		t.Fatal("Get")
	}
	if r.Get("missing") != nil {
		t.Fatal("Get missing")
	}
	if names := r.ToolNames(); len(names) != 1 || names[0] != "e1" {
		t.Fatal("ToolNames")
	}
}

func TestRegistry_Unregister(t *testing.T) {
	r := NewToolRegistry()
	r.Register(newEchoTool("e"), false)
	r.Unregister("e")
	if r.Has("e") {
		t.Fatal("still present")
	}
}

func TestRegistry_GetDefinitionsOptional(t *testing.T) {
	r := NewToolRegistry()
	r.Register(newEchoTool("base"), false)
	r.Register(newEchoTool("opt"), true)

	defs := r.GetDefinitions(nil)
	if len(defs) != 1 {
		t.Fatalf("default should hide optional; got %d", len(defs))
	}
	defs = r.GetDefinitions(map[string]struct{}{"opt": {}})
	if len(defs) != 2 {
		t.Fatalf("include should surface optional; got %d", len(defs))
	}
}

func TestRegistry_Execute(t *testing.T) {
	r := NewToolRegistry()
	tool := newEchoTool("e")
	r.Register(tool, false)
	out, err := r.Execute(context.Background(), "e", map[string]any{"msg": "hi"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out != "echo:hi" {
		t.Fatalf("out: %s", out)
	}
	if tool.called != 1 {
		t.Fatal("not called")
	}
}

func TestRegistry_ExecuteUnknown(t *testing.T) {
	r := NewToolRegistry()
	r.Register(newEchoTool("e"), false)
	out, err := r.Execute(context.Background(), "missing", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "not found") {
		t.Fatalf("expected not-found: %s", out)
	}
}

func TestRegistry_ExecuteValidation(t *testing.T) {
	r := NewToolRegistry()
	r.Register(newSampleTool(), false)
	out, err := r.Execute(context.Background(), "sample", map[string]any{"query": "hi"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "Invalid parameters") {
		t.Fatalf("expected invalid: %s", out)
	}
}

type ctxTool struct{ ToolBase }

func newCtxTool() *ctxTool {
	t := &ctxTool{}
	t.NameField = "ctx"
	t.ParametersField = map[string]any{"type": "object", "properties": map[string]any{}}
	return t
}

func (t *ctxTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	return "plain", nil
}

func (t *ctxTool) ExecuteWithContext(ctx context.Context, tctx *ToolContext, params map[string]any) (string, error) {
	return "ctx:" + tctx.SessionKey, nil
}

func TestRegistry_ContextualPrefersWithContext(t *testing.T) {
	r := NewToolRegistry()
	r.Register(newCtxTool(), false)
	tctx := &ToolContext{SessionKey: "sk", Channel: "c", ChatID: "ch"}
	out, err := r.Execute(context.Background(), "ctx", nil, tctx)
	if err != nil {
		t.Fatal(err)
	}
	if out != "ctx:sk" {
		t.Fatalf("got %s", out)
	}
	// Without tctx, falls back to Execute.
	out, _ = r.Execute(context.Background(), "ctx", nil, nil)
	if out != "plain" {
		t.Fatalf("fallback: %s", out)
	}
}

func TestRegistry_DispatchBinding(t *testing.T) {
	r := NewToolRegistry()
	var sawReg *ToolRegistry
	tool := &fnTool{
		name: "fn",
		fn: func(ctx context.Context, params map[string]any) (string, error) {
			sawReg = CurrentFromContext(ctx)
			return "ok", nil
		},
	}
	r.Register(tool, false)
	_, _ = r.Execute(context.Background(), "fn", nil, nil)
	if sawReg != r {
		t.Fatal("dispatch registry not bound")
	}
	// No binding outside of Execute.
	if CurrentFromContext(context.Background()) != nil {
		t.Fatal("leaked")
	}
}

type fnTool struct {
	name string
	fn   func(ctx context.Context, params map[string]any) (string, error)
}

func (t *fnTool) Name() string                 { return t.name }
func (t *fnTool) Description() string          { return "" }
func (t *fnTool) Parameters() map[string]any   { return map[string]any{"type": "object"} }
func (t *fnTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	return t.fn(ctx, params)
}

func TestRegistry_ErrorHintApplied(t *testing.T) {
	r := NewToolRegistry()
	r.Register(&fnTool{
		name: "fn",
		fn: func(ctx context.Context, params map[string]any) (string, error) {
			return "Error: boom", nil
		},
	}, false)
	out, _ := r.Execute(context.Background(), "fn", nil, nil)
	if !strings.HasSuffix(out, "[Analyze the error above and try a different approach.]") {
		t.Fatalf("missing hint: %q", out)
	}
}
