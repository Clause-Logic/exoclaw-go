package tools

import (
	"context"
	"strings"
	"testing"
)

// Ported from tests/test_tool_validation.py.

func sampleSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"query": map[string]any{"type": "string", "minLength": 2},
			"count": map[string]any{"type": "integer", "minimum": 1, "maximum": 10},
			"mode":  map[string]any{"type": "string", "enum": []any{"fast", "full"}},
			"meta": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"tag":   map[string]any{"type": "string"},
					"flags": map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				},
				"required": []any{"tag"},
			},
		},
		"required": []any{"query", "count"},
	}
}

type sampleTool struct{ ToolBase }

func newSampleTool() *sampleTool {
	t := &sampleTool{}
	t.NameField = "sample"
	t.DescriptionField = "sample tool"
	t.ParametersField = sampleSchema()
	return t
}

func (t *sampleTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	return "ok", nil
}

func containsAny(errs []string, sub string) bool {
	for _, e := range errs {
		if strings.Contains(e, sub) {
			return true
		}
	}
	return false
}

func TestValidate_MissingRequired(t *testing.T) {
	errs := newSampleTool().ValidateParams(map[string]any{"query": "hi"})
	if !containsAny(errs, "missing required count") {
		t.Fatalf("got %v", errs)
	}
}

func TestValidate_TypeAndRange(t *testing.T) {
	tool := newSampleTool()
	errs := tool.ValidateParams(map[string]any{"query": "hi", "count": 0})
	if !containsAny(errs, "count must be >= 1") {
		t.Fatalf("got %v", errs)
	}
	errs = tool.ValidateParams(map[string]any{"query": "hi", "count": "2"})
	if !containsAny(errs, "count should be integer") {
		t.Fatalf("got %v", errs)
	}
}

func TestValidate_EnumAndMinLength(t *testing.T) {
	errs := newSampleTool().ValidateParams(map[string]any{
		"query": "h", "count": 2, "mode": "slow",
	})
	if !containsAny(errs, "query must be at least 2 chars") {
		t.Fatalf("missing minLength err: %v", errs)
	}
	if !containsAny(errs, "mode must be one of") {
		t.Fatalf("missing enum err: %v", errs)
	}
}

func TestValidate_NestedObjectAndArray(t *testing.T) {
	errs := newSampleTool().ValidateParams(map[string]any{
		"query": "hi",
		"count": 2,
		"meta":  map[string]any{"flags": []any{1, "ok"}},
	})
	if !containsAny(errs, "missing required meta.tag") {
		t.Fatalf("got %v", errs)
	}
	if !containsAny(errs, "meta.flags[0] should be string") {
		t.Fatalf("got %v", errs)
	}
}

func TestValidate_IgnoresUnknownFields(t *testing.T) {
	errs := newSampleTool().ValidateParams(map[string]any{"query": "hi", "count": 2, "extra": "x"})
	if len(errs) != 0 {
		t.Fatalf("got %v", errs)
	}
}

func TestCast_StringToInt(t *testing.T) {
	tool := &sampleTool{}
	tool.ParametersField = map[string]any{
		"type":       "object",
		"properties": map[string]any{"count": map[string]any{"type": "integer"}},
	}
	out := tool.CastParams(map[string]any{"count": "42"})
	if v, _ := out["count"].(int64); v != 42 {
		t.Fatalf("got %v (%T)", out["count"], out["count"])
	}
}

func TestCast_StringToBool(t *testing.T) {
	tool := &sampleTool{}
	tool.ParametersField = map[string]any{
		"type":       "object",
		"properties": map[string]any{"flag": map[string]any{"type": "boolean"}},
	}
	cases := map[string]bool{"true": true, "True": true, "1": true, "yes": true,
		"false": false, "False": false, "0": false, "no": false}
	for in, want := range cases {
		got := tool.CastParams(map[string]any{"flag": in})["flag"]
		if got != want {
			t.Errorf("cast %q -> %v want %v", in, got, want)
		}
	}
	// invalid preserved
	if v := tool.CastParams(map[string]any{"flag": "maybe"})["flag"]; v != "maybe" {
		t.Errorf("invalid not preserved: %v", v)
	}
}

func TestCast_BoolNotCastToInt(t *testing.T) {
	tool := &sampleTool{}
	tool.ParametersField = map[string]any{
		"type":       "object",
		"properties": map[string]any{"count": map[string]any{"type": "integer"}},
	}
	out := tool.CastParams(map[string]any{"count": true})
	if out["count"] != true {
		t.Fatalf("bool got cast: %v", out["count"])
	}
	errs := tool.ValidateParams(out)
	if !containsAny(errs, "count should be integer") {
		t.Fatalf("expected validation err: %v", errs)
	}
}

func TestCast_ArrayItems(t *testing.T) {
	tool := &sampleTool{}
	tool.ParametersField = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"nums": map[string]any{"type": "array", "items": map[string]any{"type": "integer"}},
		},
	}
	out := tool.CastParams(map[string]any{"nums": []any{"1", "2", "3"}})
	arr, _ := out["nums"].([]any)
	if len(arr) != 3 {
		t.Fatal("len")
	}
	for i, want := range []int64{1, 2, 3} {
		if v, _ := arr[i].(int64); v != want {
			t.Errorf("idx %d: got %v want %d", i, arr[i], want)
		}
	}
}

func TestToSchema(t *testing.T) {
	tool := newSampleTool()
	s := ToSchema(tool)
	if s["type"] != "function" {
		t.Fatal("type")
	}
	fn, _ := s["function"].(map[string]any)
	if fn["name"] != "sample" {
		t.Fatal("name")
	}
}
