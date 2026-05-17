// Package tools contains the Tool protocol, ToolContext, ToolBase mixin,
// and ToolRegistry.
//
// Ported from exoclaw/agent/tools/protocol.py and exoclaw/agent/tools/registry.py.
package tools

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Executor is the minimal interface the agent loop's executor exposes to
// tools that opt into ctx.executor.<...>. The full Executor interface lives
// in exoclaw/executor; this package keeps a narrow forward declaration to
// avoid a cyclic import.
type Executor interface{}

// ToolContext is the session context passed to tools that implement
// ExecuteWithContext. Tools that don't need it implement Execute(...) and
// the registry handles both.
type ToolContext struct {
	SessionKey string
	Channel    string
	ChatID     string
	Executor   Executor // optional; gives tools durable I/O access
}

// Tool is the structural protocol for agent tools.
//
// External packages implement this without inheriting from any exoclaw type.
// Use ToolBase as an optional helper to get cast/validate/schema utilities.
type Tool interface {
	Name() string
	Description() string
	Parameters() map[string]any
	Execute(ctx context.Context, params map[string]any) (string, error)
}

// ContextualTool is an optional capability: tools that want session context
// implement ExecuteWithContext. ToolRegistry.Execute prefers this method
// when both the tool implements it and a ToolContext is provided.
type ContextualTool interface {
	ExecuteWithContext(ctx context.Context, tctx *ToolContext, params map[string]any) (string, error)
}

// StreamingTool is the optional Step-D streaming capability.
//
// Tools that produce multi-MB results may implement ExecuteStreaming. The
// executor drains the channel into a per-turn scratch file as chunks arrive,
// avoiding materialising the full result as one Go string. Closing the
// channel signals end of stream. Push an error via the second-return path
// (return immediately with err non-nil) — chunks already pushed are kept.
type StreamingTool interface {
	ExecuteStreaming(ctx context.Context, params map[string]any) (<-chan string, error)
}

// InboundAware lets tools observe inbound messages for state updates.
type InboundAware interface {
	OnInbound(msg any)
}

// SystemContextual lets tools contribute a dynamic system-prompt section.
type SystemContextual interface {
	SystemContext() string
}

// SessionCancellable lets tools cancel session-scoped background work
// (subagents, cron tasks, …) when /stop is received.
type SessionCancellable interface {
	CancelBySession(sessionKey string) int
}

// SentInTurnTool tells the agent loop the tool already published the reply
// — the loop skips its default outbound send.
type SentInTurnTool interface {
	SentInTurn() bool
}

// BusAware lets tools receive a reference to the agent bus at registration.
type BusAware interface {
	SetBus(b any)
}

// RegistryAware lets tools receive a reference to their registry at
// registration. Prefer ToolRegistry.Current() inside tool bodies — see the
// registry docs for why.
type RegistryAware interface {
	SetRegistry(r *ToolRegistry)
}

// ParamCaster lets tools normalize params before validation.
type ParamCaster interface {
	CastParams(params map[string]any) map[string]any
}

// ParamValidator lets tools validate params against their schema.
type ParamValidator interface {
	ValidateParams(params map[string]any) []string
}

// ----------------------------------------------------------------------
// ToolBase — optional mixin for tool authors.
//
// Provides CastParams, ValidateParams, and ToSchema utilities. Compose this
// into your tool with embedding and override Name / Description / Parameters
// (or supply them as plain fields).
// ----------------------------------------------------------------------

// ToolBase carries the parameter-handling helpers and exposes Name /
// Description / Parameters via embedded fields. Concrete tools embed
// ToolBase and supply Execute (and optionally ExecuteWithContext or
// ExecuteStreaming).
type ToolBase struct {
	NameField        string
	DescriptionField string
	ParametersField  map[string]any
}

func (t *ToolBase) Name() string                  { return t.NameField }
func (t *ToolBase) Description() string           { return t.DescriptionField }
func (t *ToolBase) Parameters() map[string]any    { return t.ParametersField }

// CastParams applies safe schema-driven casts before validation.
func (t *ToolBase) CastParams(params map[string]any) map[string]any {
	schema := t.ParametersField
	if schema == nil {
		return params
	}
	typ, _ := schema["type"].(string)
	if typ != "" && typ != "object" {
		return params
	}
	return t.castObject(params, schema)
}

func (t *ToolBase) castObject(obj any, schema map[string]any) map[string]any {
	m, ok := obj.(map[string]any)
	if !ok {
		return nil
	}
	props, _ := schema["properties"].(map[string]any)
	out := make(map[string]any, len(m))
	if props == nil {
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	for k, v := range m {
		if ps, ok := props[k].(map[string]any); ok {
			out[k] = t.castValue(v, ps)
		} else {
			out[k] = v
		}
	}
	return out
}

func (t *ToolBase) castValue(val any, schema map[string]any) any {
	target, _ := schema["type"].(string)
	if target == "" {
		return val
	}
	switch target {
	case "boolean":
		if _, ok := val.(bool); ok {
			return val
		}
		if s, ok := val.(string); ok {
			switch strings.ToLower(s) {
			case "true", "1", "yes":
				return true
			case "false", "0", "no":
				return false
			}
		}
		return val
	case "integer":
		if _, ok := val.(bool); ok {
			return val
		}
		switch v := val.(type) {
		case int, int32, int64:
			return v
		case float64:
			if v == float64(int64(v)) {
				return int64(v)
			}
			return v
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
			return v
		}
		return val
	case "number":
		if _, ok := val.(bool); ok {
			return val
		}
		switch v := val.(type) {
		case int, int32, int64, float32, float64:
			return v
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
			return v
		}
		return val
	case "string":
		if val == nil {
			return val
		}
		if s, ok := val.(string); ok {
			return s
		}
		return fmt.Sprint(val)
	case "array":
		if list, ok := val.([]any); ok {
			if items, ok := schema["items"].(map[string]any); ok {
				out := make([]any, len(list))
				for i, item := range list {
					out[i] = t.castValue(item, items)
				}
				return out
			}
			return list
		}
		return val
	case "object":
		if m, ok := val.(map[string]any); ok {
			return t.castObject(m, schema)
		}
		return val
	}
	return val
}

// ValidateParams validates against the embedded JSON schema.
// Returns the (possibly-empty) list of error messages.
func (t *ToolBase) ValidateParams(params map[string]any) []string {
	if params == nil {
		// In Python an isinstance(params, dict) check returns False for
		// non-dicts; we mirror that as "must be an object".
		return []string{"parameters must be an object"}
	}
	schema := t.ParametersField
	if schema == nil {
		schema = map[string]any{}
	}
	if typ, ok := schema["type"].(string); ok && typ != "" && typ != "object" {
		panic(fmt.Sprintf("Schema must be object type, got %q", typ))
	}
	// Force "object" so the recursive validator treats the top-level value
	// as such (matches the Python ``schema_with_type["type"] = "object"``).
	merged := make(map[string]any, len(schema)+1)
	for k, v := range schema {
		merged[k] = v
	}
	merged["type"] = "object"
	return t.validate(params, merged, "")
}

func (t *ToolBase) validate(val any, schema map[string]any, path string) []string {
	typ, _ := schema["type"].(string)
	label := path
	if label == "" {
		label = "parameter"
	}

	var errs []string
	switch typ {
	case "integer":
		if !isInteger(val) {
			return []string{fmt.Sprintf("%s should be integer", label)}
		}
	case "number":
		if !isNumber(val) {
			return []string{fmt.Sprintf("%s should be number", label)}
		}
	case "boolean":
		if _, ok := val.(bool); !ok {
			return []string{fmt.Sprintf("%s should be boolean", label)}
		}
	case "string":
		if _, ok := val.(string); !ok {
			return []string{fmt.Sprintf("%s should be string", label)}
		}
	case "array":
		if _, ok := val.([]any); !ok {
			return []string{fmt.Sprintf("%s should be array", label)}
		}
	case "object":
		if _, ok := val.(map[string]any); !ok {
			return []string{fmt.Sprintf("%s should be object", label)}
		}
	}

	if enum, ok := schema["enum"].([]any); ok && enum != nil {
		match := false
		for _, e := range enum {
			if reflect.DeepEqual(e, val) {
				match = true
				break
			}
		}
		if !match {
			errs = append(errs, fmt.Sprintf("%s must be one of %v", label, enum))
		}
	}

	if typ == "integer" || typ == "number" {
		if min, ok := schema["minimum"]; ok && min != nil {
			if compareNumbers(val, min) < 0 {
				errs = append(errs, fmt.Sprintf("%s must be >= %v", label, min))
			}
		}
		if max, ok := schema["maximum"]; ok && max != nil {
			if compareNumbers(val, max) > 0 {
				errs = append(errs, fmt.Sprintf("%s must be <= %v", label, max))
			}
		}
	}
	if typ == "string" {
		s, _ := val.(string)
		if minL, ok := schema["minLength"].(int); ok && len(s) < minL {
			errs = append(errs, fmt.Sprintf("%s must be at least %d chars", label, minL))
		}
		if maxL, ok := schema["maxLength"].(int); ok && len(s) > maxL {
			errs = append(errs, fmt.Sprintf("%s must be at most %d chars", label, maxL))
		}
	}
	if typ == "object" {
		obj, _ := val.(map[string]any)
		props, _ := schema["properties"].(map[string]any)
		if req, ok := schema["required"].([]any); ok {
			for _, r := range req {
				if k, ok := r.(string); ok {
					if _, exists := obj[k]; !exists {
						p := k
						if path != "" {
							p = path + "." + k
						}
						errs = append(errs, "missing required "+p)
					}
				}
			}
		}
		if props != nil {
			for k, v := range obj {
				if ps, ok := props[k].(map[string]any); ok {
					sub := k
					if path != "" {
						sub = path + "." + k
					}
					errs = append(errs, t.validate(v, ps, sub)...)
				}
			}
		}
	}
	if typ == "array" {
		if items, ok := schema["items"].(map[string]any); ok {
			list, _ := val.([]any)
			for i, item := range list {
				sub := fmt.Sprintf("[%d]", i)
				if path != "" {
					sub = fmt.Sprintf("%s[%d]", path, i)
				}
				errs = append(errs, t.validate(item, items, sub)...)
			}
		}
	}

	return errs
}

// ToSchema renders the tool as an OpenAI-style function-tool schema.
func ToSchema(tool Tool) map[string]any {
	return map[string]any{
		"type": "function",
		"function": map[string]any{
			"name":        tool.Name(),
			"description": tool.Description(),
			"parameters":  tool.Parameters(),
		},
	}
}

// ----------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------

func isInteger(v any) bool {
	if _, ok := v.(bool); ok {
		return false
	}
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case float64:
		f := v.(float64)
		return f == float64(int64(f))
	case float32:
		f := float64(v.(float32))
		return f == float64(int64(f))
	}
	return false
}

func isNumber(v any) bool {
	if _, ok := v.(bool); ok {
		return false
	}
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return true
	}
	return false
}

func compareNumbers(a, b any) int {
	af := toFloat64(a)
	bf := toFloat64(b)
	switch {
	case af < bf:
		return -1
	case af > bf:
		return 1
	}
	return 0
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	}
	return 0
}
