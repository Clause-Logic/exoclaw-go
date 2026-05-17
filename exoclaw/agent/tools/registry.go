package tools

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// dispatchKey is the context-key under which the currently-dispatching
// ToolRegistry is bound, so fan-out tools can look up sibling tools via
// CurrentFromContext. This is the Go-flavored replacement for the per-task
// ContextVar in the Python version.
type dispatchKey struct{}

// WithDispatchRegistry returns a derived context that has reg bound as the
// current dispatch registry. ToolRegistry.Execute calls this for the duration
// of each tool body so fan-out tools can recover the registry without
// holding a stored pointer (which becomes last-write-wins when one Tool
// instance is shared across multiple AgentLoops).
func WithDispatchRegistry(ctx context.Context, reg *ToolRegistry) context.Context {
	return context.WithValue(ctx, dispatchKey{}, reg)
}

// CurrentFromContext returns the registry currently dispatching on ctx, or
// nil if the context has no registry bound.
//
// Prefer this over a stored reference from SetRegistry — a single tool
// instance can be wired into multiple AgentLoops (each with its own
// registry); a stored reference is last-write-wins, while the
// context-bound registry is per-call.
func CurrentFromContext(ctx context.Context) *ToolRegistry {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(dispatchKey{}).(*ToolRegistry)
	return r
}

// ToolRegistry is the registry for agent tools.
//
// Tools may be registered as *optional* — they are loaded and executable but
// hidden from the LLM's tool list by default. Pass their names via the
// include arg to GetDefinitions to surface them for a turn.
type ToolRegistry struct {
	mu       sync.RWMutex
	tools    map[string]Tool
	optional map[string]struct{}
}

// NewToolRegistry constructs an empty registry.
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{
		tools:    map[string]Tool{},
		optional: map[string]struct{}{},
	}
}

// Register adds a tool. If optional is true the tool is hidden from
// GetDefinitions unless its name appears in the include set.
func (r *ToolRegistry) Register(tool Tool, optional bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[tool.Name()] = tool
	if optional {
		r.optional[tool.Name()] = struct{}{}
	}
}

// Unregister removes a tool by name.
func (r *ToolRegistry) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tools, name)
	delete(r.optional, name)
}

// Get fetches a tool by name or nil if absent.
func (r *ToolRegistry) Get(name string) Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.tools[name]
}

// Has reports whether a tool is registered under name.
func (r *ToolRegistry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.tools[name]
	return ok
}

// ToolNames returns the names of all registered tools.
func (r *ToolRegistry) ToolNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.tools))
	for n := range r.tools {
		names = append(names, n)
	}
	return names
}

// All returns a snapshot of every registered tool (used by the agent loop
// for hook fan-out: OnInbound, SystemContext, CancelBySession, SentInTurn).
func (r *ToolRegistry) All() []Tool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Tool, 0, len(r.tools))
	for _, t := range r.tools {
		out = append(out, t)
	}
	return out
}

// GetDefinitions returns the OpenAI-style schemas for tools the LLM is
// allowed to see this turn. include is the optional-tool names to surface;
// pass nil to surface only non-optional tools.
func (r *ToolRegistry) GetDefinitions(include map[string]struct{}) []map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []map[string]any
	for name, tool := range r.tools {
		if _, isOptional := r.optional[name]; isOptional {
			if include == nil {
				continue
			}
			if _, ok := include[name]; !ok {
				continue
			}
		}
		out = append(out, ToSchema(tool))
	}
	return out
}

const _hint = "\n\n[Analyze the error above and try a different approach.]"

// Execute runs the named tool with the given params under tctx.
//
// If the tool implements ContextualTool and tctx is non-nil, ExecuteWithContext
// is called; otherwise Execute is. Errors returned by the tool propagate to
// the caller so the agent loop can observe them as part of its tool span.
// Domain errors (not found, invalid params) are returned as strings — they
// are normal agent-visible outcomes, not unexpected failures.
//
// Binds r as the dispatch registry on a derived context for the body's
// duration so fan-out tools can look it up via CurrentFromContext.
func (r *ToolRegistry) Execute(ctx context.Context, name string, params map[string]any, tctx *ToolContext) (string, error) {
	resolved, errStr := r.resolve(name, params)
	if errStr != "" {
		return errStr, nil
	}
	tool := resolved.tool
	params = resolved.params

	dispatchCtx := WithDispatchRegistry(ctx, r)

	var (
		result string
		err    error
	)
	if tctx != nil {
		if ctxTool, ok := tool.(ContextualTool); ok {
			result, err = ctxTool.ExecuteWithContext(dispatchCtx, tctx, params)
		} else {
			result, err = tool.Execute(dispatchCtx, params)
		}
	} else {
		result, err = tool.Execute(dispatchCtx, params)
	}
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(result, "Error") {
		return result + _hint, nil
	}
	return result, nil
}

// resolvedTool is the shared output of resolve.
type resolvedTool struct {
	tool   Tool
	params map[string]any
}

// resolve does the shared lookup + cast + validate for both execute paths.
// Returns either a resolved tool/params bundle, or a ready-to-return error
// string. The hint suffix is applied by callers as appropriate.
func (r *ToolRegistry) resolve(name string, params map[string]any) (*resolvedTool, string) {
	r.mu.RLock()
	tool, ok := r.tools[name]
	allNames := make([]string, 0, len(r.tools))
	for n := range r.tools {
		allNames = append(allNames, n)
	}
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Sprintf("Error: Tool '%s' not found. Available: %s", name, strings.Join(allNames, ", "))
	}

	if caster, ok := tool.(ParamCaster); ok {
		params = caster.CastParams(params)
	}
	if validator, ok := tool.(ParamValidator); ok {
		if errs := validator.ValidateParams(params); len(errs) > 0 {
			return nil, fmt.Sprintf("Error: Invalid parameters for tool '%s': %s%s",
				name, strings.Join(errs, "; "), _hint)
		}
	}
	return &resolvedTool{tool: tool, params: params}, ""
}

// StreamDispatch resolves a tool call for the potential streaming path
// without invoking it. Returns either the resolved tool + validated params,
// or a ready-to-return error string. Does NOT check whether the tool
// implements StreamingTool — callers inspect that themselves.
func (r *ToolRegistry) StreamDispatch(name string, params map[string]any) (Tool, map[string]any, string) {
	resolved, errStr := r.resolve(name, params)
	if errStr != "" {
		return nil, nil, errStr
	}
	return resolved.tool, resolved.params, ""
}
