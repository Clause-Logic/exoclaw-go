// Package iterationpolicy defines the IterationPolicy protocol — pluggable
// termination strategy for the agent loop.
//
// Ported from exoclaw/iteration_policy.py.
//
// The default behavior (no policy) is a hard max_iterations counter. Provide
// an IterationPolicy to replace that with pattern-based detection, adaptive
// budgets, or any other strategy — without touching the Executor.
package iterationpolicy

import "context"

// IterationPolicy controls when the agent loop should stop iterating.
type IterationPolicy interface {
	// ShouldContinue returns true to allow the next iteration, false to stop.
	// iteration is the number of completed iterations (0 before the first).
	// toolsUsed is the accumulated list of tool names called so far.
	ShouldContinue(ctx context.Context, iteration int, toolsUsed []string) (bool, error)

	// OnLimitReached returns the user-facing message when the loop is terminated.
	OnLimitReached(ctx context.Context, iteration int, toolsUsed []string) (string, error)
}
