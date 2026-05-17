// Package utils contains general helpers, ported from exoclaw/utils/tasks.py.
//
// The Python original wraps asyncio.create_task to spawn tasks with a fresh
// contextvars.Context (no inheritance of DBOS / structlog state). In Go,
// goroutines do not inherit context, so the corresponding helper is just
// a typed wrapper that runs fn in a fresh goroutine — context propagation
// is the caller's choice.
package utils

import "context"

// CreateIsolatedTask spawns fn in a fresh goroutine with the supplied
// context. Unlike Python's create_isolated_task, the goroutine starts with
// no inherited context-bound values, so the caller passes context.Background()
// (or any chosen ctx) explicitly.
//
// Returns a channel that is closed when fn returns. The returned error, if
// any, is delivered on the error channel.
func CreateIsolatedTask(fn func(ctx context.Context) error) (done <-chan struct{}, errCh <-chan error) {
	doneCh := make(chan struct{})
	errChOut := make(chan error, 1)
	go func() {
		defer close(doneCh)
		errChOut <- fn(context.Background())
	}()
	return doneCh, errChOut
}
