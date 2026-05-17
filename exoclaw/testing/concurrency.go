// Package testing contains reusable test assertions for tool authors.
//
// Ported from exoclaw/testing/concurrency.py.
package testing

import (
	"context"
	"fmt"
	"sync"
)

// AssertSetContextIsolatesPerTask runs two concurrent goroutines that each
// SetContext to a different tag, both pause to let the other interleave,
// then ReadContext. Each goroutine must observe its own tag — if not, the
// tool stored per-call state on a shared instance attribute and concurrent
// turns will cross-wire each other's routing.
//
// makeTool returns a fresh tool instance shared across both goroutines.
// setContext binds the tool's per-call destination to tag.
// readContext returns the tool's currently-bound destination.
//
// Returns an error describing the violation, or nil if the tool isolates
// per-goroutine state correctly.
func AssertSetContextIsolatesPerTask[T any](
	makeTool func() T,
	setContext func(ctx context.Context, tool T, tag string) error,
	readContext func(ctx context.Context, tool T) (string, error),
) error {
	tool := makeTool()
	observed := map[string]string{}
	var mu sync.Mutex
	var wg sync.WaitGroup

	// barrier ensures both goroutines have completed their Set() before
	// either is allowed to Read(). For a tool that stores state on a
	// shared instance attribute, the second writer's value is the one
	// both readers see — the classic cross-wire bug. For a tool that
	// keys state by goroutine (or by an explicit ctx parameter), each
	// reader gets its own tag back.
	var barrier sync.WaitGroup
	barrier.Add(2)

	turn := func(tag string) {
		defer wg.Done()
		ctx := context.Background()
		if err := setContext(ctx, tool, tag); err != nil {
			mu.Lock()
			observed[tag] = "ERR:" + err.Error()
			mu.Unlock()
			barrier.Done()
			return
		}
		barrier.Done()
		barrier.Wait()
		v, err := readContext(ctx, tool)
		mu.Lock()
		if err != nil {
			observed[tag] = "ERR:" + err.Error()
		} else {
			observed[tag] = v
		}
		mu.Unlock()
	}

	wg.Add(2)
	go turn("ALPHA")
	go turn("BRAVO")
	wg.Wait()

	if observed["ALPHA"] != "ALPHA" || observed["BRAVO"] != "BRAVO" {
		return fmt.Errorf("goroutines cross-wired their per-call state: "+
			"ALPHA observed %q, BRAVO observed %q. The tool stores per-call "+
			"state on a shared instance attribute instead of keying by "+
			"goroutine / context — concurrent turns will see the wrong "+
			"value (race / shared-state bug).", observed["ALPHA"], observed["BRAVO"])
	}
	return nil
}
