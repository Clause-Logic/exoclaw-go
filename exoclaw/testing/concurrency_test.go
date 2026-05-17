package testing

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// Ported from tests/test_testing_concurrency.py.

type buggyTool struct {
	dest string
}

func (t *buggyTool) Set(v string)  { t.dest = v }
func (t *buggyTool) Read() string { return t.dest }

// correctTool keys per-call state by the current goroutine identifier — the
// Go-idiomatic equivalent of Python's ContextVar pattern. Reading
// runtime.Stack[0..n] to extract the goroutine id is hacky but matches the
// pattern: each goroutine sees only its own value.
type correctTool struct {
	mu sync.Mutex
	m  map[uint64]string
}

func newCorrectTool() *correctTool { return &correctTool{m: map[uint64]string{}} }

func (t *correctTool) Set(v string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.m[goroutineID()] = v
}
func (t *correctTool) Read() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.m[goroutineID()]
}

func goroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// "goroutine NNN [running]: ..."
	var id uint64
	for i := len("goroutine "); i < n; i++ {
		if buf[i] < '0' || buf[i] > '9' {
			break
		}
		id = id*10 + uint64(buf[i]-'0')
	}
	return id
}

func TestHelper_PassesForCtxBackedTool(t *testing.T) {
	err := AssertSetContextIsolatesPerTask(
		newCorrectTool,
		func(_ context.Context, tool *correctTool, tag string) error {
			tool.Set(tag)
			return nil
		},
		func(_ context.Context, tool *correctTool) (string, error) {
			return tool.Read(), nil
		},
	)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestHelper_FailsForInstanceAttrBackedTool(t *testing.T) {
	err := AssertSetContextIsolatesPerTask(
		func() *buggyTool { return &buggyTool{} },
		func(_ context.Context, tool *buggyTool, tag string) error {
			tool.Set(tag)
			return nil
		},
		func(_ context.Context, tool *buggyTool) (string, error) {
			return tool.Read(), nil
		},
	)
	if err == nil {
		t.Fatal("expected helper to flag the bug")
	}
	msg := err.Error()
	if !(strings.Contains(msg, "cross-wired") || strings.Contains(msg, "shared instance") || strings.Contains(msg, "race")) {
		t.Fatalf("expected diagnostic, got: %v", err)
	}
}
