package utils

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Ported from tests/test_tasks_isolation.py + test_utils_coverage.py.

func TestCreateIsolatedTask_RunsFn(t *testing.T) {
	done, errCh := CreateIsolatedTask(func(ctx context.Context) error {
		return nil
	})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("never finished")
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestCreateIsolatedTask_PropagatesError(t *testing.T) {
	sentinel := errors.New("boom")
	_, errCh := CreateIsolatedTask(func(ctx context.Context) error { return sentinel })
	if err := <-errCh; !errors.Is(err, sentinel) {
		t.Fatalf("err: %v", err)
	}
}

func TestCreateIsolatedTask_StartsWithFreshContext(t *testing.T) {
	// Verify the wrapper never inherits the caller's context (Go has no
	// implicit context propagation, but this codifies the contract).
	type key struct{}
	parent := context.WithValue(context.Background(), key{}, "parent-binding")

	captured := make(chan any, 1)
	_, errCh := CreateIsolatedTask(func(ctx context.Context) error {
		captured <- ctx.Value(key{})
		return nil
	})
	if v := <-captured; v != nil {
		t.Fatalf("expected nil, got %v", v)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	// Confirm the parent context was untouched.
	if parent.Value(key{}) != "parent-binding" {
		t.Fatal("parent ctx mutated")
	}
}
