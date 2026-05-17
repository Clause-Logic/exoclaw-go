package iterationpolicy

import (
	"context"
	"testing"
)

// Lightweight conformance check — the package only declares the interface;
// behavioural tests live in the agent loop suite.

type cappedPolicy struct{ cap int }

func (p *cappedPolicy) ShouldContinue(_ context.Context, iter int, _ []string) (bool, error) {
	return iter < p.cap, nil
}
func (p *cappedPolicy) OnLimitReached(_ context.Context, _ int, _ []string) (string, error) {
	return "limit", nil
}

func TestPolicyInterface(t *testing.T) {
	var p IterationPolicy = &cappedPolicy{cap: 3}
	ok, err := p.ShouldContinue(context.Background(), 2, nil)
	if err != nil || !ok {
		t.Fatalf("got %v %v", ok, err)
	}
	ok, _ = p.ShouldContinue(context.Background(), 3, nil)
	if ok {
		t.Fatal("should stop at cap")
	}
	msg, _ := p.OnLimitReached(context.Background(), 3, nil)
	if msg != "limit" {
		t.Fatalf("msg: %s", msg)
	}
}
