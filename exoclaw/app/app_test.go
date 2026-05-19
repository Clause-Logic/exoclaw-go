package app

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Clause-Logic/exoclaw-go/exoclaw/bus"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/channels"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/conversation"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/providers"
)

// Ported from tests/test_app_coverage.py.

type stubConv struct{}

func (c *stubConv) BuildPrompt(ctx context.Context, sid, msg string, opts conversation.BuildPromptOptions) ([]map[string]any, error) {
	return []map[string]any{{"role": "user", "content": msg}}, nil
}
func (c *stubConv) Record(ctx context.Context, sid string, msgs []map[string]any) error { return nil }
func (c *stubConv) Clear(ctx context.Context, sid string) (bool, error)                 { return true, nil }
func (c *stubConv) ListSessions() []map[string]any                                       { return nil }

type stubProv struct{}

func (p *stubProv) Chat(ctx context.Context, msgs []map[string]any, params providers.ChatParams) (*providers.LLMResponse, error) {
	hello := "ok"
	return &providers.LLMResponse{Content: &hello, FinishReason: "stop"}, nil
}
func (p *stubProv) GetDefaultModel() string { return "test-model" }

type silentChannel struct {
	name    string
	started sync.WaitGroup
	stopped bool
	mu      sync.Mutex
}

func (c *silentChannel) Name() string { return c.name }
func (c *silentChannel) Start(ctx context.Context, b bus.Bus) error {
	c.started.Done()
	<-ctx.Done()
	return nil
}
func (c *silentChannel) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	return nil
}
func (c *silentChannel) Send(ctx context.Context, msg *bus.OutboundMessage) error { return nil }

func TestApp_RunStartsAllChannelsAndStops(t *testing.T) {
	ch := &silentChannel{name: "cli"}
	ch.started.Add(1)
	app := New(Options{
		Provider:     &stubProv{},
		Conversation: &stubConv{},
		Channels:     []channels.Channel{ch},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- app.Run(ctx) }()
	ch.started.Wait()
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run never returned")
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if !ch.stopped {
		t.Fatal("channel not stopped")
	}
}

func TestApp_DefaultsApplied(t *testing.T) {
	app := New(Options{
		Provider:     &stubProv{},
		Conversation: &stubConv{},
	})
	if app.Temperature != 0.1 || app.MaxTokens != 8192 || app.MaxIterations != 40 {
		t.Fatalf("defaults: temp=%v tokens=%d iters=%d", app.Temperature, app.MaxTokens, app.MaxIterations)
	}
	if app.Log == nil {
		t.Fatal("default logger missing")
	}
}
