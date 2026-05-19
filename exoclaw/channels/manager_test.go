package channels

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Clause-Logic/exoclaw-go/exoclaw/bus"
)

// Ported from tests/test_channel_manager_coverage.py and micro/test_channels.py.

type fakeChannel struct {
	name    string
	started bool
	stopped bool
	sent    []*bus.OutboundMessage
	mu      sync.Mutex
	startErr error
	sendErr  error
}

func (c *fakeChannel) Name() string { return c.name }
func (c *fakeChannel) Start(ctx context.Context, b bus.Bus) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.started = true
	return c.startErr
}
func (c *fakeChannel) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	return nil
}
func (c *fakeChannel) Send(ctx context.Context, msg *bus.OutboundMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, msg)
	return c.sendErr
}

func TestManager_StartAllStartsEveryChannel(t *testing.T) {
	c1, c2 := &fakeChannel{name: "a"}, &fakeChannel{name: "b"}
	m := NewChannelManager([]Channel{c1, c2}, bus.NewMessageBus(), false, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go m.StartAll(ctx)
	// Allow the goroutines to start.
	time.Sleep(100 * time.Millisecond)
	c1.mu.Lock()
	c2.mu.Lock()
	if !c1.started || !c2.started {
		t.Fatalf("started: c1=%v c2=%v", c1.started, c2.started)
	}
	c1.mu.Unlock()
	c2.mu.Unlock()
	cancel()
	_ = m.StopAll(context.Background())
}

func TestManager_DispatchOutboundRoutesByChannel(t *testing.T) {
	c := &fakeChannel{name: "cli"}
	b := bus.NewMessageBus()
	m := NewChannelManager([]Channel{c}, b, false, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.StartAll(ctx)
	// Allow dispatcher to start.
	time.Sleep(50 * time.Millisecond)
	if err := b.PublishOutbound(ctx, &bus.OutboundMessage{Channel: "cli", ChatID: "x", Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sent) != 1 || c.sent[0].Content != "hello" {
		t.Fatalf("sent: %v", c.sent)
	}
}

func TestManager_FilterToolHints(t *testing.T) {
	c := &fakeChannel{name: "cli"}
	b := bus.NewMessageBus()
	m := NewChannelManager([]Channel{c}, b, true, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.StartAll(ctx)
	time.Sleep(50 * time.Millisecond)
	if err := b.PublishOutbound(ctx, &bus.OutboundMessage{
		Channel: "cli", ChatID: "x", Content: "hint",
		Metadata: map[string]any{"_tool_hint": true},
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.sent) != 0 {
		t.Fatalf("hint not filtered: %v", c.sent)
	}
}

func TestManager_RegisterAndGet(t *testing.T) {
	m := NewChannelManager(nil, bus.NewMessageBus(), false, nil)
	c := &fakeChannel{name: "x"}
	m.Register(c)
	if got := m.GetChannel("x"); got != c {
		t.Fatal("Register/GetChannel")
	}
	if m.GetChannel("missing") != nil {
		t.Fatal("missing")
	}
}
