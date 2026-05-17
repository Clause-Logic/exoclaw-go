package bus

import (
	"context"
	"testing"
	"time"
)

// Ported from tests/micro/test_bus.py.

func TestInboundMessage_SessionKey(t *testing.T) {
	m := NewInboundMessage("telegram", "user1", "chat1", "hi")
	if m.SessionKey() != "telegram:chat1" {
		t.Fatalf("session key: %s", m.SessionKey())
	}
	m.SessionKeyOverride = "custom:key"
	if m.SessionKey() != "custom:key" {
		t.Fatalf("override key: %s", m.SessionKey())
	}
}

func TestInboundMessage_TimestampSet(t *testing.T) {
	m := NewInboundMessage("c", "s", "ch", "x")
	if m.Timestamp.IsZero() {
		t.Fatal("timestamp not set")
	}
}

func TestMessageBus_RoundTrip(t *testing.T) {
	b := NewMessageBus()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	in := NewInboundMessage("cli", "u", "c", "hello")
	if err := b.PublishInbound(ctx, in); err != nil {
		t.Fatal(err)
	}
	got, err := b.ConsumeInbound(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.Content != "hello" {
		t.Fatalf("content: %s", got.Content)
	}

	out := NewOutboundMessage("cli", "c", "world")
	if err := b.PublishOutbound(ctx, out); err != nil {
		t.Fatal(err)
	}
	got2, err := b.ConsumeOutbound(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got2.Content != "world" {
		t.Fatal("outbound content")
	}
}

func TestMessageBus_InboundHook(t *testing.T) {
	b := NewMessageBus()
	captured := make(chan *InboundMessage, 1)
	b.SetInboundHook(func(_ context.Context, msg *InboundMessage) error {
		captured <- msg
		return nil
	})
	if err := b.PublishInbound(context.Background(), NewInboundMessage("c", "s", "ch", "x")); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-captured:
		if m.Content != "x" {
			t.Fatal("content")
		}
	case <-time.After(time.Second):
		t.Fatal("hook not invoked")
	}
}

func TestMessageBus_ContextCancel(t *testing.T) {
	b := NewMessageBus()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.ConsumeInbound(ctx); err == nil {
		t.Fatal("expected cancel error")
	}
}
