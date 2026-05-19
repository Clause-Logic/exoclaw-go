package stdin

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Clause-Logic/exoclaw-go/exoclaw/bus"
)

func TestChannel_ReadsInboundFromStdin(t *testing.T) {
	in := strings.NewReader("hello\nworld\n")
	var out bytes.Buffer
	c := &Channel{In: in, Out: &out, Prompt: "", SenderID: "u"}

	b := bus.NewMessageBus()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Start(ctx, b); err != nil {
		t.Fatal(err)
	}

	first, err := b.ConsumeInbound(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Content != "hello" || first.Channel != ChannelName || first.ChatID != ChatID {
		t.Fatalf("got %+v", first)
	}
	second, err := b.ConsumeInbound(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if second.Content != "world" {
		t.Fatalf("got %+v", second)
	}
	_ = c.Stop(context.Background())
}

func TestChannel_SendPrintsToStdout(t *testing.T) {
	var out bytes.Buffer
	c := &Channel{In: strings.NewReader(""), Out: &out, Prompt: "> "}
	err := c.Send(context.Background(), &bus.OutboundMessage{
		Channel: ChannelName, ChatID: ChatID, Content: "reply",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if !strings.Contains(got, "reply\n") {
		t.Fatalf("missing reply: %q", got)
	}
	if !strings.HasSuffix(got, "> ") {
		t.Fatalf("missing trailing prompt: %q", got)
	}
}

func TestChannel_SendSkipsProgressMessages(t *testing.T) {
	var out bytes.Buffer
	c := &Channel{In: strings.NewReader(""), Out: &out, Prompt: ""}
	_ = c.Send(context.Background(), &bus.OutboundMessage{
		Channel: ChannelName, Content: "tool-hint",
		Metadata: map[string]any{"_tool_hint": true},
	})
	_ = c.Send(context.Background(), &bus.OutboundMessage{
		Channel: ChannelName, Content: "progress",
		Metadata: map[string]any{"_progress": true},
	})
	if out.Len() != 0 {
		t.Fatalf("progress leaked: %q", out.String())
	}
}

func TestChannel_StopIsIdempotent(t *testing.T) {
	c := &Channel{In: strings.NewReader(""), Out: &bytes.Buffer{}, Prompt: ""}
	b := bus.NewMessageBus()
	if err := c.Start(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestChannel_DoubleStartRejected(t *testing.T) {
	c := &Channel{In: strings.NewReader(""), Out: &bytes.Buffer{}, Prompt: ""}
	b := bus.NewMessageBus()
	if err := c.Start(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	defer c.Stop(context.Background())
	if err := c.Start(context.Background(), b); err == nil {
		t.Fatal("expected error on double start")
	}
}
