// Package stdin is a minimal Channel implementation: reads inbound from
// stdin (one message per line) and writes outbound to stdout. Intended
// for local development and quick smoke tests — no markdown rendering,
// no command history, no extra deps.
package stdin

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/Clause-Logic/exoclaw-go/exoclaw/bus"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/channels"
)

// ChannelName is the routing key. OutboundMessage.Channel must match.
const ChannelName = "cli"

// ChatID is what an inbound message is tagged with — the session is
// `cli:direct` end-to-end so the conversation layer keys correctly.
const ChatID = "direct"

// Channel reads stdin into bus.PublishInbound, and Send writes outbound
// payloads to stdout. Progress / tool-hint markers are skipped so a user
// staring at the terminal isn't drowned in intermediate updates — only
// the final assistant reply is printed.
type Channel struct {
	In  io.Reader // defaults to os.Stdin in New
	Out io.Writer // defaults to os.Stdout in New
	// Prompt is printed before each input read. Default "> ".
	Prompt string
	// SenderID tags each inbound message; defaults to "user".
	SenderID string

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// New returns a Channel wired to os.Stdin / os.Stdout.
func New() *Channel {
	return &Channel{In: os.Stdin, Out: os.Stdout, Prompt: "> ", SenderID: "user"}
}

// Name implements channels.Channel.
func (c *Channel) Name() string { return ChannelName }

// Start launches a goroutine that reads lines from In and publishes them
// to the bus. Returns immediately. The reader goroutine runs until ctx
// (or the context passed to Start) is cancelled, or until stdin EOFs.
func (c *Channel) Start(ctx context.Context, b bus.Bus) error {
	c.mu.Lock()
	if c.cancel != nil {
		c.mu.Unlock()
		return errors.New("stdin channel already started")
	}
	readCtx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.done = make(chan struct{})
	c.mu.Unlock()

	go c.readLoop(readCtx, b)
	return nil
}

// Stop cancels the reader goroutine and waits for it to exit.
func (c *Channel) Stop(_ context.Context) error {
	c.mu.Lock()
	cancel := c.cancel
	done := c.done
	c.cancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	return nil
}

// Send implements channels.Channel. Writes the message content to Out,
// suppressing progress markers and tool hints so the user sees only the
// final assistant reply.
func (c *Channel) Send(_ context.Context, msg *bus.OutboundMessage) error {
	if isProgress(msg) {
		return nil
	}
	if strings.TrimSpace(msg.Content) == "" {
		return nil
	}
	_, err := fmt.Fprintln(c.Out, msg.Content)
	if err != nil {
		return err
	}
	// Re-print the prompt so the user knows we're ready for more input.
	if c.Prompt != "" {
		_, _ = fmt.Fprint(c.Out, c.Prompt)
	}
	return nil
}

func isProgress(msg *bus.OutboundMessage) bool {
	if msg.Metadata == nil {
		return false
	}
	if v, ok := msg.Metadata["_progress"].(bool); ok && v {
		return true
	}
	if v, ok := msg.Metadata["_tool_hint"].(bool); ok && v {
		return true
	}
	return false
}

func (c *Channel) readLoop(ctx context.Context, b bus.Bus) {
	defer close(c.done)
	scanner := bufio.NewScanner(c.In)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	if c.Prompt != "" {
		_, _ = fmt.Fprint(c.Out, c.Prompt)
	}
	for scanner.Scan() {
		if ctx.Err() != nil {
			return
		}
		line := strings.TrimRight(scanner.Text(), "\r\n")
		if line == "" {
			if c.Prompt != "" {
				_, _ = fmt.Fprint(c.Out, c.Prompt)
			}
			continue
		}
		msg := bus.NewInboundMessage(ChannelName, c.SenderID, ChatID, line)
		if err := b.PublishInbound(ctx, msg); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			fmt.Fprintf(c.Out, "[publish error: %v]\n", err)
		}
	}
	// EOF on stdin (or scanner error) — exit gracefully.
}

// Compile-time check that *Channel satisfies channels.Channel.
var _ channels.Channel = (*Channel)(nil)
