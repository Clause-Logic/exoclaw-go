package bus

import (
	"context"
	"sync"
)

// MessageBus is the default channel-based implementation of Bus.
//
// Channels push messages to the inbound queue, and the agent loop processes
// them and pushes responses to the outbound queue.
//
// When a durable executor is in play, AgentLoop installs an inbound hook via
// SetInboundHook. After that, channels still call PublishInbound the same
// way, but the message is handed off to the executor's durable store instead
// of the in-memory queue.
type MessageBus struct {
	inbound  chan *InboundMessage
	outbound chan *OutboundMessage

	hookMu sync.RWMutex
	hook   InboundHookHandler
}

// NewMessageBus constructs a MessageBus with unbounded* in-memory queues.
//
// *Go channels are bounded; the Python original used unbounded asyncio.Queue.
// We give the inbound and outbound channels a generous default buffer (1024)
// — large enough for typical chat loads, small enough to fail fast if a
// consumer stalls. Adjust to taste at the call site by wrapping NewMessageBus.
func NewMessageBus() *MessageBus {
	return &MessageBus{
		inbound:  make(chan *InboundMessage, 1024),
		outbound: make(chan *OutboundMessage, 1024),
	}
}

// SetInboundHook installs (or clears, with nil) a durable-inbound handler.
//
// When set, PublishInbound forwards to this handler instead of putting the
// message on the inbound channel. Intended for durable executors: the handler
// starts a workflow so the message is persisted before PublishInbound returns.
func (b *MessageBus) SetInboundHook(handler InboundHookHandler) {
	b.hookMu.Lock()
	defer b.hookMu.Unlock()
	b.hook = handler
}

// PublishInbound publishes a message from a channel to the agent.
func (b *MessageBus) PublishInbound(ctx context.Context, msg *InboundMessage) error {
	b.hookMu.RLock()
	hook := b.hook
	b.hookMu.RUnlock()
	if hook != nil {
		return hook(ctx, msg)
	}
	select {
	case b.inbound <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ConsumeInbound consumes the next inbound message (blocks until available).
func (b *MessageBus) ConsumeInbound(ctx context.Context) (*InboundMessage, error) {
	select {
	case msg := <-b.inbound:
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// PublishOutbound publishes a response from the agent to channels.
func (b *MessageBus) PublishOutbound(ctx context.Context, msg *OutboundMessage) error {
	select {
	case b.outbound <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ConsumeOutbound consumes the next outbound message (blocks until available).
func (b *MessageBus) ConsumeOutbound(ctx context.Context) (*OutboundMessage, error) {
	select {
	case msg := <-b.outbound:
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
