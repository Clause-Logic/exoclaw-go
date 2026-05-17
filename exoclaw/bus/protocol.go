package bus

import "context"

// Bus is the only bus surface external code should depend on.
type Bus interface {
	PublishInbound(ctx context.Context, msg *InboundMessage) error
	ConsumeInbound(ctx context.Context) (*InboundMessage, error)
	PublishOutbound(ctx context.Context, msg *OutboundMessage) error
	ConsumeOutbound(ctx context.Context) (*OutboundMessage, error)
}

// InboundHookHandler is the signature of a durable inbound handler.
type InboundHookHandler func(ctx context.Context, msg *InboundMessage) error

// InboundHookBus is an optional capability layered on Bus: durable executors
// can install a synchronous handler for every PublishInbound. Buses that
// don't need the capability omit this interface.
type InboundHookBus interface {
	Bus
	// SetInboundHook installs (or, with nil, clears) the durable handler.
	// When set, PublishInbound forwards to the handler instead of queueing.
	SetInboundHook(handler InboundHookHandler)
}
