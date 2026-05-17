// Package channels contains the Channel protocol and ChannelManager.
//
// Ported from exoclaw/channels/protocol.py and exoclaw/channels/manager.py.
package channels

import (
	"context"

	"github.com/standd/exoclaw-go/exoclaw/bus"
)

// Channel is the protocol any chat channel implementation must satisfy.
type Channel interface {
	// Name returns the channel's identifier (matched against
	// OutboundMessage.Channel for routing).
	Name() string

	// Start connects to the platform and begins receiving messages.
	Start(ctx context.Context, b bus.Bus) error

	// Stop disconnects and releases resources.
	Stop(ctx context.Context) error

	// Send delivers an outbound message to the platform.
	Send(ctx context.Context, msg *bus.OutboundMessage) error
}
