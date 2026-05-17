// Package bus contains the message bus, its events, and protocols.
//
// Ported from exoclaw/bus/events.py, exoclaw/bus/protocol.py, exoclaw/bus/queue.py.
package bus

import (
	"fmt"
	"time"
)

// InboundMessage is a message received from a chat channel.
type InboundMessage struct {
	Channel            string // telegram, discord, slack, whatsapp, ...
	SenderID           string
	ChatID             string
	Content            string
	Timestamp          time.Time
	Media              []string
	Metadata           map[string]any
	SessionKeyOverride string
	ModelOverride      string
}

// NewInboundMessage builds an InboundMessage with the time.Now timestamp and
// non-nil zero slices/maps — matching the Python dataclass default_factory shape.
func NewInboundMessage(channel, senderID, chatID, content string) *InboundMessage {
	return &InboundMessage{
		Channel:   channel,
		SenderID:  senderID,
		ChatID:    chatID,
		Content:   content,
		Timestamp: time.Now(),
		Media:     []string{},
		Metadata:  map[string]any{},
	}
}

// SessionKey returns the unique session id ("channel:chat_id" by default).
func (m *InboundMessage) SessionKey() string {
	if m.SessionKeyOverride != "" {
		return m.SessionKeyOverride
	}
	return fmt.Sprintf("%s:%s", m.Channel, m.ChatID)
}

// OutboundMessage is a message to send to a chat channel.
type OutboundMessage struct {
	Channel  string
	ChatID   string
	Content  string
	ReplyTo  string
	Media    []string
	Metadata map[string]any
	// Buttons holds inline button rows for interactive callbacks (ask_user,
	// message tool). Each inner slice is one row of button labels.
	Buttons [][]string
}

// NewOutboundMessage constructs an OutboundMessage with non-nil zero slices/maps.
func NewOutboundMessage(channel, chatID, content string) *OutboundMessage {
	return &OutboundMessage{
		Channel:  channel,
		ChatID:   chatID,
		Content:  content,
		Media:    []string{},
		Metadata: map[string]any{},
		Buttons:  [][]string{},
	}
}
