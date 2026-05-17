package channels

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/standd/exoclaw-go/exoclaw/bus"
)

// ChannelManager coordinates a set of Channel instances.
//
// Accepts any slice of Channels — no knowledge of specific platforms or their
// configuration. Platform wiring lives in the caller (e.g. a ChannelFactory).
type ChannelManager struct {
	Bus              bus.Bus
	channels         map[string]Channel
	chMu             sync.RWMutex
	log              *slog.Logger
	filterToolHints  bool

	dispatchCancel context.CancelFunc
	dispatchDone   chan struct{}
}

// NewChannelManager builds a ChannelManager.
func NewChannelManager(channels []Channel, b bus.Bus, filterToolHints bool, log *slog.Logger) *ChannelManager {
	if log == nil {
		log = slog.Default()
	}
	m := &ChannelManager{
		Bus:             b,
		channels:        make(map[string]Channel, len(channels)),
		log:             log,
		filterToolHints: filterToolHints,
	}
	for _, ch := range channels {
		m.channels[ch.Name()] = ch
	}
	return m
}

// Register registers a channel after construction.
func (m *ChannelManager) Register(ch Channel) {
	m.chMu.Lock()
	defer m.chMu.Unlock()
	m.channels[ch.Name()] = ch
}

// GetChannel returns the channel registered for name, or nil.
func (m *ChannelManager) GetChannel(name string) Channel {
	m.chMu.RLock()
	defer m.chMu.RUnlock()
	return m.channels[name]
}

func (m *ChannelManager) startChannel(ctx context.Context, name string, ch Channel) {
	if err := ch.Start(ctx, m.Bus); err != nil {
		m.log.Error("channel_start_error", "channel", name, "err", err)
	}
}

// StartAll starts the outbound dispatcher and every registered channel.
// Returns after all channel Start() calls return.
func (m *ChannelManager) StartAll(ctx context.Context) error {
	m.chMu.RLock()
	count := len(m.channels)
	m.chMu.RUnlock()

	if count == 0 {
		m.log.Warn("no_channels")
		return nil
	}

	dispatchCtx, cancel := context.WithCancel(ctx)
	m.dispatchCancel = cancel
	m.dispatchDone = make(chan struct{})
	go func() {
		defer close(m.dispatchDone)
		m.dispatchOutbound(dispatchCtx)
	}()

	m.chMu.RLock()
	names := make([]string, 0, len(m.channels))
	chans := make(map[string]Channel, len(m.channels))
	for n, c := range m.channels {
		names = append(names, n)
		chans[n] = c
	}
	m.chMu.RUnlock()

	m.log.Info("channels_start", "channels", names)

	var wg sync.WaitGroup
	for name, ch := range chans {
		wg.Add(1)
		go func(name string, ch Channel) {
			defer wg.Done()
			m.startChannel(ctx, name, ch)
		}(name, ch)
	}
	wg.Wait()
	return nil
}

// StopAll cancels the outbound dispatcher and stops every registered channel.
func (m *ChannelManager) StopAll(ctx context.Context) error {
	m.log.Info("channels_stop")

	if m.dispatchCancel != nil {
		m.dispatchCancel()
	}
	if m.dispatchDone != nil {
		<-m.dispatchDone
	}

	m.chMu.RLock()
	chans := make(map[string]Channel, len(m.channels))
	for n, c := range m.channels {
		chans[n] = c
	}
	m.chMu.RUnlock()

	for name, ch := range chans {
		if err := ch.Stop(ctx); err != nil {
			m.log.Error("channel_stop_error", "channel", name, "err", err)
		}
	}
	return nil
}

func (m *ChannelManager) dispatchOutbound(ctx context.Context) {
	for {
		// 1-second poll cycle to mirror the Python asyncio.wait_for(timeout=1.0).
		consumeCtx, cancel := context.WithTimeout(ctx, time.Second)
		msg, err := m.Bus.ConsumeOutbound(consumeCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			if errors.Is(err, context.Canceled) {
				return
			}
			continue
		}

		if m.filterToolHints && msg.Metadata != nil {
			if hint, ok := msg.Metadata["_tool_hint"]; ok {
				if b, _ := hint.(bool); b {
					continue
				}
			}
		}

		ch := m.GetChannel(msg.Channel)
		if ch == nil {
			m.log.Warn("unknown_channel", "channel", msg.Channel)
			continue
		}
		if err := ch.Send(ctx, msg); err != nil {
			m.log.Error("outbound_send_error", "channel", msg.Channel, "err", err)
		}
	}
}
