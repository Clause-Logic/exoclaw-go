// Package app contains the Exoclaw composition root.
//
// Ported from exoclaw/app.py.
package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/Clause-Logic/exoclaw-go/exoclaw/agent"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/agent/tools"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/bus"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/channels"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/conversation"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/executor"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/iterationpolicy"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/providers"
)

// Exoclaw wires together all exoclaw components and runs the event loop.
type Exoclaw struct {
	Provider         providers.LLMProvider
	Conversation     conversation.Conversation
	Channels         []channels.Channel
	Tools            []tools.Tool
	Bus              bus.Bus
	Model            string
	Temperature      float64
	MaxTokens        int
	MaxIterations    int
	ReasoningEffort  string
	IterationPolicy  iterationpolicy.IterationPolicy
	Executor         executor.Executor
	Log              *slog.Logger
}

// Options bundles Exoclaw construction parameters. Required: Provider,
// Conversation. Everything else has a default.
type Options struct {
	Provider         providers.LLMProvider
	Conversation     conversation.Conversation
	Channels         []channels.Channel
	Tools            []tools.Tool
	Bus              bus.Bus
	Model            string
	Temperature      float64
	MaxTokens        int
	MaxIterations    int
	ReasoningEffort  string
	IterationPolicy  iterationpolicy.IterationPolicy
	Executor         executor.Executor
	Log              *slog.Logger
}

// New constructs an Exoclaw composition root.
func New(opts Options) *Exoclaw {
	temp := opts.Temperature
	if temp == 0 {
		temp = 0.1
	}
	tokens := opts.MaxTokens
	if tokens == 0 {
		tokens = 8192
	}
	iters := opts.MaxIterations
	if iters == 0 {
		iters = 40
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	return &Exoclaw{
		Provider:        opts.Provider,
		Conversation:    opts.Conversation,
		Channels:        opts.Channels,
		Tools:           opts.Tools,
		Bus:             opts.Bus,
		Model:           opts.Model,
		Temperature:     temp,
		MaxTokens:       tokens,
		MaxIterations:   iters,
		ReasoningEffort: opts.ReasoningEffort,
		IterationPolicy: opts.IterationPolicy,
		Executor:        opts.Executor,
		Log:             log,
	}
}

// build instantiates the internal components. Called once at run time.
func (x *Exoclaw) build() (bus.Bus, *agent.AgentLoop, *channels.ChannelManager) {
	b := x.Bus
	if b == nil {
		b = bus.NewMessageBus()
	}
	model := x.Model
	if model == "" {
		model = x.Provider.GetDefaultModel()
	}
	loop := agent.New(agent.Options{
		Bus:             b,
		Provider:        x.Provider,
		Conversation:    x.Conversation,
		Model:           model,
		MaxIterations:   x.MaxIterations,
		Temperature:     x.Temperature,
		MaxTokens:       x.MaxTokens,
		ReasoningEffort: x.ReasoningEffort,
		Tools:           x.Tools,
		IterationPolicy: x.IterationPolicy,
		Executor:        x.Executor,
		Log:             x.Log,
	})
	cm := channels.NewChannelManager(x.Channels, b, false, x.Log)
	return b, loop, cm
}

// Run starts all components and runs until ctx is cancelled.
func (x *Exoclaw) Run(ctx context.Context) error {
	_, loop, cm := x.build()
	x.Log.Info("exoclaw_starting")

	defer func() {
		loop.Stop()
		stopCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_ = cm.StopAll(stopCtx)
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	loopErrCh := make(chan error, 1)
	chErrCh := make(chan error, 1)
	go func() {
		defer wg.Done()
		loopErrCh <- loop.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		chErrCh <- cm.StartAll(ctx)
	}()

	wg.Wait()
	loopErr := <-loopErrCh
	chErr := <-chErrCh

	x.Log.Info("exoclaw_stopping")
	if loopErr != nil && !errors.Is(loopErr, context.Canceled) {
		return loopErr
	}
	if chErr != nil && !errors.Is(chErr, context.Canceled) {
		return chErr
	}
	return nil
}
