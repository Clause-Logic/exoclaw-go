package conversationfile

import (
	"context"
	"log/slog"
	"strings"
)

// Ported from exoclaw_conversation/summarizing_policy.py.
//
// Default ConsolidationPolicy: rolling summary + sidecar-backed tail
// pointer. Implements the policy-as-transform contract from protocols.go.
// The policy owns its own state in a per-session JSON sidecar
// (consolidation_state.go). The session log itself stays append-only —
// this policy never mutates message data.

// ChunkSummarizer is an async chunk-summariser callable. Defaults to
// MemoryBackend.Summarize but can be overridden for tests / durable
// boundaries where the LLM call must run via a different transport.
type ChunkSummarizer func(ctx context.Context, chunk []map[string]any) (string, error)

// SummarizingConsolidationPolicy is the rolling-summary policy backed by a
// per-session JSON sidecar.
type SummarizingConsolidationPolicy struct {
	Memory       MemoryBackend
	StateDir     string
	MemoryWindow int

	summarize ChunkSummarizer
	log       *slog.Logger
}

// SummarizingPolicyOptions bundles construction options.
type SummarizingPolicyOptions struct {
	// MemoryWindow is the number of unconsolidated tail messages before
	// OnTurnComplete triggers a periodic consolidation pass. Also the
	// chunk size used by the recovery cascade. Default 50.
	MemoryWindow int
	// SummarizeChunk is an optional override for how a chunk gets
	// summarised. Defaults to Memory.Summarize.
	SummarizeChunk ChunkSummarizer
	// Log is the slog logger; defaults to slog.Default.
	Log *slog.Logger
}

// NewSummarizingConsolidationPolicy constructs a policy backed by memory
// and persisted in stateDir.
func NewSummarizingConsolidationPolicy(memory MemoryBackend, stateDir string, opts SummarizingPolicyOptions) *SummarizingConsolidationPolicy {
	memWindow := opts.MemoryWindow
	if memWindow <= 0 {
		memWindow = 50
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	p := &SummarizingConsolidationPolicy{
		Memory:       memory,
		StateDir:     stateDir,
		MemoryWindow: memWindow,
		log:          log,
	}
	if opts.SummarizeChunk != nil {
		p.summarize = opts.SummarizeChunk
	} else {
		p.summarize = p.defaultSummarize
	}
	return p
}

func (p *SummarizingConsolidationPolicy) defaultSummarize(ctx context.Context, chunk []map[string]any) (string, error) {
	return p.Memory.Summarize(ctx, chunk)
}

// ─── ConsolidationPolicy interface surface ───

// Transform materialises the LLM-input view from the append-only log.
// Emits [summary preamble?, tail...] where the tail is the segment of the
// log past state.SummarizedThrough.
//
// budget is accepted for the interface but ignored — preemptive routing
// was dropped in favour of reactive-only overflow recovery via
// RecoverFromOverflow.
func (p *SummarizingConsolidationPolicy) Transform(ctx context.Context, reader SessionReader, budget int) <-chan StreamMessage {
	_ = budget
	out := make(chan StreamMessage)
	go func() {
		defer close(out)
		state := LoadState(p.StateDir, reader.Key(), p.log)
		if state.Summary != "" {
			select {
			case out <- StreamMessage{Message: map[string]any{
				"role":    "system",
				"content": formatSummaryPreamble(state.Summary),
			}}:
			case <-ctx.Done():
				return
			}
		}
		for item := range reader.Stream(ctx, state.SummarizedThrough, -1) {
			select {
			case out <- item:
				if item.Err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// OnTurnComplete is the periodic-consolidation hook. Runs at most one
// summarise pass per call; long sessions catch up over multiple turns
// rather than freezing one turn for a multi-chunk batch.
func (p *SummarizingConsolidationPolicy) OnTurnComplete(ctx context.Context, reader SessionReader) error {
	state := LoadState(p.StateDir, reader.Key(), p.log)
	total, err := reader.Count(ctx)
	if err != nil {
		return err
	}
	unconsolidated := total - state.SummarizedThrough
	if unconsolidated < p.MemoryWindow {
		return nil
	}
	advanced, err := p.summarizeOneChunk(ctx, reader, state, total)
	if err != nil {
		return err
	}
	if advanced {
		return SaveState(p.StateDir, reader.Key(), state)
	}
	return nil
}

// RecoverFromOverflow is the reactive overflow-recovery seam. Summarises
// one chunk and advances the sidecar pointer.
//
// Called by DefaultConversation.RecoverFromOverflow when the agent loop
// catches ContextWindowExceededError. Returns true if the pointer
// advanced (caller re-materialises the view and retries the LLM call);
// false if there's nothing left to summarise.
//
// Only summarises one chunk per call — the agent loop's
// MaxRecoveryAttempts cap drives multiple invocations if a single chunk
// isn't enough.
func (p *SummarizingConsolidationPolicy) RecoverFromOverflow(ctx context.Context, reader SessionReader) (bool, error) {
	state := LoadState(p.StateDir, reader.Key(), p.log)
	total, err := reader.Count(ctx)
	if err != nil {
		return false, err
	}
	advanced, err := p.summarizeOneChunk(ctx, reader, state, total)
	if err != nil {
		return false, err
	}
	if advanced {
		if err := SaveState(p.StateDir, reader.Key(), state); err != nil {
			return false, err
		}
	}
	return advanced, nil
}

// ─── Internal: chunk summarisation + boundary repair ───

func (p *SummarizingConsolidationPolicy) summarizeOneChunk(ctx context.Context, reader SessionReader, state *ConsolidationState, total int) (bool, error) {
	chunkEndTarget := state.SummarizedThrough + p.MemoryWindow
	if chunkEndTarget > total {
		chunkEndTarget = total
	}
	if chunkEndTarget <= state.SummarizedThrough {
		return false, nil
	}

	var chunk []map[string]any
	for item := range reader.Stream(ctx, state.SummarizedThrough, chunkEndTarget) {
		if item.Err != nil {
			return false, item.Err
		}
		chunk = append(chunk, item.Message)
	}
	if len(chunk) == 0 {
		return false, nil
	}

	historyEntry, err := p.summarize(ctx, chunk)
	if err != nil {
		p.log.Warn("consolidation_chunk_summarize_failed",
			"session.key", reader.Key(),
			"chunk.size", len(chunk),
			"err", err,
		)
		return false, nil
	}
	if historyEntry == "" {
		p.log.Warn("consolidation_chunk_summarize_failed",
			"session.key", reader.Key(),
			"chunk.size", len(chunk),
		)
		return false, nil
	}

	// Advance past tool_use/tool_result pairs.
	newBoundary, err := p.repairBoundary(ctx, reader, chunkEndTarget, total)
	if err != nil {
		return false, err
	}
	state.SummarizedThrough = newBoundary
	state.Summary = mergeSummary(state.Summary, historyEntry)
	p.log.Info("consolidation_chunk_summarized",
		"session.key", reader.Key(),
		"chunk.size", len(chunk),
		"summarized_through", state.SummarizedThrough,
	)
	return true, nil
}

func (p *SummarizingConsolidationPolicy) repairBoundary(ctx context.Context, reader SessionReader, boundary, total int) (int, error) {
	if boundary >= total {
		return boundary, nil
	}
	var prev map[string]any
	if boundary > 0 {
		var err error
		prev, err = reader.At(ctx, boundary-1)
		if err != nil {
			return boundary, err
		}
	}
	curIdx := boundary
	for item := range reader.Stream(ctx, boundary, total) {
		if item.Err != nil {
			return curIdx, item.Err
		}
		curr := item.Message
		role, _ := curr["role"].(string)
		if role == "tool" {
			curIdx++
			prev = curr
			continue
		}
		if prev != nil {
			prevRole, _ := prev["role"].(string)
			if tcs, _ := prev["tool_calls"].([]any); prevRole == "assistant" && len(tcs) > 0 {
				curIdx++
				prev = curr
				continue
			}
		}
		break
	}
	return curIdx, nil
}

func formatSummaryPreamble(summary string) string {
	return "## Previous Session Summary\n" + summary
}

func mergeSummary(existing, newEntry string) string {
	newEntry = strings.TrimSpace(newEntry)
	existing = strings.TrimSpace(existing)
	if existing == "" {
		return newEntry
	}
	return existing + "\n\n" + newEntry
}

// Compile-time check that SummarizingConsolidationPolicy satisfies the
// ConsolidationPolicy interface.
var _ ConsolidationPolicy = (*SummarizingConsolidationPolicy)(nil)
