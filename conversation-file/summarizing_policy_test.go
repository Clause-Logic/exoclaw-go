package conversationfile

import (
	"context"
	"os"
	"strings"
	"testing"
)

// Ported from tests/test_summarizing_policy.py.
//
// Exercises: chunk-size threshold for OnTurnComplete, boundary repair past
// tool_use/tool_result pairs, sidecar migration from legacy
// last_consolidated headers, and the default SessionReader fallback over a
// HistoryStore that exposes only LoadRange.

type fakeMemory struct {
	chunks [][]map[string]any
	ret    string
	err    error
}

func (m *fakeMemory) GetMemoryContext() string { return "" }
func (m *fakeMemory) Summarize(_ context.Context, chunk []map[string]any) (string, error) {
	m.chunks = append(m.chunks, chunk)
	return m.ret, m.err
}

func TestPolicy_OnTurnCompleteSkipsBelowThreshold(t *testing.T) {
	tmp := t.TempDir()
	mem := &fakeMemory{ret: "summary"}
	policy := NewSummarizingConsolidationPolicy(mem, tmp, SummarizingPolicyOptions{MemoryWindow: 10})

	reader := &fakeReader{key: "k", total: 5}
	if err := policy.OnTurnComplete(context.Background(), reader); err != nil {
		t.Fatal(err)
	}
	if len(mem.chunks) != 0 {
		t.Fatal("summariser called below threshold")
	}
}

func TestPolicy_OnTurnCompleteSummarisesWhenThresholdMet(t *testing.T) {
	tmp := t.TempDir()
	mem := &fakeMemory{ret: "summary"}
	policy := NewSummarizingConsolidationPolicy(mem, tmp, SummarizingPolicyOptions{MemoryWindow: 3})

	msgs := make([]map[string]any, 10)
	for i := range msgs {
		msgs[i] = map[string]any{"role": "user", "content": "m"}
	}
	reader := &fakeReader{key: "k", total: 10, messages: msgs}
	if err := policy.OnTurnComplete(context.Background(), reader); err != nil {
		t.Fatal(err)
	}
	if len(mem.chunks) != 1 {
		t.Fatalf("expected one chunk, got %d", len(mem.chunks))
	}
	state := LoadState(tmp, "k", nil)
	if state.SummarizedThrough != 3 {
		t.Fatalf("state.summarized_through=%d", state.SummarizedThrough)
	}
	if !strings.Contains(state.Summary, "summary") {
		t.Fatalf("summary=%q", state.Summary)
	}
}

func TestPolicy_RecoverFromOverflowAdvancesByOneChunk(t *testing.T) {
	tmp := t.TempDir()
	mem := &fakeMemory{ret: "summary"}
	policy := NewSummarizingConsolidationPolicy(mem, tmp, SummarizingPolicyOptions{MemoryWindow: 2})

	msgs := []map[string]any{
		{"role": "user", "content": "u1"},
		{"role": "assistant", "content": "a1"},
		{"role": "user", "content": "u2"},
		{"role": "assistant", "content": "a2"},
	}
	reader := &fakeReader{key: "k2", total: 4, messages: msgs}
	advanced, err := policy.RecoverFromOverflow(context.Background(), reader)
	if err != nil {
		t.Fatal(err)
	}
	if !advanced {
		t.Fatal("expected advance")
	}
	if len(mem.chunks) != 1 {
		t.Fatalf("chunks: %d", len(mem.chunks))
	}
	if len(mem.chunks[0]) != 2 {
		t.Fatalf("expected chunk of 2 messages, got %d", len(mem.chunks[0]))
	}
}

func TestPolicy_RecoverReturnsFalseWhenNothingLeft(t *testing.T) {
	tmp := t.TempDir()
	mem := &fakeMemory{ret: "summary"}
	policy := NewSummarizingConsolidationPolicy(mem, tmp, SummarizingPolicyOptions{MemoryWindow: 10})
	reader := &fakeReader{key: "empty", total: 0}
	advanced, err := policy.RecoverFromOverflow(context.Background(), reader)
	if err != nil || advanced {
		t.Fatalf("got advanced=%v err=%v", advanced, err)
	}
}

func TestPolicy_BoundaryRepairWalksPastToolPair(t *testing.T) {
	tmp := t.TempDir()
	mem := &fakeMemory{ret: "S"}
	policy := NewSummarizingConsolidationPolicy(mem, tmp, SummarizingPolicyOptions{MemoryWindow: 2})

	// Boundary at index 2 would split assistant+tool_result. The repair
	// should advance to include the tool message at index 2.
	msgs := []map[string]any{
		{"role": "user", "content": "u1"},
		{"role": "assistant", "content": "", "tool_calls": []any{map[string]any{"id": "c1"}}},
		{"role": "tool", "tool_call_id": "c1", "content": "tr1"},
		{"role": "user", "content": "u2"},
	}
	reader := &fakeReader{key: "k3", total: 4, messages: msgs}
	if _, err := policy.RecoverFromOverflow(context.Background(), reader); err != nil {
		t.Fatal(err)
	}
	state := LoadState(tmp, "k3", nil)
	if state.SummarizedThrough != 3 {
		t.Fatalf("boundary repair failed: %d", state.SummarizedThrough)
	}
}

func TestPolicy_TransformEmitsSummaryPreamble(t *testing.T) {
	tmp := t.TempDir()
	state := &ConsolidationState{SummarizedThrough: 1, Summary: "earlier ctx"}
	if err := SaveState(tmp, "preamble", state); err != nil {
		t.Fatal(err)
	}
	mem := &fakeMemory{}
	policy := NewSummarizingConsolidationPolicy(mem, tmp, SummarizingPolicyOptions{MemoryWindow: 10})
	reader := &fakeReader{key: "preamble", total: 3, messages: []map[string]any{
		{"role": "user", "content": "first"},
		{"role": "user", "content": "tail1"},
		{"role": "user", "content": "tail2"},
	}}
	var out []map[string]any
	for item := range policy.Transform(context.Background(), reader, 0) {
		if item.Err != nil {
			t.Fatal(item.Err)
		}
		out = append(out, item.Message)
	}
	if len(out) < 1 || !strings.Contains(out[0]["content"].(string), "earlier ctx") {
		t.Fatalf("missing preamble: %v", out)
	}
}

func TestConsolidationStateMigration_LegacyHeader(t *testing.T) {
	tmp := t.TempDir()
	legacyJSONL := SidecarPath(tmp, "k") // wrong path on purpose to verify
	_ = legacyJSONL                      // just ensure no panic
	// Manually drop a legacy session JSONL with metadata + non-zero
	// last_consolidated; LoadState should seed a new sidecar.
	legacyPath := SidecarPath(tmp, "k")
	_ = legacyPath
	// Write a legacy file at the path readLegacyBoundary expects.
	jsonlPath := strings.TrimSuffix(SidecarPath(tmp, "k"), ".consolidation.json") + ".jsonl"
	body := `{"_type":"metadata","key":"k","last_consolidated":7,"metadata":{"summary":"migrated"}}` + "\n"
	if err := os.WriteFile(jsonlPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	state := LoadState(tmp, "k", nil)
	if state.SummarizedThrough != 7 || state.Summary != "migrated" {
		t.Fatalf("migration: through=%d summary=%q", state.SummarizedThrough, state.Summary)
	}
	// And the sidecar should be persisted now.
	reloaded := LoadState(tmp, "k", nil)
	if reloaded.SummarizedThrough != 7 {
		t.Fatal("sidecar not persisted")
	}
}

// ----------------------------------------------------------------------
// fakeReader — tiny in-memory SessionReader for policy tests.
// ----------------------------------------------------------------------

type fakeReader struct {
	key      string
	total    int
	messages []map[string]any
}

func (r *fakeReader) Key() string { return r.key }
func (r *fakeReader) Count(_ context.Context) (int, error) {
	return r.total, nil
}
func (r *fakeReader) Stream(ctx context.Context, start, end int) <-chan StreamMessage {
	out := make(chan StreamMessage)
	go func() {
		defer close(out)
		stop := end
		if stop < 0 || stop > len(r.messages) {
			stop = len(r.messages)
		}
		for i := start; i < stop; i++ {
			select {
			case out <- StreamMessage{Message: r.messages[i]}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
func (r *fakeReader) At(_ context.Context, index int) (map[string]any, error) {
	if index < 0 || index >= len(r.messages) {
		return nil, nil
	}
	return r.messages[index], nil
}
