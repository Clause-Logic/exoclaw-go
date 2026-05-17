package conversationfile

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// Ported from tests/test_compaction.py.

func TestEstimateTokensBasic(t *testing.T) {
	msgs := []map[string]any{
		{"role": "user", "content": "hello"},
		{"role": "assistant", "content": "world"},
	}
	if got := estimateTokens(msgs); got != (len("hello")+len("world"))/charsPerToken {
		t.Fatalf("got %d", got)
	}
}

func TestEstimateTokensCountsToolCalls(t *testing.T) {
	msgs := []map[string]any{
		{"role": "assistant", "content": "", "tool_calls": []any{
			map[string]any{"function": map[string]any{"name": "fn", "arguments": `{"q":"hello world"}`}},
		}},
	}
	got := estimateTokens(msgs)
	if got == 0 {
		t.Fatal("tool call args not counted")
	}
}

func TestCompactUnderBudgetIsNoop(t *testing.T) {
	msgs := []map[string]any{{"role": "tool", "content": strings.Repeat("a", 200)}}
	out := CompactToolResults(msgs, 1_000_000, 0.75)
	if out[0]["content"].(string) != strings.Repeat("a", 200) {
		t.Fatal("modified under budget")
	}
}

func TestCompactReplacesOldToolResults(t *testing.T) {
	// 5 tool results each 1000 chars — should compact the first one(s)
	// since the last 4 are protected.
	msgs := []map[string]any{
		{"role": "system", "content": "sys"},
	}
	for i := 0; i < 5; i++ {
		msgs = append(msgs, map[string]any{"role": "tool", "content": strings.Repeat("x", 1000)})
	}
	out := CompactToolResults(msgs, 100, 0.5)
	// The first tool message (oldest, not protected) should be compacted.
	if out[1]["content"].(string) != compactionMarker {
		t.Fatalf("expected oldest compacted, got %q", out[1]["content"])
	}
	// The last 4 should be untouched.
	for i := 2; i < 6; i++ {
		if out[i]["content"].(string) == compactionMarker {
			t.Fatalf("recent message %d was compacted", i)
		}
	}
}

func TestCompactSkipsLast4Messages(t *testing.T) {
	msgs := []map[string]any{
		{"role": "tool", "content": strings.Repeat("a", 1000)},
		{"role": "tool", "content": strings.Repeat("b", 1000)},
		{"role": "tool", "content": strings.Repeat("c", 1000)},
		{"role": "tool", "content": strings.Repeat("d", 1000)},
	}
	out := CompactToolResults(msgs, 100, 0.5)
	for i, m := range out {
		if m["content"].(string) == compactionMarker {
			t.Fatalf("message %d compacted despite only 4 messages", i)
		}
	}
}

func TestCompactSkipsAlreadyCompacted(t *testing.T) {
	msgs := []map[string]any{
		{"role": "tool", "content": compactionMarker},
		{"role": "tool", "content": strings.Repeat("a", 1000)},
		{"role": "tool", "content": strings.Repeat("b", 1000)},
		{"role": "tool", "content": strings.Repeat("c", 1000)},
		{"role": "tool", "content": strings.Repeat("d", 1000)},
	}
	out := CompactToolResults(msgs, 100, 0.5)
	if out[0]["content"].(string) != compactionMarker {
		t.Fatal("compacted marker overwritten")
	}
}

func TestCompactSkipsShortResults(t *testing.T) {
	short := "x"
	msgs := []map[string]any{
		{"role": "tool", "content": short},
		{"role": "tool", "content": strings.Repeat("a", 1000)},
		{"role": "tool", "content": strings.Repeat("b", 1000)},
		{"role": "tool", "content": strings.Repeat("c", 1000)},
		{"role": "tool", "content": strings.Repeat("d", 1000)},
	}
	out := CompactToolResults(msgs, 100, 0.5)
	if out[0]["content"].(string) == compactionMarker {
		t.Fatal("short result compacted")
	}
}

func TestDropOldestHalfKeepsSystem(t *testing.T) {
	msgs := []map[string]any{
		{"role": "system", "content": "sys"},
		{"role": "user", "content": "u1"},
		{"role": "assistant", "content": "a1"},
		{"role": "user", "content": "u2"},
		{"role": "assistant", "content": "a2"},
	}
	out := DropOldestHalf(msgs)
	if out[0]["role"] != "system" {
		t.Fatalf("system not preserved: %v", out[0])
	}
	if len(out) > len(msgs) {
		t.Fatal("dropped grew the list")
	}
}

func TestDropOldestHalfRepairsOrphanedToolResults(t *testing.T) {
	// First half (dropped): user + assistant with tool_call.
	// Second half (kept): orphaned tool result + assistant + user.
	msgs := []map[string]any{
		{"role": "user", "content": "u1"},
		{"role": "assistant", "content": "", "tool_calls": []any{map[string]any{"id": "call-1"}}},
		{"role": "tool", "tool_call_id": "call-1", "content": "tr1"},
		{"role": "assistant", "content": "a1"},
		{"role": "user", "content": "u2"},
		{"role": "assistant", "content": "a2"},
	}
	out := DropOldestHalf(msgs)
	for _, m := range out {
		if role, _ := m["role"].(string); role == "tool" {
			t.Fatalf("orphaned tool result kept: %v", m)
		}
	}
}

func TestDropOldestHalfEmpty(t *testing.T) {
	if out := DropOldestHalf(nil); len(out) != 0 {
		t.Fatalf("got %v", out)
	}
}

func TestTruncateUnderBudgetIsNoop(t *testing.T) {
	msgs := []map[string]any{{"role": "tool", "content": "short"}}
	out, cleared := TruncateOldestToolResults(msgs, 1_000_000, "")
	if cleared != 0 {
		t.Fatal("cleared under budget")
	}
	if out[0]["content"].(string) != "short" {
		t.Fatal("modified under budget")
	}
}

func TestTruncateClearsOldestToolResults(t *testing.T) {
	msgs := []map[string]any{
		{"role": "tool", "content": strings.Repeat("a", 1000)},
		{"role": "tool", "content": strings.Repeat("b", 1000)},
	}
	out, cleared := TruncateOldestToolResults(msgs, 50, "")
	if cleared == 0 {
		t.Fatal("nothing cleared")
	}
	if out[0]["content"].(string) != recoveryHardClearMarker {
		t.Fatalf("first not cleared: %q", out[0]["content"])
	}
}

func TestTruncateDoesNotProtectRecent(t *testing.T) {
	// Unlike CompactToolResults, TruncateOldestToolResults clears anything
	// eligible — including the most recent.
	msgs := []map[string]any{
		{"role": "tool", "content": strings.Repeat("a", 1000)},
	}
	out, cleared := TruncateOldestToolResults(msgs, 1, "")
	if cleared != 1 || out[0]["content"].(string) != recoveryHardClearMarker {
		t.Fatalf("got cleared=%d out=%v", cleared, out)
	}
}

func TestTruncateSkipsAlreadyCleared(t *testing.T) {
	msgs := []map[string]any{
		{"role": "tool", "content": recoveryHardClearMarker},
		{"role": "tool", "content": strings.Repeat("a", 1000)},
	}
	out, cleared := TruncateOldestToolResults(msgs, 50, "")
	if cleared != 1 {
		t.Fatalf("expected 1 cleared, got %d", cleared)
	}
	if out[0]["content"].(string) != recoveryHardClearMarker {
		t.Fatal("first overwritten")
	}
}

func TestTruncateReturnsZeroWhenNothingEligible(t *testing.T) {
	msgs := []map[string]any{
		{"role": "user", "content": strings.Repeat("u", 1000)},
	}
	_, cleared := TruncateOldestToolResults(msgs, 1, "")
	if cleared != 0 {
		t.Fatalf("got %d", cleared)
	}
}

// --- summarize_old_chunks ---

func TestSummarizeUnderBudgetIsNoop(t *testing.T) {
	msgs := []map[string]any{{"role": "user", "content": "short"}}
	summarizer := func(_ context.Context, _ []map[string]any) (string, error) { return "S", nil }
	out, summarised, err := SummarizeOldChunks(context.Background(), msgs, 1_000_000, summarizer, 4, 0)
	if err != nil || summarised {
		t.Fatal("unexpected summarisation")
	}
	if len(out) != 1 {
		t.Fatal("modified under budget")
	}
}

func TestSummarizeReplacesOldChunk(t *testing.T) {
	// Each big message ~150 tokens (450 chars / 3 chars-per-token); cap
	// defaults to target/2 = 250 tokens, so individual messages fit.
	// Total non-system is ~600 tokens which exceeds target=500.
	big := strings.Repeat("x", 450)
	msgs := []map[string]any{
		{"role": "system", "content": "sys"},
		{"role": "user", "content": big},
		{"role": "assistant", "content": big},
		{"role": "user", "content": big},
		{"role": "assistant", "content": big},
		{"role": "user", "content": "recent 1"},
		{"role": "assistant", "content": "recent 2"},
		{"role": "user", "content": "recent 3"},
		{"role": "assistant", "content": "recent 4"},
	}
	called := false
	summarizer := func(_ context.Context, _ []map[string]any) (string, error) {
		called = true
		return "USER ASKED ABOUT X; ASSISTANT RESPONDED Y", nil
	}
	out, summarised, err := SummarizeOldChunks(context.Background(), msgs, 500, summarizer, 4, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !summarised || !called {
		t.Fatal("summariser not invoked")
	}
	var found bool
	for _, m := range out {
		if c, _ := m["content"].(string); strings.HasPrefix(c, recoverySummaryPrefix) {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("summary message not inserted")
	}
}

func TestSummarizeSkipsWhenOnlyRecentMessages(t *testing.T) {
	msgs := []map[string]any{
		{"role": "user", "content": strings.Repeat("x", 1000)},
		{"role": "assistant", "content": strings.Repeat("y", 1000)},
	}
	summarizer := func(_ context.Context, _ []map[string]any) (string, error) { return "S", nil }
	_, summarised, err := SummarizeOldChunks(context.Background(), msgs, 1, summarizer, 4, 0)
	if err != nil || summarised {
		t.Fatalf("got summarised=%v err=%v", summarised, err)
	}
}

func TestSummarizeRepairsOrphanedToolResults(t *testing.T) {
	big := strings.Repeat("x", 600) // ~200 tokens
	msgs := []map[string]any{
		{"role": "user", "content": big},
		{"role": "assistant", "content": "", "tool_calls": []any{map[string]any{"id": "c1"}}},
		{"role": "tool", "tool_call_id": "c1", "content": big},
		{"role": "user", "content": big},
		{"role": "assistant", "content": big},
		// keepRecent=4
		{"role": "user", "content": "r1"},
		{"role": "assistant", "content": "r2"},
		{"role": "user", "content": "r3"},
		{"role": "assistant", "content": "r4"},
	}
	summarizer := func(_ context.Context, _ []map[string]any) (string, error) { return "summary", nil }
	out, summarised, err := SummarizeOldChunks(context.Background(), msgs, 500, summarizer, 4, 0)
	if err != nil || !summarised {
		t.Fatalf("expected summarisation; got summarised=%v err=%v", summarised, err)
	}
	for _, m := range out {
		if role, _ := m["role"].(string); role == "tool" {
			t.Fatal("orphaned tool result kept")
		}
	}
}

func TestSummarizeBailsWhenFirstEligibleExceedsCap(t *testing.T) {
	msgs := []map[string]any{
		{"role": "user", "content": strings.Repeat("x", 10_000)},
		{"role": "user", "content": "tail1"},
		{"role": "user", "content": "tail2"},
		{"role": "user", "content": "tail3"},
		{"role": "user", "content": "tail4"},
	}
	summarizer := func(_ context.Context, _ []map[string]any) (string, error) { return "S", nil }
	out, summarised, err := SummarizeOldChunks(context.Background(), msgs, 1000, summarizer, 4, 100)
	if err != nil || summarised {
		t.Fatalf("expected bail; got summarised=%v err=%v out=%v", summarised, err, out)
	}
}

func TestSummarizeReturnsUnchangedWhenSummarizerEmpty(t *testing.T) {
	big := strings.Repeat("x", 450)
	msgs := []map[string]any{
		{"role": "system", "content": "sys"},
		{"role": "user", "content": big},
		{"role": "assistant", "content": big},
		{"role": "user", "content": big},
		{"role": "assistant", "content": big},
		{"role": "user", "content": "r1"},
		{"role": "assistant", "content": "r2"},
		{"role": "user", "content": "r3"},
		{"role": "assistant", "content": "r4"},
	}
	summarizer := func(_ context.Context, _ []map[string]any) (string, error) { return "", nil }
	out, summarised, err := SummarizeOldChunks(context.Background(), msgs, 500, summarizer, 4, 0)
	if err != nil || summarised || len(out) != len(msgs) {
		t.Fatalf("got summarised=%v err=%v len=%d", summarised, err, len(out))
	}
}

func TestSummarizePropagatesSummarizerError(t *testing.T) {
	big := strings.Repeat("x", 450)
	msgs := []map[string]any{
		{"role": "system", "content": "sys"},
		{"role": "user", "content": big},
		{"role": "assistant", "content": big},
		{"role": "user", "content": big},
		{"role": "assistant", "content": big},
		{"role": "user", "content": "r1"},
		{"role": "assistant", "content": "r2"},
		{"role": "user", "content": "r3"},
		{"role": "assistant", "content": "r4"},
	}
	boom := errors.New("boom")
	summarizer := func(_ context.Context, _ []map[string]any) (string, error) { return "", boom }
	_, _, err := SummarizeOldChunks(context.Background(), msgs, 500, summarizer, 4, 0)
	if !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
}
