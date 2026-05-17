package conversationfile

import (
	"testing"

	"github.com/standd/exoclaw-go/conversation-file/session"
)

// Ported from tests/test_streaming_history.py.
//
// Verifies that when SessionManager is constructed with streamingHistory=true,
// the in-memory session.Messages slice is NOT populated by reload — the tail
// lives only on disk. ReadHistory still works by streaming.

func TestStreamingHistory_LoadKeepsMessagesEmpty(t *testing.T) {
	tmp := t.TempDir()
	mgr, err := session.NewSessionManager(tmp, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	sess := mgr.GetOrCreate("cli:s")
	msgs := []map[string]any{
		{"role": "user", "content": "u1", "timestamp": "2020-01-01"},
		{"role": "assistant", "content": "a1", "timestamp": "2020-01-01"},
	}
	if err := mgr.SaveAppend(sess, msgs); err != nil {
		t.Fatal(err)
	}
	mgr.Invalidate("cli:s")
	reloaded := mgr.GetOrCreate("cli:s")
	if got := reloaded.Messages(); len(got) != 0 {
		t.Fatalf("expected empty in-memory under streaming, got %d", len(got))
	}
	if got := reloaded.TotalMessages(); got != 2 {
		t.Fatalf("total: %d", got)
	}
}

func TestStreamingHistory_ReadHistoryStreamsFromDisk(t *testing.T) {
	tmp := t.TempDir()
	mgr, _ := session.NewSessionManager(tmp, true, nil)
	sess := mgr.GetOrCreate("cli:s")
	msgs := []map[string]any{
		{"role": "user", "content": "first"},
		{"role": "assistant", "content": "second"},
	}
	_ = mgr.SaveAppend(sess, msgs)
	mgr.Invalidate("cli:s")
	got := mgr.ReadHistory("cli:s", -1)
	if len(got) != 2 {
		t.Fatalf("got %d", len(got))
	}
	if got[0]["content"] != "first" {
		t.Fatalf("first: %v", got[0])
	}
}

func TestStreamingHistory_NonStreamingPopulatesInMemory(t *testing.T) {
	tmp := t.TempDir()
	mgr, _ := session.NewSessionManager(tmp, false, nil)
	sess := mgr.GetOrCreate("cli:n")
	_ = mgr.SaveAppend(sess, []map[string]any{{"role": "user", "content": "hi"}})
	mgr.Invalidate("cli:n")
	reloaded := mgr.GetOrCreate("cli:n")
	if got := reloaded.Messages(); len(got) != 1 {
		t.Fatalf("expected in-memory populated, got %v", got)
	}
}
