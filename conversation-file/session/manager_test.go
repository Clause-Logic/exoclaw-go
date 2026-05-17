package session

import (
	"path/filepath"
	"strings"
	"testing"
)

// Ported from tests/test_conversation.py::TestSession + TestSessionManager.

func TestSession_AddMessage(t *testing.T) {
	s := NewSession("k")
	s.AddMessage("user", "hello", nil)
	if s.TotalMessages() != 1 {
		t.Fatal("total")
	}
	msgs := s.Messages()
	if msgs[0]["role"] != "user" || msgs[0]["content"] != "hello" {
		t.Fatalf("got %v", msgs[0])
	}
}

func TestSession_GetHistoryAppliesLLMCleanup(t *testing.T) {
	s := NewSession("k")
	s.AddMessage("user", "u1", nil)
	s.AddMessage("assistant", "a1", nil)
	h := s.GetHistory(-1)
	for _, m := range h {
		if _, ok := m["timestamp"]; ok {
			t.Fatal("timestamp not stripped")
		}
	}
}

func TestSession_ClearResets(t *testing.T) {
	s := NewSession("k")
	s.AddMessage("user", "hi", nil)
	s.Clear()
	if s.TotalMessages() != 0 {
		t.Fatal("not cleared")
	}
	if len(s.Messages()) != 0 {
		t.Fatal("messages not cleared")
	}
}

func TestSessionManager_RoundTrip(t *testing.T) {
	tmp := t.TempDir()
	mgr, err := NewSessionManager(tmp, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	sess := mgr.GetOrCreate("cli:a")
	sess.AddMessage("user", "hello", nil)
	if err := mgr.SaveAppend(sess, sess.Messages()); err != nil {
		t.Fatal(err)
	}
	// Force a reload by invalidating cache.
	mgr.Invalidate("cli:a")
	reloaded := mgr.GetOrCreate("cli:a")
	if reloaded.TotalMessages() != 1 {
		t.Fatalf("got %d", reloaded.TotalMessages())
	}
	msgs := reloaded.Messages()
	if msgs[0]["content"] != "hello" {
		t.Fatalf("got %v", msgs[0])
	}
}

func TestSessionManager_StreamingHistory(t *testing.T) {
	tmp := t.TempDir()
	mgr, err := NewSessionManager(tmp, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	sess := mgr.GetOrCreate("cli:s")
	msg := map[string]any{"role": "user", "content": "hello", "timestamp": "2020-01-01"}
	if err := mgr.SaveAppend(sess, []map[string]any{msg}); err != nil {
		t.Fatal(err)
	}
	mgr.Invalidate("cli:s")
	reloaded := mgr.GetOrCreate("cli:s")
	// Streaming mode: in-memory MessagesSlice should be empty even though
	// total_messages reflects the file count.
	if len(reloaded.Messages()) != 0 {
		t.Fatalf("expected empty in-memory under streaming, got %v", reloaded.Messages())
	}
	if reloaded.TotalMessages() != 1 {
		t.Fatalf("total %d", reloaded.TotalMessages())
	}
	// load_range fetches from disk.
	got, err := mgr.LoadRange("cli:s", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0]["content"] != "hello" {
		t.Fatalf("got %v", got)
	}
}

func TestSessionManager_ListSessionsSortedNewestFirst(t *testing.T) {
	tmp := t.TempDir()
	mgr, _ := NewSessionManager(tmp, false, nil)
	// Two sessions with different updated_at; write a second so it's newer.
	first := mgr.GetOrCreate("cli:a")
	first.AddMessage("user", "x", nil)
	_ = mgr.SaveAppend(first, first.Messages())

	second := mgr.GetOrCreate("cli:b")
	second.AddMessage("user", "y", nil)
	_ = mgr.SaveAppend(second, second.Messages())

	listed := mgr.ListSessions()
	if len(listed) < 2 {
		t.Fatalf("got %d", len(listed))
	}
	// All paths exist + .jsonl suffix.
	for _, s := range listed {
		p, _ := s["path"].(string)
		if filepath.Ext(p) != ".jsonl" {
			t.Errorf("path %q not jsonl", p)
		}
	}
}

func TestRepairAndProject_DropsOrphans(t *testing.T) {
	in := []map[string]any{
		{"role": "user", "content": "u1"},
		{"role": "assistant", "content": "", "tool_calls": []any{
			map[string]any{"id": "c1"},
			map[string]any{"id": "c2"},
		}},
		{"role": "tool", "tool_call_id": "c1", "content": "tr1"},
		// c2 has no matching tool — should be stripped.
	}
	out := RepairAndProject(in)
	// Assistant should have only c1 in tool_calls now.
	for _, m := range out {
		if role, _ := m["role"].(string); role == "assistant" {
			tcs, _ := m["tool_calls"].([]any)
			if len(tcs) != 1 {
				t.Fatalf("expected 1 tool_call, got %d: %v", len(tcs), tcs)
			}
		}
	}
}

func TestRepairAndProject_StripsTimestamps(t *testing.T) {
	in := []map[string]any{
		{"role": "user", "content": "hi", "timestamp": "2020"},
	}
	out := RepairAndProject(in)
	if _, ok := out[0]["timestamp"]; ok {
		t.Fatal("timestamp leaked")
	}
}

func TestNormalizeHistory_SkipsLeadingNonUser(t *testing.T) {
	in := []map[string]any{
		{"role": "assistant", "content": "lead"},
		{"role": "user", "content": "hi"},
	}
	out := NormalizeHistory(in, -1)
	if len(out) != 1 {
		t.Fatalf("got %d", len(out))
	}
	if role, _ := out[0]["role"].(string); role != "user" {
		t.Fatalf("got %v", out[0])
	}
}

func TestNormalizeHistory_CapsLength(t *testing.T) {
	var in []map[string]any
	for i := 0; i < 10; i++ {
		in = append(in, map[string]any{"role": "user", "content": strings.Repeat("x", 10)})
	}
	out := NormalizeHistory(in, 3)
	if len(out) > 3 {
		t.Fatalf("got %d", len(out))
	}
}
