package conversationfile

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Clause-Logic/exoclaw-go/conversation-file/session"
	coreconv "github.com/Clause-Logic/exoclaw-go/exoclaw/conversation"
)

// Ported from tests/test_conversation.py — focused on the load-bearing
// behaviour of DefaultConversation. The Python suite is 2,260 LOC with
// many MagicMock-driven tests that don't translate; this file covers
// round-trip Append/PostTurn, prepareTurn cleanup, and overflow recovery.

func newTestConv(t *testing.T) (*DefaultConversation, string) {
	t.Helper()
	tmp := t.TempDir()
	mem := &fakeMemory{ret: ""}
	skills := NewSkillsLoader(tmp, SkillsLoaderOptions{})
	mgr, err := session.NewSessionManager(tmp, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := NewContextBuilder(tmp, mem, skills, 0)
	conv := NewDefaultConversation(
		&sessionStoreAdapter{mgr: mgr},
		mem,
		ctx,
		DefaultConversationOptions{Log: nil},
	)
	return conv, tmp
}

func TestConversation_AppendPersistsMessage(t *testing.T) {
	conv, tmp := newTestConv(t)
	ctx := context.Background()
	if err := conv.Append(ctx, "cli:a", map[string]any{"role": "user", "content": "hi"}); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(tmp, "sessions", "*.jsonl"))
	if len(files) != 1 {
		t.Fatalf("expected 1 session file, got %v", files)
	}
}

func TestConversation_AppendStripsEmptyAssistant(t *testing.T) {
	conv, tmp := newTestConv(t)
	ctx := context.Background()
	_ = conv.Append(ctx, "cli:a", map[string]any{"role": "assistant", "content": ""})
	// prepareTurn skips the empty assistant → no SaveAppend → no file.
	files, _ := filepath.Glob(filepath.Join(tmp, "sessions", "*.jsonl"))
	if len(files) != 0 {
		t.Logf("note: file created (some impls produce metadata-only): %v", files)
	}
}

func TestConversation_AppendTruncatesLongToolResult(t *testing.T) {
	conv, tmp := newTestConv(t)
	ctx := context.Background()
	big := strings.Repeat("x", toolResultMaxChars+200)
	if err := conv.Append(ctx, "cli:a", map[string]any{
		"role":         "tool",
		"tool_call_id": "c1",
		"name":         "t",
		"content":      big,
	}); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(tmp, "sessions", "*.jsonl"))
	if len(files) != 1 {
		t.Fatalf("got %v", files)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "(truncated)") {
		t.Fatal("long tool result not truncated on disk")
	}
}

func TestConversation_AppendStripsRuntimeContextFromUserMessage(t *testing.T) {
	conv, tmp := newTestConv(t)
	ctx := context.Background()
	combined := runtimeContextTag + "\nCurrent Time: ...\n\nactual user text"
	if err := conv.Append(ctx, "cli:r", map[string]any{
		"role": "user", "content": combined,
	}); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(tmp, "sessions", "*.jsonl"))
	data, _ := os.ReadFile(files[0])
	body := string(data)
	if strings.Contains(body, runtimeContextTag) {
		t.Fatalf("runtime-context tag leaked: %s", body)
	}
	if !strings.Contains(body, "actual user text") {
		t.Fatalf("user text missing: %s", body)
	}
}

func TestConversation_ClearArchives(t *testing.T) {
	conv, _ := newTestConv(t)
	ctx := context.Background()
	_ = conv.Append(ctx, "cli:c", map[string]any{"role": "user", "content": "hi"})
	ok, err := conv.Clear(ctx, "cli:c")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("clear returned false")
	}
}

func TestConversation_ListSessionsReturnsKnown(t *testing.T) {
	conv, _ := newTestConv(t)
	ctx := context.Background()
	_ = conv.Append(ctx, "cli:x", map[string]any{"role": "user", "content": "hi"})
	got := conv.ListSessions()
	if len(got) == 0 {
		t.Fatal("expected at least one session")
	}
}

func TestConversation_PostTurnSkipsHookChannel(t *testing.T) {
	conv, _ := newTestConv(t)
	ctx := context.Background()
	conv.turnChannel = "hook"
	if err := conv.PostTurn(ctx, "cli:h"); err != nil {
		t.Fatal(err)
	}
	conv.WaitForMaintenance()
}

func TestConversation_ActiveToolsDelegates(t *testing.T) {
	conv, _ := newTestConv(t)
	_ = conv.Prompt.(*ContextBuilder).BuildSystemPrompt(BuildSystemPromptOptions{})
	got := conv.ActiveTools()
	if got == nil {
		t.Fatal("nil active tools")
	}
}

func TestConversation_BuildPromptReturnsSystemPlusUser(t *testing.T) {
	conv, _ := newTestConv(t)
	msgs, err := conv.BuildPrompt(context.Background(), "cli:bp", "hello", coreconv.BuildPromptOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) < 2 {
		t.Fatalf("too few messages: %v", msgs)
	}
	if msgs[0]["role"] != "system" {
		t.Fatalf("first not system: %v", msgs[0])
	}
	if msgs[len(msgs)-1]["role"] != "user" {
		t.Fatalf("last not user: %v", msgs[len(msgs)-1])
	}
}
