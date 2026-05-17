package workspace

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Ported from tests/test_tools_workspace.py — TestResolvePath,
// TestReadFileTool, TestWriteFileTool, TestEditFileTool, TestListDirTool,
// plus the streaming and edge-case classes.

// ---------- resolvePath ----------

func TestResolvePath_RelativeJoinsWorkspace(t *testing.T) {
	tmp := t.TempDir()
	resolved, err := resolvePath("a/b.txt", tmp, "")
	if err != nil {
		t.Fatal(err)
	}
	want, _ := filepath.EvalSymlinks(tmp)
	if !strings.HasPrefix(resolved, want) {
		t.Fatalf("resolved %q not under %q", resolved, want)
	}
}

func TestResolvePath_RejectsDotDotSegments(t *testing.T) {
	tmp := t.TempDir()
	if _, err := resolvePath("../escape", tmp, ""); err == nil {
		t.Fatal("expected reject")
	}
	if _, err := resolvePath("a/../b", tmp, ""); err == nil {
		t.Fatal("expected reject")
	}
}

func TestResolvePath_StripsWorkspacePrefix(t *testing.T) {
	tmp := t.TempDir()
	base := filepath.Base(tmp)
	resolved, err := resolvePath(base+"/foo.txt", tmp, "")
	if err != nil {
		t.Fatal(err)
	}
	want, _ := filepath.EvalSymlinks(tmp)
	if !strings.HasSuffix(resolved, filepath.Join(want, "foo.txt")) {
		t.Fatalf("double-nest leaked: %q", resolved)
	}
}

func TestResolvePath_RejectsOutsideSandbox(t *testing.T) {
	tmp := t.TempDir()
	if _, err := resolvePath("/etc/passwd", tmp, tmp); err == nil {
		t.Fatal("expected outside-sandbox reject")
	}
}

// ---------- ReadFileTool ----------

func TestReadFile_ReadsContent(t *testing.T) {
	tmp := t.TempDir()
	p := filepath.Join(tmp, "hello.txt")
	if err := os.WriteFile(p, []byte("hi there"), 0o644); err != nil {
		t.Fatal(err)
	}
	tool := NewReadFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{"path": "hello.txt"})
	if got != "hi there" {
		t.Fatalf("got %q", got)
	}
}

func TestReadFile_NotFoundError(t *testing.T) {
	tmp := t.TempDir()
	tool := NewReadFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{"path": "missing.txt"})
	if !strings.HasPrefix(got, "Error: File not found") {
		t.Fatalf("got %q", got)
	}
}

func TestReadFile_NotAFileError(t *testing.T) {
	tmp := t.TempDir()
	sub := filepath.Join(tmp, "subdir")
	_ = os.Mkdir(sub, 0o755)
	tool := NewReadFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{"path": "subdir"})
	if !strings.Contains(got, "Not a file") {
		t.Fatalf("got %q", got)
	}
}

func TestReadFile_RangedRead(t *testing.T) {
	tmp := t.TempDir()
	var lines []string
	for i := 0; i < 10; i++ {
		lines = append(lines, "line "+string(rune('0'+i)))
	}
	p := filepath.Join(tmp, "lines.txt")
	_ = os.WriteFile(p, []byte(strings.Join(lines, "\n")+"\n"), 0o644)
	tool := NewReadFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{
		"path": "lines.txt", "offset": 2, "limit": 3,
	})
	if !strings.Contains(got, "[lines 3-5 of 10]") {
		t.Fatalf("missing header: %q", got)
	}
	if !strings.Contains(got, "line 2") || !strings.Contains(got, "line 4") {
		t.Fatalf("missing body: %q", got)
	}
}

func TestReadFile_RejectsNegativeOffset(t *testing.T) {
	tmp := t.TempDir()
	tool := NewReadFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{"path": "x", "offset": -1})
	if !strings.Contains(got, "offset must be >= 0") {
		t.Fatalf("got %q", got)
	}
}

func TestReadFile_RejectsZeroLimit(t *testing.T) {
	tmp := t.TempDir()
	tool := NewReadFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{"path": "x", "limit": 0})
	if !strings.Contains(got, "limit must be >= 1") {
		t.Fatalf("got %q", got)
	}
}

func TestReadFile_TooLargeWithoutOffsetReturnsHint(t *testing.T) {
	tmp := t.TempDir()
	p := filepath.Join(tmp, "big.txt")
	if err := os.WriteFile(p, []byte(strings.Repeat("x", 1000)), 0o644); err != nil {
		t.Fatal(err)
	}
	tool := NewReadFileTool(tmp, "")
	tool.MaxCharsOverride = 100 // 4*100 = 400 cap
	got, _ := tool.Execute(context.Background(), map[string]any{"path": "big.txt"})
	if !strings.Contains(got, "Use offset and limit") {
		t.Fatalf("got %q", got)
	}
}

func TestReadFile_TruncatesAtMaxChars(t *testing.T) {
	tmp := t.TempDir()
	p := filepath.Join(tmp, "med.txt")
	if err := os.WriteFile(p, []byte(strings.Repeat("y", 150)), 0o644); err != nil {
		t.Fatal(err)
	}
	tool := NewReadFileTool(tmp, "")
	tool.MaxCharsOverride = 50
	got, _ := tool.Execute(context.Background(), map[string]any{"path": "med.txt"})
	if !strings.Contains(got, "truncated") {
		t.Fatalf("got %q", got)
	}
}

// ---------- ReadFileTool streaming ----------

func TestReadFile_StreamingYieldsChunks(t *testing.T) {
	tmp := t.TempDir()
	body := strings.Repeat("z", 20_000)
	if err := os.WriteFile(filepath.Join(tmp, "s.txt"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	tool := NewReadFileTool(tmp, "")
	ch, err := tool.ExecuteStreaming(context.Background(), map[string]any{"path": "s.txt"})
	if err != nil {
		t.Fatal(err)
	}
	var assembled strings.Builder
	chunkCount := 0
	for c := range ch {
		assembled.WriteString(c)
		chunkCount++
	}
	if assembled.String() != body {
		t.Fatalf("body mismatch: got %d want %d", assembled.Len(), len(body))
	}
	if chunkCount < 2 {
		t.Fatalf("expected multiple chunks, got %d", chunkCount)
	}
}

func TestReadFile_StreamingValidationErrors(t *testing.T) {
	tool := NewReadFileTool(t.TempDir(), "")
	ch, _ := tool.ExecuteStreaming(context.Background(), map[string]any{"path": "x", "offset": -1})
	first := <-ch
	for range ch { /* drain */
	}
	if !strings.Contains(first, "offset must be >= 0") {
		t.Fatalf("got %q", first)
	}
}

// ---------- WriteFileTool ----------

func TestWriteFile_CreatesParentDirs(t *testing.T) {
	tmp := t.TempDir()
	tool := NewWriteFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{
		"path": "a/b/c.txt", "content": "hello",
	})
	if !strings.HasPrefix(got, "Successfully wrote") {
		t.Fatalf("got %q", got)
	}
	body, err := os.ReadFile(filepath.Join(tmp, "a", "b", "c.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "hello" {
		t.Fatalf("body: %q", body)
	}
}

func TestWriteFile_RejectsTraversal(t *testing.T) {
	tmp := t.TempDir()
	tool := NewWriteFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{
		"path": "../escape.txt", "content": "x",
	})
	if !strings.HasPrefix(got, "Error:") {
		t.Fatalf("got %q", got)
	}
}

// ---------- EditFileTool ----------

func TestEditFile_ReplacesUniqueMatch(t *testing.T) {
	tmp := t.TempDir()
	p := filepath.Join(tmp, "f.txt")
	_ = os.WriteFile(p, []byte("alpha bravo charlie"), 0o644)
	tool := NewEditFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{
		"path": "f.txt", "old_text": "bravo", "new_text": "BRAVO",
	})
	if !strings.HasPrefix(got, "Successfully edited") {
		t.Fatalf("got %q", got)
	}
	body, _ := os.ReadFile(p)
	if string(body) != "alpha BRAVO charlie" {
		t.Fatalf("body: %q", body)
	}
}

func TestEditFile_RejectsAmbiguousMatch(t *testing.T) {
	tmp := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmp, "f.txt"), []byte("xx xx xx"), 0o644)
	tool := NewEditFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{
		"path": "f.txt", "old_text": "xx", "new_text": "yy",
	})
	if !strings.Contains(got, "appears 3 times") {
		t.Fatalf("got %q", got)
	}
}

func TestEditFile_NotFoundGivesProximityHint(t *testing.T) {
	tmp := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmp, "f.txt"), []byte("the quick brown fox"), 0o644)
	tool := NewEditFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{
		"path": "f.txt", "old_text": "the quick brwn fox", "new_text": "X",
	})
	if !strings.Contains(got, "Closest match") {
		t.Fatalf("got %q", got)
	}
}

func TestEditFile_NotFoundNoSimilar(t *testing.T) {
	tmp := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmp, "f.txt"), []byte("hello world"), 0o644)
	tool := NewEditFileTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{
		"path": "f.txt", "old_text": "QQQ", "new_text": "X",
	})
	if !strings.Contains(got, "No similar text found") {
		t.Fatalf("got %q", got)
	}
}

// ---------- ListDirTool ----------

func TestListDir_FilesAndDirs(t *testing.T) {
	tmp := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmp, "a.txt"), []byte("x"), 0o644)
	_ = os.Mkdir(filepath.Join(tmp, "sub"), 0o755)
	tool := NewListDirTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{"path": "."})
	if !strings.Contains(got, "[f] a.txt") || !strings.Contains(got, "[d] sub") {
		t.Fatalf("got %q", got)
	}
}

func TestListDir_EmptyDir(t *testing.T) {
	tmp := t.TempDir()
	tool := NewListDirTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{"path": "."})
	if !strings.Contains(got, "is empty") {
		t.Fatalf("got %q", got)
	}
}

func TestListDir_NotFoundError(t *testing.T) {
	tmp := t.TempDir()
	tool := NewListDirTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{"path": "nope"})
	if !strings.Contains(got, "not found") {
		t.Fatalf("got %q", got)
	}
}

func TestListDir_NotADirError(t *testing.T) {
	tmp := t.TempDir()
	_ = os.WriteFile(filepath.Join(tmp, "f.txt"), []byte("x"), 0o644)
	tool := NewListDirTool(tmp, "")
	got, _ := tool.Execute(context.Background(), map[string]any{"path": "f.txt"})
	if !strings.Contains(got, "Not a directory") {
		t.Fatalf("got %q", got)
	}
}
