package workspace

import (
	"context"
	"strings"
	"testing"
	"time"
)

// Ported from tests/test_tools_workspace.py — TestExecTool +
// TestExecToolStreaming.

func mustExecTool(t *testing.T, opts ExecOptions) *ExecTool {
	t.Helper()
	tool, err := NewExecTool(opts)
	if err != nil {
		t.Fatal(err)
	}
	return tool
}

func TestExec_RunsCommandAndReturnsStdout(t *testing.T) {
	tool := mustExecTool(t, ExecOptions{Timeout: 5 * time.Second})
	out, _ := tool.Execute(context.Background(), map[string]any{"command": "echo hello"})
	if !strings.Contains(out, "hello") {
		t.Fatalf("got %q", out)
	}
}

func TestExec_NonZeroExitIncludesStatus(t *testing.T) {
	tool := mustExecTool(t, ExecOptions{Timeout: 5 * time.Second})
	out, _ := tool.Execute(context.Background(), map[string]any{"command": "exit 3"})
	if !strings.Contains(out, "Exit code: 3") {
		t.Fatalf("got %q", out)
	}
}

func TestExec_StderrShownWithPrefix(t *testing.T) {
	tool := mustExecTool(t, ExecOptions{Timeout: 5 * time.Second})
	out, _ := tool.Execute(context.Background(), map[string]any{
		"command": "echo oops 1>&2",
	})
	if !strings.Contains(out, "STDERR:") || !strings.Contains(out, "oops") {
		t.Fatalf("got %q", out)
	}
}

func TestExec_TimeoutEnforced(t *testing.T) {
	tool := mustExecTool(t, ExecOptions{Timeout: 200 * time.Millisecond})
	out, _ := tool.Execute(context.Background(), map[string]any{"command": "sleep 5"})
	if !strings.Contains(out, "timed out") {
		t.Fatalf("got %q", out)
	}
}

func TestExec_GuardBlocksDangerousPattern(t *testing.T) {
	tool := mustExecTool(t, ExecOptions{Timeout: 5 * time.Second})
	out, _ := tool.Execute(context.Background(), map[string]any{"command": "rm -rf /tmp/x"})
	if !strings.Contains(out, "blocked by safety guard") {
		t.Fatalf("got %q", out)
	}
}

func TestExec_GuardBlocksShutdown(t *testing.T) {
	tool := mustExecTool(t, ExecOptions{Timeout: 5 * time.Second})
	out, _ := tool.Execute(context.Background(), map[string]any{"command": "shutdown -h now"})
	if !strings.Contains(out, "blocked") {
		t.Fatalf("got %q", out)
	}
}

func TestExec_AllowlistAcceptsMatchingCommand(t *testing.T) {
	tool := mustExecTool(t, ExecOptions{
		Timeout:       5 * time.Second,
		AllowPatterns: []string{`^echo `},
	})
	out, _ := tool.Execute(context.Background(), map[string]any{"command": "echo ok"})
	if !strings.Contains(out, "ok") {
		t.Fatalf("got %q", out)
	}
}

func TestExec_AllowlistRejectsNonMatching(t *testing.T) {
	tool := mustExecTool(t, ExecOptions{
		Timeout:       5 * time.Second,
		AllowPatterns: []string{`^echo `},
	})
	out, _ := tool.Execute(context.Background(), map[string]any{"command": "ls /tmp"})
	if !strings.Contains(out, "not in allowlist") {
		t.Fatalf("got %q", out)
	}
}

func TestExec_StreamingEmitsStdout(t *testing.T) {
	tool := mustExecTool(t, ExecOptions{Timeout: 5 * time.Second})
	ch, err := tool.ExecuteStreaming(context.Background(), map[string]any{"command": "echo streamed"})
	if err != nil {
		t.Fatal(err)
	}
	var assembled strings.Builder
	for c := range ch {
		assembled.WriteString(c)
	}
	if !strings.Contains(assembled.String(), "streamed") {
		t.Fatalf("got %q", assembled.String())
	}
}

func TestExec_StreamingTimeoutSurfaced(t *testing.T) {
	tool := mustExecTool(t, ExecOptions{Timeout: 200 * time.Millisecond})
	ch, _ := tool.ExecuteStreaming(context.Background(), map[string]any{"command": "sleep 5"})
	var assembled strings.Builder
	for c := range ch {
		assembled.WriteString(c)
	}
	if !strings.Contains(assembled.String(), "timed out") {
		t.Fatalf("got %q", assembled.String())
	}
}

func TestExec_GuardedStreamingShortCircuits(t *testing.T) {
	tool := mustExecTool(t, ExecOptions{Timeout: 5 * time.Second})
	ch, _ := tool.ExecuteStreaming(context.Background(), map[string]any{"command": "rm -rf /tmp/x"})
	got := <-ch
	for range ch { /* drain */
	}
	if !strings.Contains(got, "blocked by safety guard") {
		t.Fatalf("got %q", got)
	}
}
