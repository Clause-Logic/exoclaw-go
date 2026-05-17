package cron

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// Ported from tests/test_cron.py — TestCronToolExecute*, TestCronTool*.

func newToolService(t *testing.T) (*Tool, *Service) {
	t.Helper()
	svc := NewService(filepath.Join(t.TempDir(), "cron.json"), func(_ context.Context, _ *CronJob) error { return nil })
	return NewTool(NewLocalBackend(svc)), svc
}

func ctxWithSession() context.Context {
	ctx := WithChannel(context.Background(), "cli")
	return WithChatID(ctx, "direct")
}

func TestTool_AddEvery(t *testing.T) {
	tool, svc := newToolService(t)
	out, err := tool.Execute(ctxWithSession(), map[string]any{
		"action": "add", "message": "ping", "every_seconds": 30,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out, "Created job 'ping'") {
		t.Fatalf("got %q", out)
	}
	if jobs := svc.ListJobs(false); len(jobs) != 1 || jobs[0].Schedule.EveryMS != 30_000 {
		t.Fatalf("not stored: %+v", jobs)
	}
}

func TestTool_AddRequiresSessionContext(t *testing.T) {
	tool, _ := newToolService(t)
	out, _ := tool.Execute(context.Background(), map[string]any{
		"action": "add", "message": "ping", "every_seconds": 30,
	})
	if !strings.Contains(out, "no session context") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_AddRefusesInsideCronContext(t *testing.T) {
	tool, _ := newToolService(t)
	ctx := WithInCronContext(ctxWithSession(), true)
	out, _ := tool.Execute(ctx, map[string]any{
		"action": "add", "message": "p", "every_seconds": 10,
	})
	if !strings.Contains(out, "cannot schedule new jobs") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_AddRejectsBadISODatetime(t *testing.T) {
	tool, _ := newToolService(t)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{
		"action": "add", "message": "m", "at": "not a date",
	})
	if !strings.Contains(out, "invalid ISO datetime") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_AddOneShotAtDeleteAfter(t *testing.T) {
	tool, svc := newToolService(t)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{
		"action": "add", "message": "one", "at": "2099-01-01T00:00:00",
	})
	if !strings.HasPrefix(out, "Created job") {
		t.Fatalf("got %q", out)
	}
	jobs := svc.ListJobs(false)
	if len(jobs) != 1 || !jobs[0].DeleteAfterRun {
		t.Fatalf("delete_after_run not set: %+v", jobs)
	}
}

func TestTool_AddRequiresMessage(t *testing.T) {
	tool, _ := newToolService(t)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{
		"action": "add", "every_seconds": 30,
	})
	if !strings.Contains(out, "message is required") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_AddRequiresScheduleArg(t *testing.T) {
	tool, _ := newToolService(t)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{
		"action": "add", "message": "m",
	})
	if !strings.Contains(out, "every_seconds, cron_expr, or at is required") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_AddRejectsTzWithoutCronExpr(t *testing.T) {
	tool, _ := newToolService(t)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{
		"action": "add", "message": "m", "every_seconds": 30, "tz": "UTC",
	})
	if !strings.Contains(out, "tz can only be used with cron_expr") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_AddRejectsUnknownTimezone(t *testing.T) {
	tool, _ := newToolService(t)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{
		"action": "add", "message": "m", "cron_expr": "0 9 * * *", "tz": "Atlantis/Lost",
	})
	if !strings.Contains(out, "unknown timezone") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_AddUnknownWakeModeRejected(t *testing.T) {
	tool, _ := newToolService(t)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{
		"action": "add", "message": "m", "every_seconds": 30, "wake_mode": "asap",
	})
	if !strings.Contains(out, "unknown wake_mode") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_AddWakeModeNextHeartbeat(t *testing.T) {
	tool, svc := newToolService(t)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{
		"action": "add", "message": "m", "every_seconds": 30, "wake_mode": "next-heartbeat",
	})
	if !strings.HasPrefix(out, "Created job") {
		t.Fatalf("got %q", out)
	}
	if jobs := svc.ListJobs(false); jobs[0].WakeMode != WakeNextHeartbeat {
		t.Fatalf("wake_mode: %v", jobs[0].WakeMode)
	}
}

func TestTool_ListEmpty(t *testing.T) {
	tool, _ := newToolService(t)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{"action": "list"})
	if out != "No scheduled jobs." {
		t.Fatalf("got %q", out)
	}
}

func TestTool_ListPopulated(t *testing.T) {
	tool, _ := newToolService(t)
	_, _ = tool.Execute(ctxWithSession(), map[string]any{"action": "add", "message": "ping", "every_seconds": 30})
	out, _ := tool.Execute(ctxWithSession(), map[string]any{"action": "list"})
	if !strings.Contains(out, "ping") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_RemoveByID(t *testing.T) {
	tool, svc := newToolService(t)
	_, _ = tool.Execute(ctxWithSession(), map[string]any{"action": "add", "message": "m", "every_seconds": 30})
	jobs := svc.ListJobs(false)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{"action": "remove", "job_id": jobs[0].ID})
	if !strings.HasPrefix(out, "Removed job") {
		t.Fatalf("got %q", out)
	}
	if got := svc.ListJobs(false); len(got) != 0 {
		t.Fatalf("still present: %v", got)
	}
}

func TestTool_RemoveMissingReturnsNotFound(t *testing.T) {
	tool, _ := newToolService(t)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{"action": "remove", "job_id": "nope"})
	if !strings.Contains(out, "not found") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_UpdateMessage(t *testing.T) {
	tool, svc := newToolService(t)
	_, _ = tool.Execute(ctxWithSession(), map[string]any{"action": "add", "message": "old", "every_seconds": 30})
	id := svc.ListJobs(false)[0].ID
	out, _ := tool.Execute(ctxWithSession(), map[string]any{
		"action": "update", "job_id": id, "message": "fresh body",
	})
	if !strings.HasPrefix(out, "Updated job") {
		t.Fatalf("got %q", out)
	}
	if svc.GetJob(id).Payload.Message != "fresh body" {
		t.Fatal("payload not updated")
	}
}

func TestTool_UpdateUnknownWakeModeRejected(t *testing.T) {
	tool, svc := newToolService(t)
	_, _ = tool.Execute(ctxWithSession(), map[string]any{"action": "add", "message": "m", "every_seconds": 30})
	id := svc.ListJobs(false)[0].ID
	out, _ := tool.Execute(ctxWithSession(), map[string]any{
		"action": "update", "job_id": id, "wake_mode": "asap",
	})
	if !strings.Contains(out, "unknown wake_mode") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_EnableDisable(t *testing.T) {
	tool, svc := newToolService(t)
	_, _ = tool.Execute(ctxWithSession(), map[string]any{"action": "add", "message": "m", "every_seconds": 30})
	id := svc.ListJobs(false)[0].ID
	out, _ := tool.Execute(ctxWithSession(), map[string]any{"action": "disable", "job_id": id})
	if !strings.HasPrefix(out, "Disabled job") {
		t.Fatalf("got %q", out)
	}
	if svc.GetJob(id).Enabled {
		t.Fatal("still enabled")
	}
	out, _ = tool.Execute(ctxWithSession(), map[string]any{"action": "enable", "job_id": id})
	if !strings.HasPrefix(out, "Enabled job") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_UnknownAction(t *testing.T) {
	tool, _ := newToolService(t)
	out, _ := tool.Execute(ctxWithSession(), map[string]any{"action": "weird"})
	if !strings.HasPrefix(out, "Unknown action") {
		t.Fatalf("got %q", out)
	}
}

func TestTool_ExecuteWithContextBindsSession(t *testing.T) {
	tool, _ := newToolService(t)
	tctx := &Tool{} // unused, just for the tools.ToolContext shape
	_ = tctx
	out, _ := tool.ExecuteWithContext(context.Background(), nil, map[string]any{
		"action": "add", "message": "m", "every_seconds": 10,
	})
	if !strings.Contains(out, "no session context") {
		t.Fatalf("nil tctx should fall through: %q", out)
	}
}
