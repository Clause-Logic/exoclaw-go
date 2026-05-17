package cron

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// Ported from tests/test_cron.py — covers computeNextRun,
// validateScheduleForAdd, store load/save, public API, deferred flushing,
// and heartbeat coalescing.

// ─────────────────────────────────────────────────────────────────────
// computeNextRun
// ─────────────────────────────────────────────────────────────────────

func TestComputeNextRun_AtFutureReturnsAt(t *testing.T) {
	svc := NewService(filepath.Join(t.TempDir(), "c.json"), nil)
	now := int64(1_000_000)
	got := svc.computeNextRun(&CronSchedule{Kind: ScheduleAt, AtMS: now + 10_000}, now)
	if got != now+10_000 {
		t.Fatalf("got %d", got)
	}
}

func TestComputeNextRun_AtPastReturnsZero(t *testing.T) {
	svc := NewService(filepath.Join(t.TempDir(), "c.json"), nil)
	now := int64(1_000_000)
	got := svc.computeNextRun(&CronSchedule{Kind: ScheduleAt, AtMS: now - 5_000}, now)
	if got != 0 {
		t.Fatalf("got %d", got)
	}
}

func TestComputeNextRun_EveryAddsInterval(t *testing.T) {
	svc := NewService(filepath.Join(t.TempDir(), "c.json"), nil)
	now := int64(1_000_000)
	got := svc.computeNextRun(&CronSchedule{Kind: ScheduleEvery, EveryMS: 60_000}, now)
	if got != now+60_000 {
		t.Fatalf("got %d", got)
	}
}

func TestComputeNextRun_EveryWithZeroIntervalReturnsZero(t *testing.T) {
	svc := NewService(filepath.Join(t.TempDir(), "c.json"), nil)
	got := svc.computeNextRun(&CronSchedule{Kind: ScheduleEvery}, 1_000_000)
	if got != 0 {
		t.Fatalf("got %d", got)
	}
}

func TestComputeNextRun_CronWithoutEvaluator(t *testing.T) {
	svc := NewService(filepath.Join(t.TempDir(), "c.json"), nil)
	got := svc.computeNextRun(&CronSchedule{Kind: ScheduleCron, Expr: "0 9 * * *"}, 1_000_000)
	if got != 0 {
		t.Fatal("expected 0 without CronExprNext")
	}
}

func TestComputeNextRun_CronWithEvaluator(t *testing.T) {
	svc := NewService(filepath.Join(t.TempDir(), "c.json"), nil)
	svc.CronExprNext = func(expr, tz string, nowMS int64) int64 { return nowMS + 12345 }
	got := svc.computeNextRun(&CronSchedule{Kind: ScheduleCron, Expr: "any"}, 1_000_000)
	if got != 1_012_345 {
		t.Fatalf("got %d", got)
	}
}

// ─────────────────────────────────────────────────────────────────────
// validateScheduleForAdd
// ─────────────────────────────────────────────────────────────────────

func TestValidate_TZOnlyWithCron(t *testing.T) {
	if err := validateScheduleForAdd(&CronSchedule{Kind: ScheduleEvery, EveryMS: 1000, TZ: "UTC"}); err == nil {
		t.Fatal("expected error")
	}
	if err := validateScheduleForAdd(&CronSchedule{Kind: ScheduleCron, Expr: "0 9 * * *", TZ: "UTC"}); err != nil {
		t.Fatalf("UTC should be valid: %v", err)
	}
}

func TestValidate_UnknownTimezoneRejected(t *testing.T) {
	if err := validateScheduleForAdd(&CronSchedule{Kind: ScheduleCron, Expr: "0 9 * * *", TZ: "Atlantis/Lost"}); err == nil {
		t.Fatal("expected error")
	}
}

// ─────────────────────────────────────────────────────────────────────
// Store load / save round-trip
// ─────────────────────────────────────────────────────────────────────

func newService(t *testing.T) *Service {
	t.Helper()
	return NewService(filepath.Join(t.TempDir(), "cron.json"), nil)
}

func TestStore_LoadMissingReturnsEmpty(t *testing.T) {
	svc := newService(t)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	svc.loadStoreLocked()
	if len(svc.store.Jobs) != 0 {
		t.Fatalf("expected empty, got %d", len(svc.store.Jobs))
	}
}

func TestStore_RoundTripPreservesJob(t *testing.T) {
	svc := newService(t)
	if _, err := svc.AddJob("ping", &CronSchedule{Kind: ScheduleEvery, EveryMS: 60_000}, "ping body", AddOptions{
		Channel: "cli", To: "direct",
	}); err != nil {
		t.Fatal(err)
	}

	// Fresh service against the same store path — verifies persistence.
	svc2 := NewService(svc.StorePath, nil)
	jobs := svc2.ListJobs(false)
	if len(jobs) != 1 || jobs[0].Name != "ping" {
		t.Fatalf("got %+v", jobs)
	}
}

func TestStore_LoadCoercesUnknownWakeMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cron.json")
	body := `{"version":1,"jobs":[{"id":"x","name":"n","enabled":true,"schedule":{"kind":"every","every_ms":1000},"wake_mode":"weird"}]}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	svc := NewService(path, nil)
	jobs := svc.ListJobs(true)
	if len(jobs) != 1 || jobs[0].WakeMode != WakeNow {
		t.Fatalf("got %+v", jobs)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────

func TestAdd_ListSortedByNextRun(t *testing.T) {
	svc := newService(t)
	soon, err := svc.AddJob("a", &CronSchedule{Kind: ScheduleEvery, EveryMS: 1000}, "msg", AddOptions{})
	if err != nil {
		t.Fatal(err)
	}
	late, err := svc.AddJob("b", &CronSchedule{Kind: ScheduleEvery, EveryMS: 60_000}, "msg", AddOptions{})
	if err != nil {
		t.Fatal(err)
	}
	jobs := svc.ListJobs(false)
	if len(jobs) != 2 || jobs[0].ID != soon.ID || jobs[1].ID != late.ID {
		t.Fatalf("got %v / %v", jobs[0].ID, jobs[1].ID)
	}
}

func TestRemove_DropsJob(t *testing.T) {
	svc := newService(t)
	job, _ := svc.AddJob("a", &CronSchedule{Kind: ScheduleEvery, EveryMS: 1000}, "m", AddOptions{})
	ok, err := svc.RemoveJob(job.ID)
	if err != nil || !ok {
		t.Fatalf("remove: %v %v", ok, err)
	}
	if got := svc.ListJobs(false); len(got) != 0 {
		t.Fatalf("still present: %v", got)
	}
}

func TestRemove_MissingReturnsFalse(t *testing.T) {
	svc := newService(t)
	ok, _ := svc.RemoveJob("nope")
	if ok {
		t.Fatal("expected false")
	}
}

func TestEnable_TogglesAndRecomputes(t *testing.T) {
	svc := newService(t)
	job, _ := svc.AddJob("a", &CronSchedule{Kind: ScheduleEvery, EveryMS: 1000}, "m", AddOptions{})
	if _, err := svc.EnableJob(job.ID, false); err != nil {
		t.Fatal(err)
	}
	got := svc.GetJob(job.ID)
	if got.Enabled || got.State.NextRunAtMS != 0 {
		t.Fatalf("disable failed: %+v", got)
	}
	if _, err := svc.EnableJob(job.ID, true); err != nil {
		t.Fatal(err)
	}
	got = svc.GetJob(job.ID)
	if !got.Enabled || got.State.NextRunAtMS == 0 {
		t.Fatalf("re-enable failed: %+v", got)
	}
}

func TestUpdate_RenamesAndReschedules(t *testing.T) {
	svc := newService(t)
	job, _ := svc.AddJob("orig", &CronSchedule{Kind: ScheduleEvery, EveryMS: 1000}, "m", AddOptions{})
	newMsg := "this is the new reminder message that exceeds the 30 char name cap"
	updated, err := svc.UpdateJob(job.ID, UpdateOptions{
		Message:  &newMsg,
		Schedule: &CronSchedule{Kind: ScheduleEvery, EveryMS: 5000},
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated == nil || updated.Payload.Message != newMsg {
		t.Fatalf("payload not updated: %+v", updated)
	}
	if len(updated.Name) > 30 {
		t.Fatalf("name not capped: %q", updated.Name)
	}
	if updated.Schedule.EveryMS != 5000 {
		t.Fatalf("schedule not updated: %+v", updated.Schedule)
	}
}

func TestUpdate_MissingReturnsNil(t *testing.T) {
	svc := newService(t)
	got, err := svc.UpdateJob("nope", UpdateOptions{})
	if err != nil || got != nil {
		t.Fatalf("got %v %v", got, err)
	}
}

// ─────────────────────────────────────────────────────────────────────
// Deferred / heartbeat
// ─────────────────────────────────────────────────────────────────────

func TestFlushDeferred_FiresDeferredJobs(t *testing.T) {
	var fires atomic.Int64
	cb := func(_ context.Context, _ *CronJob) error {
		fires.Add(1)
		return nil
	}
	svc := NewService(filepath.Join(t.TempDir(), "c.json"), cb)
	job, _ := svc.AddJob("hb", &CronSchedule{Kind: ScheduleEvery, EveryMS: 1000}, "m", AddOptions{
		WakeMode: WakeNextHeartbeat,
	})
	// Simulate the timer firing into a deferred queue.
	svc.mu.Lock()
	svc.deferJobLocked(job)
	svc.mu.Unlock()
	n, err := svc.FlushDeferred(context.Background())
	if err != nil || n != 1 {
		t.Fatalf("flush: %v %v", n, err)
	}
	if fires.Load() != 1 {
		t.Fatalf("callback fires: %d", fires.Load())
	}
	// Empty queue → flush is a no-op.
	n2, _ := svc.FlushDeferred(context.Background())
	if n2 != 0 {
		t.Fatalf("second flush returned %d", n2)
	}
}

func TestFlushDeferred_OneShotAtIsRemovedAfterFiring(t *testing.T) {
	svc := NewService(filepath.Join(t.TempDir(), "c.json"), func(_ context.Context, _ *CronJob) error { return nil })
	now := nowMS()
	job, _ := svc.AddJob("once", &CronSchedule{Kind: ScheduleAt, AtMS: now + 60_000}, "m", AddOptions{
		WakeMode:       WakeNextHeartbeat,
		DeleteAfterRun: true,
	})
	svc.mu.Lock()
	svc.deferJobLocked(job)
	svc.mu.Unlock()
	_, _ = svc.FlushDeferred(context.Background())
	if svc.GetJob(job.ID) != nil {
		t.Fatal("one-shot at + delete_after_run should be removed")
	}
}

func TestExecuteJob_AdvancesEveryNextRun(t *testing.T) {
	svc := NewService(filepath.Join(t.TempDir(), "c.json"), func(_ context.Context, _ *CronJob) error { return nil })
	job, _ := svc.AddJob("ev", &CronSchedule{Kind: ScheduleEvery, EveryMS: 1000}, "m", AddOptions{})
	startedAt := nowMS()
	svc.mu.Lock()
	svc.executeJobLocked(context.Background(), job)
	svc.mu.Unlock()
	if job.State.NextRunAtMS < startedAt+1000 {
		t.Fatalf("next run not advanced: started=%d next=%d", startedAt, job.State.NextRunAtMS)
	}
	if job.State.LastStatus != JobStatusOK {
		t.Fatalf("status: %v", job.State.LastStatus)
	}
}

func TestExecuteJob_CallbackErrorRecorded(t *testing.T) {
	boom := errors.New("kaboom")
	svc := NewService(filepath.Join(t.TempDir(), "c.json"), func(_ context.Context, _ *CronJob) error { return boom })
	job, _ := svc.AddJob("e", &CronSchedule{Kind: ScheduleEvery, EveryMS: 1000}, "m", AddOptions{})
	svc.mu.Lock()
	svc.executeJobLocked(context.Background(), job)
	svc.mu.Unlock()
	if job.State.LastStatus != JobStatusError || job.State.LastError != "kaboom" {
		t.Fatalf("err not recorded: %+v", job.State)
	}
}

func TestHeartbeatLoop_FlushesDeferred(t *testing.T) {
	var fires atomic.Int64
	cb := func(_ context.Context, _ *CronJob) error {
		fires.Add(1)
		return nil
	}
	svc := NewService(filepath.Join(t.TempDir(), "c.json"), cb)
	svc.HeartbeatInterval = 30 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer svc.Stop()

	job, _ := svc.AddJob("hb", &CronSchedule{Kind: ScheduleEvery, EveryMS: 60_000}, "m", AddOptions{
		WakeMode: WakeNextHeartbeat,
	})
	svc.mu.Lock()
	svc.deferJobLocked(job)
	svc.mu.Unlock()

	// Wait up to a few heartbeats.
	deadline := time.Now().Add(500 * time.Millisecond)
	for fires.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if fires.Load() == 0 {
		t.Fatal("heartbeat never flushed")
	}
}

// ─────────────────────────────────────────────────────────────────────
// JSON shape round-trip — guards on-disk format
// ─────────────────────────────────────────────────────────────────────

func TestStoreFormat_RoundTripUnmarshals(t *testing.T) {
	svc := newService(t)
	_, err := svc.AddJob("a", &CronSchedule{Kind: ScheduleEvery, EveryMS: 1000}, "msg", AddOptions{
		Channel: "cli", To: "x",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(svc.StorePath)
	if err != nil {
		t.Fatal(err)
	}
	var got CronStorePayload
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("on-disk JSON unparseable: %v", err)
	}
	if got.Version != 1 || len(got.Jobs) != 1 {
		t.Fatalf("got %+v", got)
	}
}
