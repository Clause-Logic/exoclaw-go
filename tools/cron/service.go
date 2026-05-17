package cron

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Ported from exoclaw_tools_cron/service.py.
//
// Two public types:
//
//   - Service — the low-level engine (JSON storage + time.Timer scheduler).
//     Mostly sync, used directly by the timer internals.
//   - LocalBackend — async wrapper that implements the Backend interface.
//     This is what CronTool depends on.

// JobCallback is invoked when a scheduled job becomes due. Returning an
// error records the failure in job.State.LastError; the schedule still
// advances normally.
type JobCallback func(ctx context.Context, job *CronJob) error

// CronExprNext is the pluggable cron-expression evaluator. The default is
// nil — a "cron"-kind schedule with no evaluator wired in returns 0
// (no next run), matching the Python original's behaviour when
// `croniter` isn't installed.
//
// Callers that want cron-expression support can wire a function that
// parses Expr (and optional TZ) and returns the next Unix-ms timestamp
// after nowMS. For example, with github.com/robfig/cron/v3:
//
//	svc.CronExprNext = func(expr, tz string, nowMS int64) int64 {
//	    parser := cron.NewParser(cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow)
//	    sched, err := parser.Parse(expr)
//	    if err != nil { return 0 }
//	    return sched.Next(time.UnixMilli(nowMS)).UnixMilli()
//	}
type CronExprNext func(expr, tz string, nowMS int64) int64

// Service is the cron engine — JSON storage + time.Timer scheduler.
//
// HeartbeatInterval, when > 0, runs an internal periodic tick that
// flushes any jobs queued by WakeMode=WakeNextHeartbeat. Without it,
// deferred jobs accumulate until something else calls FlushDeferred.
type Service struct {
	StorePath         string
	OnJob             JobCallback
	HeartbeatInterval time.Duration
	CronExprNext      CronExprNext
	Log               *slog.Logger

	mu          sync.Mutex
	store       *CronStorePayload
	lastMtime   time.Time
	timer       *time.Timer
	heartbeatStop chan struct{}
	deferred    []*CronJob
	running     bool
}

// NewService constructs a Service with the given JSON store path.
func NewService(storePath string, onJob JobCallback) *Service {
	return &Service{
		StorePath: storePath,
		OnJob:     onJob,
		Log:       slog.Default(),
	}
}

// ─────────────────────────────────────────────────────────────────────
// Lifecycle
// ─────────────────────────────────────────────────────────────────────

// Start loads the store, recomputes next-run times, arms the timer, and
// (if HeartbeatInterval > 0) spawns the heartbeat loop.
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	s.running = true
	s.loadStoreLocked()
	s.recomputeNextRunsLocked()
	if err := s.saveStoreLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.armTimerLocked()
	store := s.store
	heartbeat := s.HeartbeatInterval
	s.mu.Unlock()

	if heartbeat > 0 {
		s.heartbeatStop = make(chan struct{})
		go s.heartbeatLoop(ctx, heartbeat)
	}
	s.Log.Info("cron_started",
		"job.count", len(store.Jobs),
		"heartbeat.interval_ms", heartbeat.Milliseconds(),
	)
	return nil
}

// Stop halts the scheduler. Safe to call multiple times.
func (s *Service) Stop() {
	s.mu.Lock()
	s.running = false
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	hbStop := s.heartbeatStop
	s.heartbeatStop = nil
	s.mu.Unlock()
	if hbStop != nil {
		close(hbStop)
	}
}

func (s *Service) heartbeatLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.heartbeatStop:
			return
		case <-ticker.C:
			s.mu.Lock()
			running := s.running
			s.mu.Unlock()
			if !running {
				return
			}
			fired, err := s.FlushDeferred(ctx)
			if err != nil {
				s.Log.Error("cron_heartbeat_flush_failed", "err", err)
			}
			if fired > 0 {
				s.Log.Info("cron_heartbeat_flush", "deferred.fired", fired)
			}
		}
	}
}

// ─────────────────────────────────────────────────────────────────────
// Storage (mu-guarded)
// ─────────────────────────────────────────────────────────────────────

func (s *Service) loadStoreLocked() {
	if s.store != nil {
		// Cache-invalidation: if the file was modified externally
		// since we last wrote it, drop the cache.
		if info, err := os.Stat(s.StorePath); err == nil {
			if !info.ModTime().Equal(s.lastMtime) {
				s.Log.Info("cron_store_reloaded")
				s.store = nil
			}
		}
	}
	if s.store != nil {
		return
	}

	data, err := os.ReadFile(s.StorePath)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			s.Log.Warn("cron_store_load_failed", "err", err)
		}
		s.store = NewCronStorePayload()
		return
	}
	var payload CronStorePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		s.Log.Warn("cron_store_load_failed", "err", err)
		s.store = NewCronStorePayload()
		return
	}
	if payload.Jobs == nil {
		payload.Jobs = []*CronJob{}
	}
	// Coerce unknown WakeMode values back to WakeNow so a manual JSON
	// edit / a writer from a future format version doesn't silently
	// route a job through the deferred queue.
	for _, j := range payload.Jobs {
		if j.WakeMode != WakeNow && j.WakeMode != WakeNextHeartbeat {
			j.WakeMode = WakeNow
		}
		if j.Schedule == nil {
			j.Schedule = &CronSchedule{Kind: ScheduleEvery}
		}
		if j.Payload == nil {
			j.Payload = &CronPayload{Kind: PayloadAgentTurn}
		}
		if j.State == nil {
			j.State = &CronJobState{}
		}
	}
	s.store = &payload
}

func (s *Service) saveStoreLocked() error {
	if s.store == nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(s.StorePath), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(s.store)
	if err != nil {
		return err
	}
	tmp := s.StorePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.StorePath); err != nil {
		return err
	}
	if info, err := os.Stat(s.StorePath); err == nil {
		s.lastMtime = info.ModTime()
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────
// Scheduling helpers
// ─────────────────────────────────────────────────────────────────────

func nowMS() int64 { return time.Now().UnixMilli() }

// computeNextRun returns the next Unix-ms run time for a schedule, or 0
// if the schedule never fires again.
func (s *Service) computeNextRun(schedule *CronSchedule, now int64) int64 {
	switch schedule.Kind {
	case ScheduleAt:
		if schedule.AtMS > now {
			return schedule.AtMS
		}
		return 0
	case ScheduleEvery:
		if schedule.EveryMS <= 0 {
			return 0
		}
		return now + schedule.EveryMS
	case ScheduleCron:
		if s.CronExprNext == nil || schedule.Expr == "" {
			return 0
		}
		return s.CronExprNext(schedule.Expr, schedule.TZ, now)
	}
	return 0
}

func validateScheduleForAdd(schedule *CronSchedule) error {
	if schedule == nil {
		return errors.New("schedule is required")
	}
	if schedule.TZ != "" && schedule.Kind != ScheduleCron {
		return errors.New("tz can only be used with cron schedules")
	}
	if schedule.Kind == ScheduleCron && schedule.TZ != "" {
		if _, err := time.LoadLocation(schedule.TZ); err != nil {
			return fmt.Errorf("unknown timezone '%s'", schedule.TZ)
		}
	}
	return nil
}

func (s *Service) recomputeNextRunsLocked() {
	if s.store == nil {
		return
	}
	now := nowMS()
	for _, j := range s.store.Jobs {
		if j.Enabled {
			j.State.NextRunAtMS = s.computeNextRun(j.Schedule, now)
		}
	}
}

// getNextWakeMSLocked returns the earliest next-run timestamp across all
// enabled jobs, or 0 if none are scheduled.
func (s *Service) getNextWakeMSLocked() int64 {
	if s.store == nil {
		return 0
	}
	var best int64 = math.MaxInt64
	found := false
	for _, j := range s.store.Jobs {
		if !j.Enabled || j.State.NextRunAtMS == 0 {
			continue
		}
		if j.State.NextRunAtMS < best {
			best = j.State.NextRunAtMS
			found = true
		}
	}
	if !found {
		return 0
	}
	return best
}

// armTimerLocked schedules the next on-timer fire. Caller holds s.mu.
func (s *Service) armTimerLocked() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	if !s.running {
		return
	}
	next := s.getNextWakeMSLocked()
	if next == 0 {
		return
	}
	delay := time.Duration(next-nowMS()) * time.Millisecond
	if delay < 0 {
		delay = 0
	}
	s.timer = time.AfterFunc(delay, func() {
		s.onTimer(context.Background())
	})
}

func (s *Service) onTimer(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return
	}
	s.loadStoreLocked()
	if s.store == nil {
		return
	}
	now := nowMS()
	due := []*CronJob{}
	for _, j := range s.store.Jobs {
		if j.Enabled && j.State.NextRunAtMS > 0 && now >= j.State.NextRunAtMS {
			due = append(due, j)
		}
	}
	for _, job := range due {
		if job.WakeMode == WakeNextHeartbeat {
			s.deferJobLocked(job)
		} else {
			s.executeJobLocked(ctx, job)
		}
	}
	_ = s.saveStoreLocked()
	s.armTimerLocked()
}

func (s *Service) deferJobLocked(job *CronJob) {
	s.deferred = append(s.deferred, job)
	now := nowMS()
	job.State.LastRunAtMS = now
	job.UpdatedAtMS = now
	if job.Schedule.Kind == ScheduleAt {
		job.Enabled = false
		job.State.NextRunAtMS = 0
	} else {
		job.State.NextRunAtMS = s.computeNextRun(job.Schedule, now)
	}
}

// FlushDeferred fires OnJob for every job queued by WakeNextHeartbeat.
// Returns the count fired.
func (s *Service) FlushDeferred(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.deferred) == 0 {
		return 0, nil
	}
	s.loadStoreLocked()
	batch := s.deferred
	s.deferred = nil
	for _, job := range batch {
		start := nowMS()
		if s.OnJob != nil {
			if err := s.OnJob(ctx, job); err != nil {
				job.State.LastStatus = JobStatusError
				job.State.LastError = err.Error()
				s.Log.Error("cron_job_failed", "job.name", job.Name, "err", err)
			} else {
				job.State.LastStatus = JobStatusOK
				job.State.LastError = ""
				s.Log.Info("cron_job_executed",
					"job.name", job.Name,
					"job.id", job.ID,
					"wake_mode", job.WakeMode,
				)
			}
		}
		job.State.LastRunAtMS = start
		job.UpdatedAtMS = nowMS()
		// One-shot at + delete_after_run cleanup.
		if job.Schedule.Kind == ScheduleAt && job.DeleteAfterRun && s.store != nil {
			s.store.Jobs = filterJobs(s.store.Jobs, func(j *CronJob) bool { return j.ID != job.ID })
		}
	}
	if err := s.saveStoreLocked(); err != nil {
		return len(batch), err
	}
	return len(batch), nil
}

func (s *Service) executeJobLocked(ctx context.Context, job *CronJob) {
	start := nowMS()
	if s.OnJob != nil {
		if err := s.OnJob(ctx, job); err != nil {
			job.State.LastStatus = JobStatusError
			job.State.LastError = err.Error()
			s.Log.Error("cron_job_failed", "job.name", job.Name, "err", err)
		} else {
			job.State.LastStatus = JobStatusOK
			job.State.LastError = ""
			s.Log.Info("cron_job_executed", "job.name", job.Name, "job.id", job.ID)
		}
	}
	job.State.LastRunAtMS = start
	job.UpdatedAtMS = nowMS()

	if job.Schedule.Kind == ScheduleAt {
		if job.DeleteAfterRun && s.store != nil {
			s.store.Jobs = filterJobs(s.store.Jobs, func(j *CronJob) bool { return j.ID != job.ID })
		} else {
			job.Enabled = false
			job.State.NextRunAtMS = 0
		}
	} else {
		job.State.NextRunAtMS = s.computeNextRun(job.Schedule, nowMS())
	}
}

func filterJobs(jobs []*CronJob, keep func(*CronJob) bool) []*CronJob {
	out := jobs[:0]
	for _, j := range jobs {
		if keep(j) {
			out = append(out, j)
		}
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────

// ListJobs returns all jobs sorted by NextRunAtMS ascending. Disabled
// jobs are excluded unless includeDisabled is true.
func (s *Service) ListJobs(includeDisabled bool) []*CronJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadStoreLocked()
	var out []*CronJob
	for _, j := range s.store.Jobs {
		if !includeDisabled && !j.Enabled {
			continue
		}
		out = append(out, j)
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i].State.NextRunAtMS, out[j].State.NextRunAtMS
		if a == 0 {
			return false
		}
		if b == 0 {
			return true
		}
		return a < b
	})
	return out
}

// GetJob fetches a job by ID, or nil if absent.
func (s *Service) GetJob(jobID string) *CronJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadStoreLocked()
	for _, j := range s.store.Jobs {
		if j.ID == jobID {
			return j
		}
	}
	return nil
}

// AddJob adds a new agent-turn job.
func (s *Service) AddJob(name string, schedule *CronSchedule, message string, opts AddOptions) (*CronJob, error) {
	if err := validateScheduleForAdd(schedule); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadStoreLocked()
	now := nowMS()
	wakeMode := opts.WakeMode
	if wakeMode == "" {
		wakeMode = WakeNow
	}
	job := &CronJob{
		ID:       shortID(),
		Name:     name,
		Enabled:  true,
		Schedule: schedule,
		Payload: &CronPayload{
			Kind:      PayloadAgentTurn,
			Message:   message,
			Deliver:   opts.Deliver,
			Channel:   opts.Channel,
			To:        opts.To,
			Skills:    opts.Skills,
			Stateless: opts.Stateless,
			Model:     opts.Model,
		},
		State:          &CronJobState{NextRunAtMS: s.computeNextRun(schedule, now)},
		CreatedAtMS:    now,
		UpdatedAtMS:    now,
		DeleteAfterRun: opts.DeleteAfterRun,
		WakeMode:       wakeMode,
	}
	s.store.Jobs = append(s.store.Jobs, job)
	if err := s.saveStoreLocked(); err != nil {
		return nil, err
	}
	s.armTimerLocked()
	s.Log.Info("cron_job_added", "job.name", name, "job.id", job.ID, "wake_mode", wakeMode)
	return job, nil
}

// RemoveJob deletes a job by ID. Returns true if a job was removed.
func (s *Service) RemoveJob(jobID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadStoreLocked()
	before := len(s.store.Jobs)
	s.store.Jobs = filterJobs(s.store.Jobs, func(j *CronJob) bool { return j.ID != jobID })
	removed := len(s.store.Jobs) < before
	if removed {
		if err := s.saveStoreLocked(); err != nil {
			return false, err
		}
		s.armTimerLocked()
		s.Log.Info("cron_job_removed", "job.id", jobID)
	}
	return removed, nil
}

// UpdateJob applies UpdateOptions to an existing job. Returns nil if jobID
// is unknown.
func (s *Service) UpdateJob(jobID string, opts UpdateOptions) (*CronJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadStoreLocked()
	for _, job := range s.store.Jobs {
		if job.ID != jobID {
			continue
		}
		if opts.Message != nil {
			job.Payload.Message = *opts.Message
			n := *opts.Message
			if len(n) > 30 {
				n = n[:30]
			}
			job.Name = n
		}
		if opts.Deliver != nil {
			job.Payload.Deliver = *opts.Deliver
		}
		if opts.Channel != nil {
			job.Payload.Channel = *opts.Channel
		}
		if opts.To != nil {
			job.Payload.To = *opts.To
		}
		if opts.Skills != nil {
			job.Payload.Skills = *opts.Skills
		}
		if opts.Stateless != nil {
			job.Payload.Stateless = *opts.Stateless
		}
		if opts.Model != nil {
			job.Payload.Model = *opts.Model
		}
		if opts.Schedule != nil {
			if err := validateScheduleForAdd(opts.Schedule); err != nil {
				return nil, err
			}
			job.Schedule = opts.Schedule
			job.State.NextRunAtMS = s.computeNextRun(opts.Schedule, nowMS())
		}
		if opts.WakeMode != nil {
			job.WakeMode = *opts.WakeMode
		}
		job.UpdatedAtMS = nowMS()
		if err := s.saveStoreLocked(); err != nil {
			return nil, err
		}
		s.armTimerLocked()
		s.Log.Info("cron_job_updated", "job.name", job.Name, "job.id", job.ID)
		return job, nil
	}
	return nil, nil
}

// EnableJob enables or disables a job by ID. Returns nil if jobID is
// unknown.
func (s *Service) EnableJob(jobID string, enabled bool) (*CronJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadStoreLocked()
	for _, job := range s.store.Jobs {
		if job.ID != jobID {
			continue
		}
		job.Enabled = enabled
		job.UpdatedAtMS = nowMS()
		if enabled {
			job.State.NextRunAtMS = s.computeNextRun(job.Schedule, nowMS())
		} else {
			job.State.NextRunAtMS = 0
		}
		if err := s.saveStoreLocked(); err != nil {
			return nil, err
		}
		s.armTimerLocked()
		return job, nil
	}
	return nil, nil
}

// RunJob manually fires a job by ID. force=true overrides the enabled
// check. Returns true if the job was found and run.
func (s *Service) RunJob(ctx context.Context, jobID string, force bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadStoreLocked()
	for _, job := range s.store.Jobs {
		if job.ID != jobID {
			continue
		}
		if !force && !job.Enabled {
			return false, nil
		}
		s.executeJobLocked(ctx, job)
		if err := s.saveStoreLocked(); err != nil {
			return true, err
		}
		s.armTimerLocked()
		return true, nil
	}
	return false, nil
}

// Status returns a snapshot of service state.
func (s *Service) Status() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadStoreLocked()
	return map[string]any{
		"enabled":         s.running,
		"jobs":            len(s.store.Jobs),
		"next_wake_at_ms": s.getNextWakeMSLocked(),
	}
}

// ─────────────────────────────────────────────────────────────────────
// LocalBackend — Backend interface adapter
// ─────────────────────────────────────────────────────────────────────

// LocalBackend wraps Service as a Backend so CronTool can depend on the
// protocol without caring whether the backend is local or remote.
type LocalBackend struct {
	Svc *Service
}

// NewLocalBackend wraps svc.
func NewLocalBackend(svc *Service) *LocalBackend { return &LocalBackend{Svc: svc} }

func (b *LocalBackend) Add(_ context.Context, name string, schedule *CronSchedule, message string, opts AddOptions) (*CronJob, error) {
	return b.Svc.AddJob(name, schedule, message, opts)
}

func (b *LocalBackend) ListJobs(_ context.Context, includeDisabled bool) ([]*CronJob, error) {
	return b.Svc.ListJobs(includeDisabled), nil
}

func (b *LocalBackend) Get(_ context.Context, jobID string) (*CronJob, error) {
	return b.Svc.GetJob(jobID), nil
}

func (b *LocalBackend) Update(_ context.Context, jobID string, opts UpdateOptions) (*CronJob, error) {
	return b.Svc.UpdateJob(jobID, opts)
}

func (b *LocalBackend) Remove(_ context.Context, jobID string) (bool, error) {
	return b.Svc.RemoveJob(jobID)
}

func (b *LocalBackend) Enable(_ context.Context, jobID string, enabled bool) (*CronJob, error) {
	return b.Svc.EnableJob(jobID, enabled)
}

// FlushDeferred forwards to Service.FlushDeferred so callers without
// direct Service access can drive the heartbeat from outside.
func (b *LocalBackend) FlushDeferred(ctx context.Context) (int, error) {
	return b.Svc.FlushDeferred(ctx)
}

// Compile-time conformance.
var _ Backend = (*LocalBackend)(nil)

// ─────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────

// shortID returns an 8-char hex id. crypto/rand for safety; collisions in
// the cron-job namespace are essentially impossible at this width and
// even one collision wouldn't be catastrophic (jobs would clobber on
// save, recoverable by editing the JSON).
func shortID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
