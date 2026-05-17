// Package cron contains the CronTool, the JSON-backed scheduler service,
// and the supporting types.
//
// Ported from exoclaw-plugins/packages/exoclaw-tools-cron.
package cron

// Ported from exoclaw_tools_cron/types.py.

// ScheduleKind is the discriminator for CronSchedule.
type ScheduleKind string

const (
	// ScheduleAt fires once at AtMS (Unix ms timestamp).
	ScheduleAt ScheduleKind = "at"
	// ScheduleEvery fires repeatedly with EveryMS between runs.
	ScheduleEvery ScheduleKind = "every"
	// ScheduleCron fires according to a 5-field cron expression.
	ScheduleCron ScheduleKind = "cron"
)

// CronSchedule is the schedule definition for a cron job.
type CronSchedule struct {
	Kind ScheduleKind `json:"kind"`
	// AtMS, for ScheduleAt: Unix-epoch timestamp in milliseconds.
	AtMS int64 `json:"at_ms,omitempty"`
	// EveryMS, for ScheduleEvery: interval between runs in milliseconds.
	EveryMS int64 `json:"every_ms,omitempty"`
	// Expr, for ScheduleCron: 5-field cron expression (e.g. "0 9 * * *").
	Expr string `json:"expr,omitempty"`
	// TZ is the timezone for cron expressions.
	TZ string `json:"tz,omitempty"`
}

// PayloadKind discriminates the firing semantics.
type PayloadKind string

const (
	// PayloadSystemEvent is a fire-and-forget system trigger.
	PayloadSystemEvent PayloadKind = "system_event"
	// PayloadAgentTurn is a regular agent turn — Message becomes the
	// inbound user content.
	PayloadAgentTurn PayloadKind = "agent_turn"
)

// CronPayload describes what to do when the job runs.
type CronPayload struct {
	Kind     PayloadKind `json:"kind"`
	Message  string      `json:"message"`
	// Deliver controls whether the response is sent back to the
	// originating channel/recipient.
	Deliver bool   `json:"deliver"`
	Channel string `json:"channel,omitempty"` // e.g. "whatsapp"
	To      string `json:"to,omitempty"`      // e.g. phone number
	// Skills to load into context when this job runs.
	Skills []string `json:"skills,omitempty"`
	// Stateless: run without session history. Default false — stateful.
	Stateless bool `json:"stateless,omitempty"`
	// Model overrides the agent's default model for this turn.
	Model string `json:"model,omitempty"`
}

// JobStatus is the recorded outcome of the previous run.
type JobStatus string

const (
	JobStatusOK      JobStatus = "ok"
	JobStatusError   JobStatus = "error"
	JobStatusSkipped JobStatus = "skipped"
)

// CronJobState is the runtime state of a job.
type CronJobState struct {
	NextRunAtMS int64     `json:"next_run_at_ms,omitempty"`
	LastRunAtMS int64     `json:"last_run_at_ms,omitempty"`
	LastStatus  JobStatus `json:"last_status,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
}

// WakeMode controls how a due job is delivered.
type WakeMode string

const (
	// WakeNow fires immediately at the scheduled time. Each firing
	// wakes the agent / chip independently.
	WakeNow WakeMode = "now"
	// WakeNextHeartbeat defers the firing until the next heartbeat
	// tick. The cron service queues the job, advances its schedule
	// as if it had run, but holds back the OnJob callback until
	// FlushDeferred is called. Saves battery on a deep-sleep chip
	// and folds non-urgent notifications into one bundle.
	WakeNextHeartbeat WakeMode = "next-heartbeat"
)

// CronJob is a scheduled job.
type CronJob struct {
	ID             string        `json:"id"`
	Name           string        `json:"name"`
	Enabled        bool          `json:"enabled"`
	Schedule       *CronSchedule `json:"schedule,omitempty"`
	Payload        *CronPayload  `json:"payload,omitempty"`
	State          *CronJobState `json:"state,omitempty"`
	CreatedAtMS    int64         `json:"created_at_ms,omitempty"`
	UpdatedAtMS    int64         `json:"updated_at_ms,omitempty"`
	DeleteAfterRun bool          `json:"delete_after_run,omitempty"`
	WakeMode       WakeMode      `json:"wake_mode,omitempty"`
}

// NewCronJob constructs a CronJob with sensible defaults for nested
// structs (matches the Python `field=...` defaults).
func NewCronJob(id, name string) *CronJob {
	return &CronJob{
		ID:       id,
		Name:     name,
		Enabled:  true,
		Schedule: &CronSchedule{Kind: ScheduleEvery},
		Payload:  &CronPayload{Kind: PayloadAgentTurn},
		State:    &CronJobState{},
		WakeMode: WakeNow,
	}
}

// CronStorePayload is the persistent on-disk shape: a version field plus
// the slice of jobs.
type CronStorePayload struct {
	Version int        `json:"version"`
	Jobs    []*CronJob `json:"jobs"`
}

// NewCronStorePayload returns a fresh, empty version-1 store.
func NewCronStorePayload() *CronStorePayload {
	return &CronStorePayload{Version: 1, Jobs: []*CronJob{}}
}
