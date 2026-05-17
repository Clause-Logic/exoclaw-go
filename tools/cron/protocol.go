package cron

import "context"

// Ported from exoclaw_tools_cron/protocol.py.
//
// CronBackend is the execution-engine seam for cron scheduling. Implementations
// handle storage, scheduling, and job lifecycle. The CronTool delegates all
// persistence and scheduling concerns through this interface, allowing
// alternative backends (e.g. Temporal Schedules) to reuse the full plugin
// feature set without reimplementing the tool.

// AddOptions bundles the optional kwargs of Backend.Add.
type AddOptions struct {
	Deliver        bool
	Channel        string
	To             string
	DeleteAfterRun bool
	Skills         []string
	Stateless      bool
	Model          string
	WakeMode       WakeMode // default WakeNow
	// Extra is an opaque per-deployment bag that backends pass through
	// to the workflow-input adapter. Used for structured context that
	// shouldn't live in the cron message text (topic ids, briefs, etc).
	Extra map[string]any
}

// UpdateOptions bundles the optional kwargs of Backend.Update.
//
// Pointer fields encode "unset = leave the existing value" (matches the
// Python `None`-default convention). Caller sets only the fields they
// want to change.
type UpdateOptions struct {
	Message   *string
	Schedule  *CronSchedule
	Deliver   *bool
	Channel   *string
	To        *string
	Skills    *[]string
	Stateless *bool
	Model     *string
	WakeMode  *WakeMode
}

// Backend is the execution engine for scheduled jobs.
//
// Renamed from CronBackend (the Python class name) to Backend so callers
// can write `cron.Backend` without the package-name stutter.
type Backend interface {
	Add(ctx context.Context, name string, schedule *CronSchedule, message string, opts AddOptions) (*CronJob, error)

	ListJobs(ctx context.Context, includeDisabled bool) ([]*CronJob, error)

	Get(ctx context.Context, jobID string) (*CronJob, error)

	Update(ctx context.Context, jobID string, opts UpdateOptions) (*CronJob, error)

	Remove(ctx context.Context, jobID string) (bool, error)

	Enable(ctx context.Context, jobID string, enabled bool) (*CronJob, error)
}
