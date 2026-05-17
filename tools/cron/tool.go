package cron

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/standd/exoclaw-go/exoclaw/agent/tools"
)

// Ported from exoclaw_tools_cron/tool.py.
//
// The Python original uses ContextVars to keep per-call routing state
// (channel, chat_id, in_cron_context) goroutine-safe. In Go we use
// context.Context values keyed by typed keys — same isolation, native
// shape.

type ctxKey int

const (
	keyChannel ctxKey = iota
	keyChatID
	keyInCron
)

// WithChannel binds the per-call delivery channel onto ctx.
func WithChannel(ctx context.Context, channel string) context.Context {
	return context.WithValue(ctx, keyChannel, channel)
}

// WithChatID binds the per-call delivery chat ID onto ctx.
func WithChatID(ctx context.Context, chatID string) context.Context {
	return context.WithValue(ctx, keyChatID, chatID)
}

// WithInCronContext marks ctx as executing inside a cron job callback.
// Used so the CronTool refuses to schedule new jobs from inside a cron
// firing (would otherwise create an infinite-loop hazard).
func WithInCronContext(ctx context.Context, active bool) context.Context {
	return context.WithValue(ctx, keyInCron, active)
}

func channelFrom(ctx context.Context) string {
	v, _ := ctx.Value(keyChannel).(string)
	return v
}

func chatIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(keyChatID).(string)
	return v
}

func inCronFrom(ctx context.Context) bool {
	v, _ := ctx.Value(keyInCron).(bool)
	return v
}

// Tool is the cron-tool — schedule reminders and recurring tasks.
type Tool struct {
	tools.ToolBase
	Backend Backend
}

// NewTool constructs a Tool backed by the given Backend.
func NewTool(backend Backend) *Tool {
	t := &Tool{Backend: backend}
	t.NameField = "cron"
	t.DescriptionField = "Schedule reminders and recurring tasks. Actions: add, list, remove, update, enable, disable."
	t.ParametersField = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"action": map[string]any{
				"type":        "string",
				"enum":        []any{"add", "list", "remove", "update", "enable", "disable"},
				"description": "Action to perform",
			},
			"message":       map[string]any{"type": "string", "description": "Reminder message (for add)"},
			"every_seconds": map[string]any{"type": "integer", "description": "Interval in seconds (for recurring tasks)"},
			"cron_expr":     map[string]any{"type": "string", "description": "Cron expression like '0 9 * * *' (for scheduled tasks)"},
			"tz":            map[string]any{"type": "string", "description": "IANA timezone for cron expressions (e.g. 'America/Vancouver')"},
			"at":            map[string]any{"type": "string", "description": "ISO datetime for one-time execution (e.g. '2026-02-12T10:30:00')"},
			"job_id":        map[string]any{"type": "string", "description": "Job ID (for remove/update)"},
			"deliver":       map[string]any{"type": "boolean", "description": "Whether to deliver the response to the user (for update)"},
			"to":            map[string]any{"type": "string", "description": "Delivery destination for this job (for update)."},
			"skills":        map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Skill names to load into context when this job runs"},
			"stateless":     map[string]any{"type": "boolean", "description": "Run without session history (default false)"},
			"model":         map[string]any{"type": "string", "description": "Override the agent's default model for this job's turn"},
			"wake_mode": map[string]any{
				"type": "string",
				"enum": []any{"now", "next-heartbeat"},
				"description": "When the firing should wake the agent. 'now' (default) fires immediately. " +
					"'next-heartbeat' coalesces the firing into the next heartbeat tick.",
			},
		},
		"required": []any{"action"},
	}
	return t
}

// ExecuteWithContext binds the agent's session ctx (channel, chat_id) onto
// the context that flows into Execute, so the cron-add path picks up the
// caller's session as the delivery destination.
func (t *Tool) ExecuteWithContext(ctx context.Context, tctx *tools.ToolContext, params map[string]any) (string, error) {
	if tctx != nil {
		ctx = WithChannel(ctx, tctx.Channel)
		ctx = WithChatID(ctx, tctx.ChatID)
	}
	return t.Execute(ctx, params)
}

// Execute dispatches the named action.
func (t *Tool) Execute(ctx context.Context, params map[string]any) (string, error) {
	action, _ := params["action"].(string)
	switch action {
	case "add":
		if inCronFrom(ctx) {
			return "Error: cannot schedule new jobs from within a cron job execution", nil
		}
		return t.addJob(ctx, params)
	case "list":
		return t.listJobs(ctx)
	case "remove":
		return t.removeJob(ctx, params)
	case "update":
		return t.updateJob(ctx, params)
	case "enable":
		return t.enableJob(ctx, params, true)
	case "disable":
		return t.enableJob(ctx, params, false)
	}
	return "Unknown action: " + action, nil
}

func (t *Tool) addJob(ctx context.Context, params map[string]any) (string, error) {
	message, _ := params["message"].(string)
	everySeconds, hasEvery := intOptional(params["every_seconds"])
	cronExpr, _ := params["cron_expr"].(string)
	tz, _ := params["tz"].(string)
	atStr, _ := params["at"].(string)
	skills := paramStringSlice(params["skills"])
	stateless, _ := params["stateless"].(bool)
	model, _ := params["model"].(string)
	wakeMode, _ := params["wake_mode"].(string)

	if message == "" {
		return "Error: message is required for add", nil
	}
	channel := channelFrom(ctx)
	chatID := chatIDFrom(ctx)
	if channel == "" || chatID == "" {
		return "Error: no session context (channel/chat_id)", nil
	}
	if tz != "" && cronExpr == "" {
		return "Error: tz can only be used with cron_expr", nil
	}
	if tz != "" {
		if _, err := time.LoadLocation(tz); err != nil {
			return fmt.Sprintf("Error: unknown timezone '%s'", tz), nil
		}
	}

	var schedule *CronSchedule
	deleteAfter := false
	switch {
	case hasEvery && everySeconds > 0:
		schedule = &CronSchedule{Kind: ScheduleEvery, EveryMS: int64(everySeconds) * 1000}
	case cronExpr != "":
		schedule = &CronSchedule{Kind: ScheduleCron, Expr: cronExpr, TZ: tz}
	case atStr != "":
		dt, err := parseISODatetime(atStr)
		if err != nil {
			return fmt.Sprintf("Error: invalid ISO datetime format '%s'. Expected format: YYYY-MM-DDTHH:MM:SS", atStr), nil
		}
		schedule = &CronSchedule{Kind: ScheduleAt, AtMS: dt.UnixMilli()}
		deleteAfter = true
	default:
		return "Error: either every_seconds, cron_expr, or at is required", nil
	}

	wm, err := resolveWakeMode(wakeMode)
	if err != nil {
		return err.Error(), nil
	}

	name := message
	if len(name) > 30 {
		name = name[:30]
	}
	job, err := t.Backend.Add(ctx, name, schedule, message, AddOptions{
		Deliver:        true,
		Channel:        channel,
		To:             chatID,
		DeleteAfterRun: deleteAfter,
		Skills:         skills,
		Stateless:      stateless,
		Model:          model,
		WakeMode:       wm,
	})
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	return fmt.Sprintf("Created job '%s' (id: %s)", job.Name, job.ID), nil
}

func (t *Tool) listJobs(ctx context.Context) (string, error) {
	jobs, err := t.Backend.ListJobs(ctx, false)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	if len(jobs) == 0 {
		return "No scheduled jobs.", nil
	}
	lines := make([]string, 0, len(jobs))
	for _, j := range jobs {
		lines = append(lines, fmt.Sprintf("- %s (id: %s, %s)", j.Name, j.ID, j.Schedule.Kind))
	}
	return "Scheduled jobs:\n" + strings.Join(lines, "\n"), nil
}

func (t *Tool) removeJob(ctx context.Context, params map[string]any) (string, error) {
	jobID, _ := params["job_id"].(string)
	if jobID == "" {
		return "Error: job_id is required for remove", nil
	}
	ok, err := t.Backend.Remove(ctx, jobID)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	if !ok {
		return fmt.Sprintf("Job %s not found", jobID), nil
	}
	return fmt.Sprintf("Removed job %s", jobID), nil
}

func (t *Tool) updateJob(ctx context.Context, params map[string]any) (string, error) {
	jobID, _ := params["job_id"].(string)
	if jobID == "" {
		return "Error: job_id is required for update", nil
	}
	cronExpr, _ := params["cron_expr"].(string)
	tz, _ := params["tz"].(string)
	if tz != "" && cronExpr == "" {
		return "Error: tz can only be used with cron_expr", nil
	}
	var schedule *CronSchedule
	if cronExpr != "" {
		if tz != "" {
			if _, err := time.LoadLocation(tz); err != nil {
				return fmt.Sprintf("Error: unknown timezone '%s'", tz), nil
			}
		}
		schedule = &CronSchedule{Kind: ScheduleCron, Expr: cronExpr, TZ: tz}
	}
	opts := UpdateOptions{Schedule: schedule}
	if message, ok := params["message"].(string); ok && message != "" {
		opts.Message = &message
	}
	if deliver, ok := params["deliver"].(bool); ok {
		opts.Deliver = &deliver
	}
	if to, ok := params["to"].(string); ok && to != "" {
		opts.To = &to
	}
	if skills := paramStringSlice(params["skills"]); skills != nil {
		opts.Skills = &skills
	}
	if stateless, ok := params["stateless"].(bool); ok {
		opts.Stateless = &stateless
	}
	if model, ok := params["model"].(string); ok && model != "" {
		opts.Model = &model
	}
	if wm, ok := params["wake_mode"].(string); ok && wm != "" {
		resolved, err := resolveWakeMode(wm)
		if err != nil {
			return err.Error(), nil
		}
		opts.WakeMode = &resolved
	}

	job, err := t.Backend.Update(ctx, jobID, opts)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	if job == nil {
		return fmt.Sprintf("Job %s not found", jobID), nil
	}
	return fmt.Sprintf("Updated job %s", jobID), nil
}

func (t *Tool) enableJob(ctx context.Context, params map[string]any, enabled bool) (string, error) {
	jobID, _ := params["job_id"].(string)
	if jobID == "" {
		return "Error: job_id is required for enable/disable", nil
	}
	job, err := t.Backend.Enable(ctx, jobID, enabled)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	if job == nil {
		return fmt.Sprintf("Job %s not found", jobID), nil
	}
	state := "Enabled"
	if !enabled {
		state = "Disabled"
	}
	return fmt.Sprintf("%s job %s", state, jobID), nil
}

// ─────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────

func intOptional(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	}
	return 0, false
}

func paramStringSlice(v any) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, item := range x {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func resolveWakeMode(s string) (WakeMode, error) {
	switch s {
	case "", "now":
		return WakeNow, nil
	case "next-heartbeat":
		return WakeNextHeartbeat, nil
	}
	return "", fmt.Errorf("Error: unknown wake_mode '%s' (expected 'now' or 'next-heartbeat')", s)
}

func parseISODatetime(s string) (time.Time, error) {
	// Python's `datetime.fromisoformat` accepts both `YYYY-MM-DDTHH:MM:SS`
	// and a few near-variants. time.Parse with RFC3339 is strict; fall
	// back to a few common shapes.
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
	}
	var lastErr error
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		} else {
			lastErr = err
		}
	}
	return time.Time{}, lastErr
}

// Compile-time conformance.
var (
	_ tools.Tool           = (*Tool)(nil)
	_ tools.ContextualTool = (*Tool)(nil)
)
