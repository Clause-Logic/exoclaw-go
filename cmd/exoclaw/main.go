// Command exoclaw runs a minimal end-to-end agent: stdin/stdout REPL,
// OpenAI-protocol provider (works against OpenAI, Ollama, OpenRouter, any
// OpenAI-compatible endpoint), a file-backed conversation store, the
// workspace tools (read_file, write_file, edit_file, list_dir, exec,
// web_search, web_fetch), and a cron scheduler.
//
// Usage:
//
//	OPENAI_API_KEY=sk-... go run ./cmd/exoclaw
//
// Optional env:
//
//	OPENAI_BASE_URL     default https://api.openai.com/v1
//	OPENAI_MODEL        default gpt-4o-mini
//	EXOCLAW_WORKSPACE   default ~/.exoclaw
//	BRAVE_API_KEY       enables web_search via Brave Search
//	CRON_HEARTBEAT_MS   when > 0, coalesce next-heartbeat jobs at this interval (ms)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	stdinch "github.com/Clause-Logic/exoclaw-go/channels/stdin"
	conv "github.com/Clause-Logic/exoclaw-go/conversation-file"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/agent/tools"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/app"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/bus"
	"github.com/Clause-Logic/exoclaw-go/exoclaw/channels"
	openaiprov "github.com/Clause-Logic/exoclaw-go/providers/openai"
	cron "github.com/Clause-Logic/exoclaw-go/tools/cron"
	workspace "github.com/Clause-Logic/exoclaw-go/tools/workspace"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "exoclaw:", err)
		os.Exit(1)
	}
}

func run() error {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if apiKey == "" {
		return fmt.Errorf("OPENAI_API_KEY is required")
	}
	baseURL := envOr("OPENAI_BASE_URL", "https://api.openai.com/v1")
	model := envOr("OPENAI_MODEL", "gpt-4o-mini")

	// Note: don't shadow the imported `workspace` package name.
	workspaceDir := envOr("EXOCLAW_WORKSPACE", "")
	if workspaceDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("resolve home: %w", err)
		}
		workspaceDir = filepath.Join(home, ".exoclaw")
	}
	if err := os.MkdirAll(workspaceDir, 0o755); err != nil {
		return err
	}

	// Quieter default logger — INFO-level for the agent loop is noisy
	// for an interactive REPL. The user-facing chat is on stdout; logs
	// go to stderr at WARN+.
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	provider, err := openaiprov.NewStreamingProvider(openaiprov.Options{
		DefaultModel: model,
		Deployments: map[string]openaiprov.Deployment{
			model: {BaseURL: baseURL, APIKey: apiKey},
		},
		Log: log,
	})
	if err != nil {
		return fmt.Errorf("provider: %w", err)
	}
	defer provider.Close()

	// Pre-register the workspace + cron skills so the agent's system
	// prompt describes the tools without the user having to /load_skill
	// them manually.
	wsSkill := workspace.Workspace()
	cronSkill := cron.Cron()

	// One shared bus so the cron service can fire jobs as inbound
	// messages and the channel manager will route the response back.
	sharedBus := bus.NewMessageBus()

	conversation, err := conv.Create(workspaceDir, provider, model, conv.CreateOptions{
		Log: log,
		Bus: sharedBus,
		SkillPackages: map[string]string{
			wsSkill["name"]:   wsSkill["content"],
			cronSkill["name"]: cronSkill["content"],
		},
	})
	if err != nil {
		return fmt.Errorf("conversation: %w", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Cron service fires due jobs by republishing them as inbound
	// messages. The agent loop processes them through the normal turn
	// path. The CronTool itself is registered alongside workspace tools
	// so the model can add/list/remove jobs.
	cronStore := filepath.Join(workspaceDir, "cron.json")
	cronSvc := cron.NewService(cronStore, makeCronCallback(sharedBus))
	cronSvc.Log = log
	cronSvc.HeartbeatInterval = parseDurationMS(os.Getenv("CRON_HEARTBEAT_MS"))
	if err := cronSvc.Start(ctx); err != nil {
		return fmt.Errorf("cron: %w", err)
	}
	defer cronSvc.Stop()
	cronBackend := cron.NewLocalBackend(cronSvc)

	workspaceTools := buildWorkspaceTools(workspaceDir)
	cronTool := cron.NewTool(cronBackend)
	allTools := append(workspaceTools, cronTool)

	cli := stdinch.New()

	fmt.Fprintln(os.Stderr, "exoclaw — interactive REPL")
	fmt.Fprintln(os.Stderr, "  workspace:", workspaceDir)
	fmt.Fprintln(os.Stderr, "  model:    ", model)
	fmt.Fprintln(os.Stderr, "  tools:    ", toolNames(allTools))
	fmt.Fprintln(os.Stderr, "  /help for commands, ctrl-d to exit")
	fmt.Fprintln(os.Stderr)

	exoclaw := app.New(app.Options{
		Provider:     provider,
		Conversation: conversation,
		Bus:          sharedBus,
		Channels:     []channels.Channel{cli},
		Tools:        allTools,
		Model:        model,
		Log:          log,
	})

	return exoclaw.Run(ctx)
}

// makeCronCallback turns a cron firing into an inbound bus message so the
// agent loop processes it through the normal turn path. The job's
// channel + recipient become the inbound message's Channel + ChatID;
// the message body is the payload text the user wrote when scheduling.
//
// Errors from PublishInbound are returned to the cron service so it
// records the failure on Job.State.LastError; the schedule still advances.
func makeCronCallback(b bus.Bus) cron.JobCallback {
	return func(ctx context.Context, job *cron.CronJob) error {
		channel := job.Payload.Channel
		chatID := job.Payload.To
		if channel == "" {
			channel = "cli"
		}
		if chatID == "" {
			chatID = "direct"
		}
		msg := bus.NewInboundMessage(channel, "cron:"+job.ID, chatID, job.Payload.Message)
		if msg.Metadata == nil {
			msg.Metadata = map[string]any{}
		}
		msg.Metadata["_cron_job_id"] = job.ID
		msg.Metadata["_cron_wake_mode"] = string(job.WakeMode)
		return b.PublishInbound(ctx, msg)
	}
}

// buildWorkspaceTools wires the file/shell/web tools, scoping every one
// to the agent's workspace directory so a misbehaving model can't reach
// outside it. Web tools have no path scope and are added unconditionally;
// web_search returns a "no API key configured" error message at call time
// if BRAVE_API_KEY isn't set, which the agent can read and recover from.
func buildWorkspaceTools(workspaceDir string) []tools.Tool {
	read := workspace.NewReadFileTool(workspaceDir, "")
	write := workspace.NewWriteFileTool(workspaceDir, "")
	edit := workspace.NewEditFileTool(workspaceDir, "")
	list := workspace.NewListDirTool(workspaceDir, "")

	exec, err := workspace.NewExecTool(workspace.ExecOptions{
		Timeout:             30 * time.Second,
		WorkingDir:          workspaceDir,
		RestrictToWorkspace: true,
	})
	if err != nil {
		// The deny-pattern compile is the only failure path; the
		// defaults always compile.
		panic(err)
	}

	search := workspace.NewWebSearchTool(workspace.WebSearchOptions{})
	fetch := workspace.NewWebFetchTool(workspace.WebFetchOptions{})

	return []tools.Tool{read, write, edit, list, exec, search, fetch}
}

func toolNames(ts []tools.Tool) []string {
	names := make([]string, 0, len(ts))
	for _, t := range ts {
		names = append(names, t.Name())
	}
	return names
}

func parseDurationMS(s string) time.Duration {
	if s == "" {
		return 0
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Millisecond
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
