package workspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/standd/exoclaw-go/exoclaw/agent/tools"
)

// Ported from exoclaw_tools_workspace/shell.py.
//
// ExecTool runs a shell command and returns the captured output (or, in
// streaming mode, a channel of stdout/stderr chunks).

// DefaultDenyPatterns is the safety-guard regex set blocked when ExecTool
// is constructed without an explicit deny list. Mirrors the Python
// originals.
var DefaultDenyPatterns = []string{
	`\brm\s+-[rf]{1,2}\b`,        // rm -r, rm -rf, rm -fr
	`\bdel\s+/[fq]\b`,            // del /f, del /q
	`\brmdir\s+/s\b`,             // rmdir /s
	`(?:^|[;&|]\s*)format\b`,     // format (as standalone command only)
	`\b(mkfs|diskpart)\b`,        // disk operations
	`\bdd\s+if=`,                 // dd
	`>\s*/dev/sd`,                // write to disk
	`\b(shutdown|reboot|poweroff)\b`,
	`:\(\)\s*\{.*\};\s*:`, // fork bomb
}

const execMaxOutputChars = 10_000

// ExecTool executes shell commands. The safety guard blocks
// configurable deny / allow patterns and optional workspace-only path
// restriction.
type ExecTool struct {
	tools.ToolBase
	Timeout             time.Duration
	WorkingDir          string
	DenyPatterns        []*regexp.Regexp
	AllowPatterns       []*regexp.Regexp
	RestrictToWorkspace bool
	PathAppend          string
}

// ExecOptions bundles the options.
type ExecOptions struct {
	Timeout             time.Duration
	WorkingDir          string
	DenyPatterns        []string
	AllowPatterns       []string
	RestrictToWorkspace bool
	PathAppend          string
}

// NewExecTool constructs an ExecTool. If opts.DenyPatterns is nil the
// DefaultDenyPatterns set is compiled.
func NewExecTool(opts ExecOptions) (*ExecTool, error) {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	denyPatterns := opts.DenyPatterns
	if denyPatterns == nil {
		denyPatterns = DefaultDenyPatterns
	}
	deny := make([]*regexp.Regexp, 0, len(denyPatterns))
	for _, p := range denyPatterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("compile deny pattern %q: %w", p, err)
		}
		deny = append(deny, re)
	}
	allow := make([]*regexp.Regexp, 0, len(opts.AllowPatterns))
	for _, p := range opts.AllowPatterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("compile allow pattern %q: %w", p, err)
		}
		allow = append(allow, re)
	}
	t := &ExecTool{
		Timeout:             timeout,
		WorkingDir:          opts.WorkingDir,
		DenyPatterns:        deny,
		AllowPatterns:       allow,
		RestrictToWorkspace: opts.RestrictToWorkspace,
		PathAppend:          opts.PathAppend,
	}
	t.NameField = "exec"
	t.DescriptionField = "Execute a shell command and return its output. Use with caution."
	t.ParametersField = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"command":     map[string]any{"type": "string", "description": "The shell command to execute"},
			"working_dir": map[string]any{"type": "string", "description": "Optional working directory for the command"},
		},
		"required": []any{"command"},
	}
	return t, nil
}

// Execute runs the command and returns the combined stdout + stderr +
// exit-code summary. Truncated past execMaxOutputChars.
func (t *ExecTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	command, _ := params["command"].(string)
	workingDir, _ := params["working_dir"].(string)
	if workingDir == "" {
		workingDir = t.WorkingDir
	}
	if workingDir == "" {
		var err error
		workingDir, err = os.Getwd()
		if err != nil {
			return "Error executing command: " + err.Error(), nil
		}
	}
	if guard := t.guard(command, workingDir); guard != "" {
		return guard, nil
	}

	runCtx, cancel := context.WithTimeout(ctx, t.Timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, shell(), shellFlag(), command)
	cmd.Dir = workingDir
	cmd.Env = os.Environ()
	if t.PathAppend != "" {
		// Append to PATH so child commands can find binaries the agent
		// installed mid-session.
		newPath := os.Getenv("PATH")
		if newPath != "" {
			newPath += string(os.PathListSeparator)
		}
		newPath += t.PathAppend
		cmd.Env = setEnv(cmd.Env, "PATH", newPath)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
		return fmt.Sprintf("Error: Command timed out after %d seconds", int(t.Timeout.Seconds())), nil
	}

	var parts []string
	if stdout.Len() > 0 {
		parts = append(parts, stdout.String())
	}
	if stderr.Len() > 0 {
		s := strings.TrimSpace(stderr.String())
		if s != "" {
			parts = append(parts, "STDERR:\n"+stderr.String())
		}
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		parts = append(parts, fmt.Sprintf("\nExit code: %d", exitErr.ExitCode()))
	} else if err != nil {
		return "Error executing command: " + err.Error(), nil
	}
	result := strings.Join(parts, "\n")
	if result == "" {
		result = "(no output)"
	}
	if len(result) > execMaxOutputChars {
		result = result[:execMaxOutputChars] + fmt.Sprintf("\n... (truncated, %d more chars)", len(result)-execMaxOutputChars)
	}
	return result, nil
}

// ExecuteStreaming streams subprocess output as it arrives, draining
// stdout and stderr concurrently so a child that floods stderr doesn't
// deadlock on its OS pipe buffer.
//
// Same safety guard, working-dir, env, and deadline semantics as
// Execute — but truncation is dropped (streaming exists to support
// multi-MB output).
func (t *ExecTool) ExecuteStreaming(ctx context.Context, params map[string]any) (<-chan string, error) {
	out := make(chan string)
	command, _ := params["command"].(string)
	workingDir, _ := params["working_dir"].(string)
	if workingDir == "" {
		workingDir = t.WorkingDir
	}
	if workingDir == "" {
		var err error
		workingDir, err = os.Getwd()
		if err != nil {
			go func() {
				out <- "Error executing command: " + err.Error()
				close(out)
			}()
			return out, nil
		}
	}
	if guard := t.guard(command, workingDir); guard != "" {
		go func() {
			out <- guard
			close(out)
		}()
		return out, nil
	}

	runCtx, cancel := context.WithTimeout(ctx, t.Timeout)
	cmd := exec.CommandContext(runCtx, shell(), shellFlag(), command)
	cmd.Dir = workingDir
	cmd.Env = os.Environ()
	if t.PathAppend != "" {
		newPath := os.Getenv("PATH")
		if newPath != "" {
			newPath += string(os.PathListSeparator)
		}
		newPath += t.PathAppend
		cmd.Env = setEnv(cmd.Env, "PATH", newPath)
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		go func() {
			out <- "Error executing command: " + err.Error()
			close(out)
		}()
		return out, nil
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		go func() {
			out <- "Error executing command: " + err.Error()
			close(out)
		}()
		return out, nil
	}
	if err := cmd.Start(); err != nil {
		cancel()
		go func() {
			out <- "Error executing command: " + err.Error()
			close(out)
		}()
		return out, nil
	}

	go func() {
		defer close(out)
		defer cancel()

		// drainedFirstStderr flips true after the first stderr chunk so
		// only the first one gets the "STDERR:\n" prefix. emitMu
		// serialises emission across the two drainer goroutines.
		var (
			drainedFirstStderr bool
			emitMu             sync.Mutex
		)
		emitChunk := func(origin, chunk string) {
			emitMu.Lock()
			defer emitMu.Unlock()
			if origin == "stderr" && !drainedFirstStderr {
				select {
				case out <- "STDERR:\n" + chunk:
				case <-runCtx.Done():
				}
				drainedFirstStderr = true
				return
			}
			select {
			case out <- chunk:
			case <-runCtx.Done():
			}
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go drainPipe(runCtx, stdoutPipe, "stdout", emitChunk, &wg)
		go drainPipe(runCtx, stderrPipe, "stderr", emitChunk, &wg)
		wg.Wait()

		err := cmd.Wait()
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			out <- fmt.Sprintf("\nError: Command timed out after %d seconds", int(t.Timeout.Seconds()))
			return
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			out <- fmt.Sprintf("\nExit code: %d", exitErr.ExitCode())
		}
	}()
	return out, nil
}

func drainPipe(ctx context.Context, r io.ReadCloser, origin string, emit func(string, string), wg *sync.WaitGroup) {
	defer wg.Done()
	defer r.Close()
	buf := make([]byte, 8192)
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := r.Read(buf)
		if n > 0 {
			emit(origin, string(buf[:n]))
		}
		if err != nil {
			return
		}
	}
}

// guard runs the deny / allow / workspace-restrict safety checks.
// Returns the agent-visible refusal string on block, or "" on pass.
func (t *ExecTool) guard(command, cwd string) string {
	cmd := strings.TrimSpace(command)
	lower := strings.ToLower(cmd)

	for _, re := range t.DenyPatterns {
		if re.MatchString(lower) {
			return "Error: Command blocked by safety guard (dangerous pattern detected)"
		}
	}
	if len(t.AllowPatterns) > 0 {
		ok := false
		for _, re := range t.AllowPatterns {
			if re.MatchString(lower) {
				ok = true
				break
			}
		}
		if !ok {
			return "Error: Command blocked by safety guard (not in allowlist)"
		}
	}

	if t.RestrictToWorkspace {
		if strings.Contains(cmd, "..\\") || strings.Contains(cmd, "../") {
			return "Error: Command blocked by safety guard (path traversal detected)"
		}
		cwdResolved, err := filepath.EvalSymlinks(cwd)
		if err != nil {
			cwdResolved = filepath.Clean(cwd)
		}
		for _, raw := range extractAbsolutePaths(cmd) {
			resolved, err := filepath.EvalSymlinks(strings.TrimSpace(raw))
			if err != nil {
				continue
			}
			if filepath.IsAbs(resolved) && !isWithin(resolved, cwdResolved) {
				return "Error: Command blocked by safety guard (path outside working dir)"
			}
		}
	}
	return ""
}

var (
	winPathRE   = regexp.MustCompile(`[A-Za-z]:\\[^\s"'|><;]+`)
	posixPathRE = regexp.MustCompile(`(?:^|[\s|>])(/[^\s"'>]+)`)
)

func extractAbsolutePaths(command string) []string {
	var out []string
	out = append(out, winPathRE.FindAllString(command, -1)...)
	for _, m := range posixPathRE.FindAllStringSubmatch(command, -1) {
		if len(m) > 1 {
			out = append(out, m[1])
		}
	}
	return out
}

func isWithin(child, parent string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return !strings.HasPrefix(rel, "..") && rel != ".."
}

func shell() string {
	if v := os.Getenv("SHELL"); v != "" {
		return v
	}
	return "/bin/sh"
}

func shellFlag() string {
	// POSIX shell `-c` invocation. The Python original used
	// `asyncio.create_subprocess_shell` which on Unix invokes
	// `/bin/sh -c <cmd>`; we mirror that. Windows is not a target.
	return "-c"
}

func setEnv(env []string, key, val string) []string {
	prefix := key + "="
	for i, kv := range env {
		if strings.HasPrefix(kv, prefix) {
			env[i] = prefix + val
			return env
		}
	}
	return append(env, prefix+val)
}

// Compile-time conformance.
var (
	_ tools.Tool          = (*ExecTool)(nil)
	_ tools.StreamingTool = (*ExecTool)(nil)
)
