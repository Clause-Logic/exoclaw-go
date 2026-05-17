// Package workspace contains the file, shell, and web tools used by every
// Standd-flavor agent deployment.
//
// Ported from exoclaw-plugins/packages/exoclaw-tools-workspace.
package workspace

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/standd/exoclaw-go/exoclaw/agent/tools"
)

// Ported from exoclaw_tools_workspace/filesystem.py.

// MaxChars is the inline char cap. The Python original scales this per
// runtime (128 KB CPython, 32 KB MicroPython); Go runs on real machines
// so we keep the CPython value.
const MaxChars = 128_000

// fileSize returns the size of path in bytes, or -1 on stat error.
func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return -1
	}
	return info.Size()
}

// resolvePath maps a caller-supplied path against workspace (for
// relatives) and rejects anything outside allowedDir. Two-layer sandbox:
//
//  1. `..` segment reject — covers the obvious traversal attempt.
//  2. `filepath.EvalSymlinks`-resolved containment check — a symlink
//     inside the workspace pointing outside it is rejected.
//
// workspace and allowedDir may be "": no sandbox, paths still resolved.
func resolvePath(path, workspace, allowedDir string) (string, error) {
	sandbox := allowedDir
	if sandbox == "" {
		sandbox = workspace
	}

	cleanIn := strings.ReplaceAll(path, "\\", "/")
	for _, seg := range strings.Split(cleanIn, "/") {
		if seg == ".." {
			return "", fmt.Errorf("path may not contain '..' segments: %q", path)
		}
	}

	// Strip a leading workspace-name prefix the model sometimes
	// includes (e.g. ".sim-workspace/screen.md" when the workspace
	// IS .sim-workspace).
	if workspace != "" {
		wsBase := filepath.Base(workspace)
		if strings.HasPrefix(path, wsBase+"/") {
			path = path[len(wsBase)+1:]
		} else if path == wsBase {
			path = "."
		}
	}

	abs := path
	if !filepath.IsAbs(abs) && workspace != "" {
		abs = filepath.Join(workspace, path)
	}
	resolved := resolveExistingAncestor(abs)

	if sandbox != "" {
		sandboxResolved := resolveExistingAncestor(sandbox)
		rel, err := filepath.Rel(sandboxResolved, resolved)
		if err != nil || strings.HasPrefix(rel, "..") || rel == ".." {
			return "", fmt.Errorf("path %q is outside sandbox %q", resolved, sandboxResolved)
		}
	}
	return resolved, nil
}

// resolveExistingAncestor returns the EvalSymlinks-resolved form of p.
// When p doesn't exist, walks up to the first existing ancestor, resolves
// THAT, then re-joins the un-resolvable suffix. This is the only way to
// keep `relative_to(sandbox)` honest under macOS where `/var/folders/...`
// is a symlink to `/private/var/folders/...`: without walking up to the
// real existing ancestor, the sandbox would resolve through the symlink
// while a not-yet-created target wouldn't, and the prefixes mismatch.
func resolveExistingAncestor(p string) string {
	if resolved, err := filepath.EvalSymlinks(p); err == nil {
		return resolved
	}
	p = filepath.Clean(p)
	suffix := []string{}
	for {
		parent, base := filepath.Dir(p), filepath.Base(p)
		if parent == p {
			break
		}
		suffix = append([]string{base}, suffix...)
		if resolved, err := filepath.EvalSymlinks(parent); err == nil {
			return filepath.Join(append([]string{resolved}, suffix...)...)
		}
		p = parent
	}
	return p
}

// ----------------------------------------------------------------------
// ReadFileTool
// ----------------------------------------------------------------------

// ReadFileTool reads file contents with optional offset/limit ranged reads
// for large files.
type ReadFileTool struct {
	tools.ToolBase
	Workspace  string
	AllowedDir string
	// MaxCharsOverride, when > 0, overrides the package-level MaxChars
	// cap for this tool instance. Useful for tests.
	MaxCharsOverride int
}

// NewReadFileTool constructs a ReadFileTool rooted at workspace.
func NewReadFileTool(workspace, allowedDir string) *ReadFileTool {
	t := &ReadFileTool{Workspace: workspace, AllowedDir: allowedDir}
	t.NameField = "read_file"
	t.DescriptionField = "Read the contents of a file. Use offset and limit to read a specific " +
		"line range instead of the entire file (recommended for large files)."
	t.ParametersField = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path":   map[string]any{"type": "string", "description": "The file path to read"},
			"offset": map[string]any{"type": "integer", "description": "Line number to start from (0-based, default 0)"},
			"limit":  map[string]any{"type": "integer", "description": "Maximum number of lines to return"},
		},
		"required": []any{"path"},
	}
	return t
}

func (t *ReadFileTool) cap() int {
	if t.MaxCharsOverride > 0 {
		return t.MaxCharsOverride
	}
	return MaxChars
}

// Execute reads the file at params["path"] respecting params["offset"]
// and params["limit"] for ranged reads.
func (t *ReadFileTool) Execute(_ context.Context, params map[string]any) (string, error) {
	path, _ := params["path"].(string)
	offset := intOrZero(params["offset"])
	limit, hasLimit := intOptional(params["limit"])
	if offset < 0 {
		return "Error: offset must be >= 0", nil
	}
	if hasLimit && limit < 1 {
		return "Error: limit must be >= 1", nil
	}

	filePath, err := resolvePath(path, t.Workspace, t.AllowedDir)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	info, err := os.Stat(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "Error: File not found: " + path, nil
		}
		return "Error: " + err.Error(), nil
	}
	if info.IsDir() {
		return "Error: Not a file: " + path, nil
	}
	size := info.Size()

	maxChars := t.cap()

	// Early exit before allocation.
	if offset == 0 && !hasLimit && size > int64(maxChars)*4 {
		return fmt.Sprintf("Error: File too large (%d bytes). "+
			"Use offset and limit to read a portion, e.g. "+
			"read_file(path, offset=0, limit=50).", size), nil
	}

	// Ranged read.
	if offset > 0 || hasLimit {
		f, err := os.Open(filePath)
		if err != nil {
			return "Error: " + err.Error(), nil
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		var selected []string
		totalLines := 0
		end := -1
		if hasLimit {
			end = offset + limit
		}
		for i := 0; scanner.Scan(); i++ {
			totalLines = i + 1
			if i < offset {
				continue
			}
			if end != -1 && i >= end {
				for scanner.Scan() {
					totalLines++
				}
				break
			}
			selected = append(selected, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			return "Error reading file: " + err.Error(), nil
		}
		actualEnd := offset + len(selected)
		header := fmt.Sprintf("[lines %d-%d of %d]\n", offset+1, actualEnd, totalLines)
		// Reattach the newline a Scanner stripped, joining with "\n"
		// and appending one trailing "\n" if the original had any
		// selected lines.
		text := strings.Join(selected, "\n")
		if len(selected) > 0 {
			text += "\n"
		}
		if len(text) > maxChars {
			text = text[:maxChars] + "\n... (truncated)"
		}
		return header + text, nil
	}

	// Full file.
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	if len(content) > maxChars {
		return string(content[:maxChars]) +
			fmt.Sprintf("\n\n... (truncated — file is %d chars, showing first %d. "+
				"Use offset/limit to read more.)", len(content), maxChars), nil
	}
	return string(content), nil
}

// ExecuteStreaming implements the StreamingTool capability — Step D from
// the memory-model doc. Streams the file's content in 8 KiB chunks for
// the full-file path; ranged reads fall back to Execute.
func (t *ReadFileTool) ExecuteStreaming(ctx context.Context, params map[string]any) (<-chan string, error) {
	path, _ := params["path"].(string)
	offset := intOrZero(params["offset"])
	limit, hasLimit := intOptional(params["limit"])

	out := make(chan string)
	emit := func(s string) {
		out <- s
	}

	go func() {
		defer close(out)
		if offset < 0 {
			emit("Error: offset must be >= 0")
			return
		}
		if hasLimit && limit < 1 {
			emit("Error: limit must be >= 1")
			return
		}
		if offset > 0 || hasLimit {
			result, err := t.Execute(ctx, params)
			if err == nil {
				emit(result)
			} else {
				emit("Error: " + err.Error())
			}
			return
		}
		filePath, err := resolvePath(path, t.Workspace, t.AllowedDir)
		if err != nil {
			emit("Error: " + err.Error())
			return
		}
		info, err := os.Stat(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				emit("Error: File not found: " + path)
				return
			}
			emit("Error: " + err.Error())
			return
		}
		if info.IsDir() {
			emit("Error: Not a file: " + path)
			return
		}
		f, err := os.Open(filePath)
		if err != nil {
			emit("Error: " + err.Error())
			return
		}
		defer f.Close()
		buf := make([]byte, 8192)
		for {
			if ctx.Err() != nil {
				return
			}
			n, rerr := f.Read(buf)
			if n > 0 {
				emit(string(buf[:n]))
			}
			if rerr != nil {
				return
			}
		}
	}()
	return out, nil
}

// Compile-time conformance checks.
var (
	_ tools.Tool          = (*ReadFileTool)(nil)
	_ tools.StreamingTool = (*ReadFileTool)(nil)
)

// ----------------------------------------------------------------------
// WriteFileTool
// ----------------------------------------------------------------------

// WriteFileTool writes content to a file, creating parent directories.
type WriteFileTool struct {
	tools.ToolBase
	Workspace  string
	AllowedDir string
}

// NewWriteFileTool constructs a WriteFileTool.
func NewWriteFileTool(workspace, allowedDir string) *WriteFileTool {
	t := &WriteFileTool{Workspace: workspace, AllowedDir: allowedDir}
	t.NameField = "write_file"
	t.DescriptionField = "Write content to a file at the given path. Creates parent directories if needed."
	t.ParametersField = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path":    map[string]any{"type": "string", "description": "The file path to write to"},
			"content": map[string]any{"type": "string", "description": "The content to write"},
		},
		"required": []any{"path", "content"},
	}
	return t
}

// Execute writes content to params["path"], creating parents as needed.
func (t *WriteFileTool) Execute(_ context.Context, params map[string]any) (string, error) {
	path, _ := params["path"].(string)
	content, _ := params["content"].(string)
	filePath, err := resolvePath(path, t.Workspace, t.AllowedDir)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return "Error writing file: " + err.Error(), nil
	}
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		return "Error writing file: " + err.Error(), nil
	}
	return fmt.Sprintf("Successfully wrote %d bytes to %s", len(content), filePath), nil
}

var _ tools.Tool = (*WriteFileTool)(nil)

// ----------------------------------------------------------------------
// EditFileTool
// ----------------------------------------------------------------------

// EditFileTool replaces exact old_text with new_text. The match must be
// unique — if old_text appears multiple times, the call is rejected.
type EditFileTool struct {
	tools.ToolBase
	Workspace  string
	AllowedDir string
}

// NewEditFileTool constructs an EditFileTool.
func NewEditFileTool(workspace, allowedDir string) *EditFileTool {
	t := &EditFileTool{Workspace: workspace, AllowedDir: allowedDir}
	t.NameField = "edit_file"
	t.DescriptionField = "Edit a file by replacing old_text with new_text. The old_text must exist " +
		"exactly in the file."
	t.ParametersField = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path":     map[string]any{"type": "string", "description": "The file path to edit"},
			"old_text": map[string]any{"type": "string", "description": "The exact text to find and replace"},
			"new_text": map[string]any{"type": "string", "description": "The text to replace with"},
		},
		"required": []any{"path", "old_text", "new_text"},
	}
	return t
}

// Execute replaces old_text with new_text in the file at params["path"].
func (t *EditFileTool) Execute(_ context.Context, params map[string]any) (string, error) {
	path, _ := params["path"].(string)
	oldText, _ := params["old_text"].(string)
	newText, _ := params["new_text"].(string)
	filePath, err := resolvePath(path, t.Workspace, t.AllowedDir)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	raw, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "Error: File not found: " + path, nil
		}
		return "Error: " + err.Error(), nil
	}
	content := string(raw)
	if !strings.Contains(content, oldText) {
		return notFoundMessage(oldText, content, path), nil
	}
	count := strings.Count(content, oldText)
	if count > 1 {
		return fmt.Sprintf("Warning: old_text appears %d times. Please provide more context "+
			"to make it unique.", count), nil
	}
	newContent := strings.Replace(content, oldText, newText, 1)
	if err := os.WriteFile(filePath, []byte(newContent), 0o644); err != nil {
		return "Error editing file: " + err.Error(), nil
	}
	return "Successfully edited " + filePath, nil
}

// notFoundMessage finds the longest prefix of oldText that appears in
// content and renders a 100-char window for context. Replaces the Python
// difflib-unified-diff output with a simpler runtime-portable hint.
func notFoundMessage(oldText, content, path string) string {
	bestPrefixLen := 0
	bestPos := -1
	maxLen := len(oldText)
	if maxLen > 64 {
		maxLen = 64
	}
	for length := maxLen; length > 0; length-- {
		prefix := oldText[:length]
		pos := strings.Index(content, prefix)
		if pos != -1 {
			bestPrefixLen = length
			bestPos = pos
			break
		}
	}
	if bestPos == -1 || bestPrefixLen < 3 {
		return fmt.Sprintf("Error: old_text not found in %s. No similar text found. "+
			"Verify the file content.", path)
	}
	ctxStart := bestPos - 100
	if ctxStart < 0 {
		ctxStart = 0
	}
	ctxEnd := bestPos + bestPrefixLen + 100
	if ctxEnd > len(content) {
		ctxEnd = len(content)
	}
	lineNo := strings.Count(content[:bestPos], "\n") + 1
	snippet := content[ctxStart:ctxEnd]
	return fmt.Sprintf("Error: old_text not found in %s. Closest match (first %d chars of "+
		"old_text) at line %d:\n%s", path, bestPrefixLen, lineNo, snippet)
}

var _ tools.Tool = (*EditFileTool)(nil)

// ----------------------------------------------------------------------
// ListDirTool
// ----------------------------------------------------------------------

// ListDirTool lists the contents of a directory.
type ListDirTool struct {
	tools.ToolBase
	Workspace  string
	AllowedDir string
}

// NewListDirTool constructs a ListDirTool.
func NewListDirTool(workspace, allowedDir string) *ListDirTool {
	t := &ListDirTool{Workspace: workspace, AllowedDir: allowedDir}
	t.NameField = "list_dir"
	t.DescriptionField = "List the contents of a directory."
	t.ParametersField = map[string]any{
		"type": "object",
		"properties": map[string]any{
			"path": map[string]any{"type": "string", "description": "The directory path to list"},
		},
		"required": []any{"path"},
	}
	return t
}

// Execute lists the entries of the directory at params["path"].
func (t *ListDirTool) Execute(_ context.Context, params map[string]any) (string, error) {
	path, _ := params["path"].(string)
	dirPath, err := resolvePath(path, t.Workspace, t.AllowedDir)
	if err != nil {
		return "Error: " + err.Error(), nil
	}
	info, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "Error: Directory not found: " + path, nil
		}
		return "Error: " + err.Error(), nil
	}
	if !info.IsDir() {
		return "Error: Not a directory: " + path, nil
	}
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return "Error listing directory: " + err.Error(), nil
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	if len(entries) == 0 {
		return "Directory " + path + " is empty", nil
	}
	var items []string
	for _, e := range entries {
		prefix := "[f] "
		if e.IsDir() {
			prefix = "[d] "
		}
		items = append(items, prefix+e.Name())
	}
	return strings.Join(items, "\n"), nil
}

var _ tools.Tool = (*ListDirTool)(nil)

// ----------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------

func intOrZero(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	}
	return 0
}

func intOptional(v any) (int, bool) {
	if v == nil {
		return 0, false
	}
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case float64:
		return int(x), true
	}
	return 0, false
}
