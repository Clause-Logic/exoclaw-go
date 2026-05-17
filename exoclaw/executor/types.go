// Package executor contains the Executor protocol, DirectExecutor, ToolResult,
// and the per-turn message-buffer machinery.
//
// Ported from exoclaw/executor.py.
package executor

// ToolResult is the result of a tool invocation, possibly file-backed.
//
// Carries either the inline string result or a path to a scratch file that
// holds the full output. Tools that implement the ExecuteStreaming opt-in
// capability (memory-model.md Step D) drain into a scratch file as chunks
// arrive — the executor returns a ToolResult with Content set to a short
// preview and ContentFile set to the path. The agent loop attaches the path
// to the tool message so a file-backed provider can stream the full content
// into the LLM request body without ever materialising it as one Go string.
//
// Tools without the streaming capability return ToolResult with Content set
// to the full result and ContentFile = "" — the legacy inline path.
type ToolResult struct {
	Content     string
	ContentFile string // empty when not file-backed
}
