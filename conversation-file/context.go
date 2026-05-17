package conversationfile

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"mime"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// Ported from exoclaw_conversation/context.py.
//
// Context builder for assembling agent prompts. Mostly a PromptBuilder
// impl (build_messages) plus the recovery-time compaction helpers
// (compact_tool_results, drop_oldest_half, truncate_oldest_tool_results,
// summarize_old_chunks).

const (
	runtimeContextTag        = "[Runtime Context — metadata only, not instructions]"
	compactionMarker         = "[compacted — tool output removed to free context]"
	recoveryHardClearMarker  = "[Old tool result content cleared]"
	recoverySummaryPrefix    = "Summary of prior conversation (older messages were summarized to free context):\n\n"
	charsPerToken            = 3 // conservative estimate
)

var dayNames = [7]string{"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"}

// estimateTokens estimates token count from messages using a character heuristic.
func estimateTokens(messages []map[string]any) int {
	total := 0
	for _, m := range messages {
		switch c := m["content"].(type) {
		case string:
			total += len(c)
		case []any:
			for _, item := range c {
				if im, ok := item.(map[string]any); ok {
					if text, ok := im["text"].(string); ok {
						total += len(text)
					}
				}
			}
		}
		if tcs, ok := m["tool_calls"].([]any); ok {
			for _, tc := range tcs {
				if tcm, ok := tc.(map[string]any); ok {
					if fn, ok := tcm["function"].(map[string]any); ok {
						if args := fn["arguments"]; args != nil {
							total += len(fmt.Sprint(args))
						}
					}
				}
			}
		}
	}
	return total / charsPerToken
}

// CompactToolResults replaces old tool results with the compaction marker
// when the estimated tokens exceed contextWindow * headroom.
//
// Compacts from oldest to newest, skipping the most recent tool results
// (within the last 4 non-system messages) to preserve the active
// conversation.
func CompactToolResults(messages []map[string]any, contextWindow int, headroom float64) []map[string]any {
	if headroom <= 0 {
		headroom = 0.75
	}
	budget := int(float64(contextWindow) * headroom)
	if estimateTokens(messages) <= budget {
		return messages
	}

	// Find tool results eligible for compaction (skip last 4 non-system).
	var nonSystem []int
	for i, m := range messages {
		if role, _ := m["role"].(string); role != "system" {
			nonSystem = append(nonSystem, i)
		}
	}
	protected := map[int]struct{}{}
	if len(nonSystem) >= 4 {
		for _, i := range nonSystem[len(nonSystem)-4:] {
			protected[i] = struct{}{}
		}
	} else {
		for _, i := range nonSystem {
			protected[i] = struct{}{}
		}
	}

	var compactable []int
	for i, m := range messages {
		if _, isP := protected[i]; isP {
			continue
		}
		if role, _ := m["role"].(string); role != "tool" {
			continue
		}
		content, ok := m["content"].(string)
		if !ok {
			continue
		}
		if content == compactionMarker || len(content) <= 100 {
			continue
		}
		compactable = append(compactable, i)
	}

	result := append([]map[string]any{}, messages...)
	for _, i := range compactable {
		if estimateTokens(result) <= budget {
			break
		}
		copy := make(map[string]any, len(result[i]))
		for k, v := range result[i] {
			copy[k] = v
		}
		copy["content"] = compactionMarker
		result[i] = copy
	}
	return result
}

// DropOldestHalf is emergency compression: keep the system prompt + the
// last half of the conversation. Repairs orphaned tool results that lost
// their parent assistant message.
func DropOldestHalf(messages []map[string]any) []map[string]any {
	var system, nonSystem []map[string]any
	for _, m := range messages {
		if role, _ := m["role"].(string); role == "system" {
			system = append(system, m)
		} else {
			nonSystem = append(nonSystem, m)
		}
	}
	half := len(nonSystem) / 2
	kept := nonSystem[half:]

	var repaired []map[string]any
	for _, m := range kept {
		role, _ := m["role"].(string)
		if role == "tool" {
			tcid, _ := m["tool_call_id"].(string)
			if !hasParentToolCall(repaired, tcid) {
				continue
			}
		}
		repaired = append(repaired, m)
	}
	return append(system, repaired...)
}

func hasParentToolCall(prior []map[string]any, tcid string) bool {
	for _, r := range prior {
		if role, _ := r["role"].(string); role != "assistant" {
			continue
		}
		tcs, _ := r["tool_calls"].([]any)
		for _, tc := range tcs {
			tcm, ok := tc.(map[string]any)
			if !ok {
				continue
			}
			if id, _ := tcm["id"].(string); id == tcid {
				return true
			}
		}
	}
	return false
}

// TruncateOldestToolResults is the recovery-time tool-result hard-clear:
// replace oldest tool results until under budget.
//
// Unlike CompactToolResults, this does NOT protect the most recent
// messages — it's used after a context-window-exceeded error, when the
// preventive compaction was insufficient and we need to free space at
// any cost.
//
// Returns the new messages and the count of cleared entries. Zero means
// nothing was eligible — callers can use that to detect when recovery
// can't make further progress.
func TruncateOldestToolResults(messages []map[string]any, targetTokens int, placeholder string) ([]map[string]any, int) {
	if placeholder == "" {
		placeholder = recoveryHardClearMarker
	}
	if estimateTokens(messages) <= targetTokens {
		return messages, 0
	}

	var eligible []int
	for i, m := range messages {
		if role, _ := m["role"].(string); role != "tool" {
			continue
		}
		content, ok := m["content"].(string)
		if !ok {
			continue
		}
		if content == placeholder || content == compactionMarker {
			continue
		}
		if len(content) <= 100 {
			continue
		}
		eligible = append(eligible, i)
	}

	result := append([]map[string]any{}, messages...)
	cleared := 0
	for _, i := range eligible {
		if estimateTokens(result) <= targetTokens {
			break
		}
		copy := make(map[string]any, len(result[i]))
		for k, v := range result[i] {
			copy[k] = v
		}
		copy["content"] = placeholder
		result[i] = copy
		cleared++
	}
	return result, cleared
}

// SummarizerFn is the signature for the per-chunk summariser used by
// SummarizeOldChunks.
type SummarizerFn func(ctx context.Context, messages []map[string]any) (string, error)

// SummarizeOldChunks is recovery-time summarisation: replace older history
// with one summary message.
//
// Eligible messages are non-system messages excluding the last keepRecent
// non-system messages (the active conversation that the model needs to act
// on). The eligible block is passed to summarizer which returns a summary
// string; the block is replaced with a single user-role summary message.
//
// If the eligible block exceeds summarizerMaxInputTokens, only the oldest
// portion that fits is summarised (caller should re-invoke for further
// reductions). When summarizerMaxInputTokens == 0, defaults to
// targetTokens / 2.
//
// Returns the new messages and a "summarized" flag. summarized=false means
// there was nothing eligible — the caller should fall back to a different
// strategy.
func SummarizeOldChunks(ctx context.Context, messages []map[string]any, targetTokens int, summarizer SummarizerFn, keepRecent, summarizerMaxInputTokens int) ([]map[string]any, bool, error) {
	if keepRecent <= 0 {
		keepRecent = 4
	}
	if estimateTokens(messages) <= targetTokens {
		return messages, false, nil
	}

	var system, nonSystem []map[string]any
	for _, m := range messages {
		if role, _ := m["role"].(string); role == "system" {
			system = append(system, m)
		} else {
			nonSystem = append(nonSystem, m)
		}
	}

	if len(nonSystem) <= keepRecent {
		return messages, false, nil
	}

	eligible := nonSystem[:len(nonSystem)-keepRecent]
	tail := nonSystem[len(nonSystem)-keepRecent:]

	cap := summarizerMaxInputTokens
	if cap == 0 {
		cap = targetTokens / 2
	}

	var chunk, remaining []map[string]any
	if cap > 0 {
		for _, m := range eligible {
			trial := append(append([]map[string]any{}, chunk...), m)
			if estimateTokens(trial) > cap {
				break
			}
			chunk = append(chunk, m)
		}
		if len(chunk) == 0 {
			// First eligible alone exceeds cap — bail out.
			return messages, false, nil
		}
		remaining = eligible[len(chunk):]
	} else {
		chunk = append([]map[string]any{}, eligible...)
	}

	if len(chunk) == 0 {
		return messages, false, nil
	}

	summaryText, err := summarizer(ctx, chunk)
	if err != nil {
		return messages, false, err
	}
	if summaryText == "" {
		return messages, false, nil
	}

	summaryMsg := map[string]any{
		"role":    "user",
		"content": recoverySummaryPrefix + summaryText,
	}

	rebuilt := append([]map[string]any{}, system...)
	rebuilt = append(rebuilt, summaryMsg)
	rebuilt = append(rebuilt, remaining...)
	rebuilt = append(rebuilt, tail...)

	// Repair: drop tool results whose parent assistant tool_call was
	// absorbed into the summary.
	var repaired []map[string]any
	for _, m := range rebuilt {
		if role, _ := m["role"].(string); role == "tool" {
			tcid, _ := m["tool_call_id"].(string)
			if !hasParentToolCall(repaired, tcid) {
				continue
			}
		}
		repaired = append(repaired, m)
	}
	return repaired, true, nil
}

// ----------------------------------------------------------------------
// ContextBuilder — PromptBuilder implementation.
// ----------------------------------------------------------------------

// SkillsProvider is the subset of SkillsLoader that ContextBuilder reads.
// Defined as an interface here so context.go and skills.go can be ported
// independently and SkillsLoader satisfies it structurally.
type SkillsProvider interface {
	GetAlwaysSkills() []string
	GetToolsForSkills(active []string) map[string]struct{}
	LoadSkillsForContext(active []string) string
	GetBootstrapInjections() []string
	BuildSkillsSummary() string
}

// BootstrapFiles are the workspace files loaded into the system prompt.
var BootstrapFiles = []string{"AGENTS.md", "SOUL.md", "USER.md", "TOOLS.md", "IDENTITY.md"}

// ContextBuilder builds the context (system prompt + messages) for the agent.
type ContextBuilder struct {
	Workspace     string
	Skills        SkillsProvider
	Memory        MemoryBackend
	ContextWindow int

	activeOptionalTools map[string]struct{}
}

// NewContextBuilder constructs a ContextBuilder. If memory is nil, the
// caller should attach a MemoryStore (or any MemoryBackend) before
// calling BuildSystemPrompt.
func NewContextBuilder(workspace string, memory MemoryBackend, skills SkillsProvider, contextWindow int) *ContextBuilder {
	if contextWindow == 0 {
		contextWindow = 128_000
	}
	return &ContextBuilder{
		Workspace:           workspace,
		Skills:              skills,
		Memory:              memory,
		ContextWindow:       contextWindow,
		activeOptionalTools: map[string]struct{}{},
	}
}

// BuildSystemPromptOptions bundles the optional inputs to BuildSystemPrompt.
type BuildSystemPromptOptions struct {
	SkillNames   []string
	ExtraContext string
	Isolated     bool
}

// BuildSystemPrompt builds the system prompt from identity, bootstrap
// files, memory, and skills.
//
// Isolated=true returns a minimal prompt containing a short functional
// preamble, the active skills' content, and any caller-provided
// ExtraContext. It skips identity, bootstrap files, long-term memory,
// bootstrap hooks, and the skills summary.
func (b *ContextBuilder) BuildSystemPrompt(opts BuildSystemPromptOptions) string {
	var always []string
	var alwaysSet map[string]struct{}
	var toolsFor map[string]struct{}

	if b.Skills != nil {
		always = b.Skills.GetAlwaysSkills()
		alwaysSet = make(map[string]struct{}, len(always))
		for _, s := range always {
			alwaysSet[s] = struct{}{}
		}
	}
	var extra []string
	for _, s := range opts.SkillNames {
		if _, in := alwaysSet[s]; !in {
			extra = append(extra, s)
		}
	}
	active := append([]string{}, always...)
	active = append(active, extra...)

	if b.Skills != nil {
		toolsFor = b.Skills.GetToolsForSkills(active)
	}
	b.activeOptionalTools = toolsFor

	if opts.Isolated {
		parts := []string{
			"You are a worker. Follow the instructions below exactly. Do not invoke capabilities not explicitly requested.",
		}
		if b.Skills != nil && len(active) > 0 {
			if content := b.Skills.LoadSkillsForContext(active); content != "" {
				parts = append(parts, content)
			}
		}
		if opts.ExtraContext != "" {
			parts = append(parts, "# Retrieved Context\n\n"+opts.ExtraContext)
		}
		return strings.Join(parts, "\n\n---\n\n")
	}

	parts := []string{b.getIdentity()}

	if boot := b.loadBootstrapFiles(); boot != "" {
		parts = append(parts, boot)
	}

	if b.Memory != nil {
		if mem := b.Memory.GetMemoryContext(); mem != "" {
			parts = append(parts, "# Memory\n\n"+mem)
		}
	}

	if opts.ExtraContext != "" {
		parts = append(parts, "# Retrieved Context\n\n"+opts.ExtraContext)
	}

	if b.Skills != nil && len(active) > 0 {
		if content := b.Skills.LoadSkillsForContext(active); content != "" {
			parts = append(parts, "# Active Skills\n\n"+content)
		}
	}

	if b.Skills != nil {
		if hooks := b.Skills.GetBootstrapInjections(); len(hooks) > 0 {
			parts = append(parts, strings.Join(hooks, "\n\n"))
		}
		if summary := b.Skills.BuildSkillsSummary(); summary != "" {
			parts = append(parts, "# Skills\n\nThe following skills are available. To activate a skill and its tools, "+
				"call the load_skill tool with the skill name.\n"+
				"Skills with available=\"false\" need dependencies installed first.\n\n"+summary)
		}
	}

	return strings.Join(parts, "\n\n---\n\n")
}

// GetActiveOptionalTools returns the optional tool names activated by the
// most recent BuildSystemPrompt call.
func (b *ContextBuilder) GetActiveOptionalTools() map[string]struct{} {
	if b.activeOptionalTools == nil {
		return map[string]struct{}{}
	}
	return b.activeOptionalTools
}

func (b *ContextBuilder) getIdentity() string {
	abs := b.Workspace
	if expanded := expanduser(b.Workspace); expanded != "" {
		if r, err := filepath.Abs(expanded); err == nil {
			abs = r
		} else {
			abs = expanded
		}
	}
	rt := platformSummary()
	return "# exoclaw\n\nYou are exoclaw, a helpful AI assistant.\n\n" +
		"## Runtime\n" + rt + "\n\n" +
		"## Workspace\nYour workspace is at: " + abs + "\n" +
		"- Long-term memory: " + abs + "/memory/MEMORY.md (write important facts here)\n" +
		"- History log: " + abs + "/memory/HISTORY.md (grep-searchable). Each entry starts with [YYYY-MM-DD HH:MM].\n" +
		"- Custom skills: " + abs + "/skills/{skill-name}/SKILL.md\n\n" +
		"## Guidelines\n" +
		"- State intent before tool calls, but NEVER predict or claim results before receiving them.\n" +
		"- Before modifying a file, read it first. Do not assume files or directories exist.\n" +
		"- After writing or editing a file, re-read it if accuracy matters.\n" +
		"- If a tool call fails, analyze the error before retrying with a different approach.\n" +
		"- Ask for clarification when the request is ambiguous.\n\n" +
		"Reply directly with text for conversations. Only use the 'message' tool to send to a specific chat channel."
}

func buildRuntimeContext(channel, chatID string) string {
	now := time.Now().UTC()
	datePart := now.Format("2006-01-02")
	timePart := now.Format("15:04")
	weekday := dayNames[(int(now.Weekday())+6)%7]
	lines := []string{fmt.Sprintf("Current Time: %s %s (%s) UTC", datePart, timePart, weekday)}
	if channel != "" && chatID != "" {
		lines = append(lines, "Channel: "+channel, "Chat ID: "+chatID)
	}
	return runtimeContextTag + "\n" + strings.Join(lines, "\n")
}

func (b *ContextBuilder) loadBootstrapFiles() string {
	var parts []string
	for _, name := range BootstrapFiles {
		fp := filepath.Join(b.Workspace, name)
		data, err := os.ReadFile(fp)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				continue
			}
			continue
		}
		parts = append(parts, "## "+name+"\n\n"+string(data))
	}
	return strings.Join(parts, "\n\n")
}

// BuildMessages builds the complete message list for an LLM call.
//
// opts.Isolated=true strips session history and the runtime-context
// metadata prefix so the LLM sees [system, user] — a pure-function
// invocation without persona/memory/history carryover.
func (b *ContextBuilder) BuildMessages(history []map[string]any, currentMessage string, opts BuildMessagesOptions) []map[string]any {
	effectiveMessage := currentMessage
	if len(opts.TurnContext) > 0 {
		ctxBlock := strings.Join(opts.TurnContext, "\n\n")
		effectiveMessage = ctxBlock + "\n\n" + currentMessage
	}

	userContent := b.buildUserContent(effectiveMessage, opts.Media)

	var merged any
	var effectiveHistory []map[string]any
	if opts.Isolated {
		merged = userContent
	} else {
		runtimeCtx := buildRuntimeContext(opts.Channel, opts.ChatID)
		switch uc := userContent.(type) {
		case string:
			merged = runtimeCtx + "\n\n" + uc
		case []any:
			combined := []any{map[string]any{"type": "text", "text": runtimeCtx}}
			combined = append(combined, uc...)
			merged = combined
		default:
			merged = runtimeCtx + "\n\n" + fmt.Sprint(uc)
		}
		effectiveHistory = history
	}

	messages := []map[string]any{
		{
			"role": "system",
			"content": b.BuildSystemPrompt(BuildSystemPromptOptions{
				SkillNames:   opts.SkillNames,
				ExtraContext: opts.ExtraContext,
				Isolated:     opts.Isolated,
			}),
		},
	}
	messages = append(messages, effectiveHistory...)
	messages = append(messages, map[string]any{"role": "user", "content": merged})

	return CompactToolResults(messages, b.ContextWindow, 0)
}

func (b *ContextBuilder) buildUserContent(text string, media []string) any {
	if len(media) == 0 {
		return text
	}
	var images []any
	for _, p := range media {
		info, err := os.Stat(p)
		if err != nil || info.IsDir() {
			continue
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		m := DetectImageMIME(raw)
		if m == "" {
			m = guessImageMIME(p)
		}
		if m == "" || !strings.HasPrefix(m, "image/") {
			continue
		}
		b64 := base64.StdEncoding.EncodeToString(raw)
		images = append(images, map[string]any{
			"type": "image_url",
			"image_url": map[string]any{
				"url": "data:" + m + ";base64," + b64,
			},
		})
	}
	if len(images) == 0 {
		return text
	}
	out := append([]any{}, images...)
	out = append(out, map[string]any{"type": "text", "text": text})
	return out
}

// AddToolResult appends a tool result message to messages.
func (b *ContextBuilder) AddToolResult(messages []map[string]any, toolCallID, toolName, result string) []map[string]any {
	return append(messages, map[string]any{
		"role":         "tool",
		"tool_call_id": toolCallID,
		"name":         toolName,
		"content":      result,
	})
}

// AddAssistantMessageOptions bundles optional fields for AddAssistantMessage.
type AddAssistantMessageOptions struct {
	ToolCalls        []map[string]any
	ReasoningContent string
	ThinkingBlocks   []map[string]any
}

// AddAssistantMessage appends an assistant message to messages.
func (b *ContextBuilder) AddAssistantMessage(messages []map[string]any, content string, opts AddAssistantMessageOptions) []map[string]any {
	msg := map[string]any{"role": "assistant", "content": content}
	if len(opts.ToolCalls) > 0 {
		msg["tool_calls"] = opts.ToolCalls
	}
	if opts.ReasoningContent != "" {
		msg["reasoning_content"] = opts.ReasoningContent
	}
	if len(opts.ThinkingBlocks) > 0 {
		msg["thinking_blocks"] = opts.ThinkingBlocks
	}
	return append(messages, msg)
}

// ----------------------------------------------------------------------
// Cross-runtime helpers (exoclaw._compat substitutes).
// ----------------------------------------------------------------------

func expanduser(p string) string {
	if strings.HasPrefix(p, "~") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, p[1:])
		}
	}
	return p
}

func platformSummary() string {
	system := runtime.GOOS
	if system == "darwin" {
		system = "macOS"
	}
	return fmt.Sprintf("%s %s, Go %s", system, runtime.GOARCH, runtime.Version())
}

func guessImageMIME(path string) string {
	ext := strings.ToLower(filepath.Ext(path))
	m := mime.TypeByExtension(ext)
	if !strings.HasPrefix(m, "image/") {
		return ""
	}
	return m
}
