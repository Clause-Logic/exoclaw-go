package conversationfile

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/Clause-Logic/exoclaw-go/exoclaw/providers"
)

// Ported from exoclaw_conversation/memory.py.
//
// Memory system for persistent agent memory.
//
// Two-artefact backend: MEMORY.md (long-term facts) + HISTORY.md
// (grep-searchable log). The store is *stateless with respect to
// sessions* — it summarises a list of messages and writes the artefacts.
// Boundary advancement, sidecar persistence, and "what to summarise when"
// all live in the ConsolidationPolicy.

var saveMemoryTool = []map[string]any{
	{
		"type": "function",
		"function": map[string]any{
			"name":        "save_memory",
			"description": "Save the memory consolidation result to persistent storage.",
			"parameters": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"history_entry": map[string]any{
						"type": "string",
						"description": "A paragraph (2-5 sentences) summarizing key events/decisions/topics. " +
							"Start with [YYYY-MM-DD HH:MM]. Include detail useful for grep search.",
					},
					"memory_update": map[string]any{
						"type": "string",
						"description": "Full updated long-term memory as markdown. Include all existing " +
							"facts plus new ones. Return unchanged if nothing new.",
					},
				},
				"required": []any{"history_entry", "memory_update"},
			},
		},
	},
}

// MemoryStore is the two-layer memory: MEMORY.md (long-term facts) +
// HISTORY.md (grep-searchable log).
//
// Stateless with respect to sessions. Summarize produces artefacts from a
// message list and returns the new history-log entry text; callers are
// responsible for tracking which messages have been summarised.
type MemoryStore struct {
	MemoryDir   string
	MemoryFile  string
	HistoryFile string

	provider providers.LLMProvider
	model    string
	log      *slog.Logger
}

// NewMemoryStore constructs a MemoryStore rooted at workspace/memory/.
// provider and model are required for Summarize; either may be nil to
// produce a read-only store.
func NewMemoryStore(workspace string, provider providers.LLMProvider, model string, log *slog.Logger) (*MemoryStore, error) {
	if log == nil {
		log = slog.Default()
	}
	memDir := filepath.Join(workspace, "memory")
	if err := os.MkdirAll(memDir, 0o755); err != nil {
		return nil, err
	}
	return &MemoryStore{
		MemoryDir:   memDir,
		MemoryFile:  filepath.Join(memDir, "MEMORY.md"),
		HistoryFile: filepath.Join(memDir, "HISTORY.md"),
		provider:    provider,
		model:       model,
		log:         log,
	}, nil
}

// ReadLongTerm returns the contents of MEMORY.md or "" when absent.
func (m *MemoryStore) ReadLongTerm() string {
	b, err := os.ReadFile(m.MemoryFile)
	if err != nil {
		return ""
	}
	return string(b)
}

// WriteLongTerm replaces MEMORY.md with content.
func (m *MemoryStore) WriteLongTerm(content string) error {
	return os.WriteFile(m.MemoryFile, []byte(content), 0o644)
}

// AppendHistory appends entry to HISTORY.md, followed by a blank line.
func (m *MemoryStore) AppendHistory(entry string) error {
	f, err := os.OpenFile(m.HistoryFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(strings.TrimRight(entry, " \t\r\n") + "\n\n")
	return err
}

// GetMemoryContext returns the long-term memory wrapped in a system-prompt
// section, or "" when MEMORY.md is empty.
func (m *MemoryStore) GetMemoryContext() string {
	lt := m.ReadLongTerm()
	if lt == "" {
		return ""
	}
	return "## Long-term Memory\n" + lt
}

// Summarize summarises messages via the configured LLM and persists
// MEMORY.md + HISTORY.md artefacts.
//
// Returns the new history_entry text on success — the policy uses it as
// its rolling preamble. Returns "" + nil if the provider is missing, the
// model declines to call the tool, or the call fails.
func (m *MemoryStore) Summarize(ctx context.Context, messages []map[string]any) (string, error) {
	if len(messages) == 0 {
		return "", nil
	}

	var lines []string
	for _, msg := range messages {
		content, _ := msg["content"].(string)
		if content == "" {
			continue
		}
		var toolsHint string
		if used, ok := msg["tools_used"].([]any); ok && len(used) > 0 {
			parts := make([]string, 0, len(used))
			for _, u := range used {
				if s, ok := u.(string); ok {
					parts = append(parts, s)
				}
			}
			toolsHint = " [tools: " + strings.Join(parts, ", ") + "]"
		}
		ts, _ := msg["timestamp"].(string)
		if ts == "" {
			ts = "?"
		} else if len(ts) > 16 {
			ts = ts[:16]
		}
		role, _ := msg["role"].(string)
		lines = append(lines, fmt.Sprintf("[%s] %s%s: %s", ts, strings.ToUpper(role), toolsHint, content))
	}

	currentMemory := m.ReadLongTerm()
	memSummary := currentMemory
	if memSummary == "" {
		memSummary = "(empty)"
	}
	prompt := "Process this conversation and call the save_memory tool with your consolidation.\n\n" +
		"## Current Long-term Memory\n" + memSummary + "\n\n" +
		"## Conversation to Process\n" + strings.Join(lines, "\n")

	if m.provider == nil || m.model == "" {
		m.log.Warn("memory_consolidation_skipped", "reason", "no_provider")
		return "", nil
	}

	response, err := m.provider.Chat(ctx,
		[]map[string]any{
			{"role": "system", "content": "You are a memory consolidation agent. Call the save_memory tool with your consolidation of the conversation."},
			{"role": "user", "content": prompt},
		},
		providers.ChatParams{
			Tools: saveMemoryTool,
			Model: m.model,
		},
	)
	if err != nil {
		m.log.Error("memory_consolidation_failed", "err", err)
		return "", err
	}
	if !response.HasToolCalls() {
		m.log.Warn("memory_consolidation_skipped", "reason", "no_tool_call")
		return "", nil
	}

	args := response.ToolCalls[0].Arguments
	// Some providers return args as a JSON string instead of a map.
	if s, ok := any(args).(string); ok {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(s), &parsed); err != nil {
			m.log.Warn("memory_consolidation_skipped", "reason", "args_unmarshal_failed")
			return "", nil
		}
		args = parsed
	}

	historyEntry := ""
	if entryRaw, ok := args["history_entry"]; ok {
		entry := toStringOrJSON(entryRaw)
		if entry != "" {
			if err := m.AppendHistory(entry); err != nil {
				return "", err
			}
			historyEntry = entry
		}
	}
	if updateRaw, ok := args["memory_update"]; ok {
		update := toStringOrJSON(updateRaw)
		if update != "" && update != currentMemory {
			if err := m.WriteLongTerm(update); err != nil {
				return "", err
			}
		}
	}
	m.log.Info("memory_consolidated",
		"message.summarized", len(messages),
		"history_entry.chars", len(historyEntry),
	)
	return historyEntry, nil
}

func toStringOrJSON(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}
