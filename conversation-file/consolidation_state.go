package conversationfile

import (
	"bufio"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Ported from exoclaw_conversation/_consolidation_state.py.
//
// Sidecar state for ConsolidationPolicy implementations.
//
// Each policy instance gets its own JSON sidecar file next to the session
// log: <state_dir>/<safe_key>.consolidation.json. The sidecar carries the
// policy's view-rebuilding state — what's been summarised, the preamble
// text, running token estimates — so the append-only session log itself
// stays untouched by consolidation.

const sidecarVersion = "summarizing/v1"

// ConsolidationState is the in-memory representation of a policy sidecar.
//
// Plain attribute container — no validation. Loaders and the policy own
// consistency. Serialised as JSON; format is forward-compatible via the
// policy version tag.
//
//   - SummarizedThrough: absolute index in the session log up to which
//     messages have been folded into Summary. The policy emits log entries
//     from this index onwards as the unconsolidated tail.
//   - Summary: rolling preamble text emitted before the tail, or empty
//     if nothing has been summarised yet.
//   - UnconsolidatedTokenEstimate: running estimate of tokens in the
//     post-summary tail. Maintained incrementally by OnTurnComplete. Lets
//     Transform(budget > 0) make O(1) decisions about whether further
//     compaction is needed.
//   - LastUpdated: ISO timestamp of the last sidecar write.
type ConsolidationState struct {
	SummarizedThrough           int
	Summary                     string
	UnconsolidatedTokenEstimate int
	LastUpdated                 string
}

// jsonShape mirrors the on-disk format. Kept private so callers go through
// (ConsolidationState).ToMap / FromMap and never have to think about JSON
// field names.
type sidecarShape struct {
	Policy                      string `json:"policy"`
	SummarizedThrough           int    `json:"summarized_through"`
	Summary                     string `json:"summary"`
	UnconsolidatedTokenEstimate int    `json:"unconsolidated_token_estimate"`
	LastUpdated                 string `json:"last_updated,omitempty"`
}

// ToMap returns the wire-format map (for callers that need to inspect or
// reserialise outside this package).
func (s *ConsolidationState) ToMap() map[string]any {
	return map[string]any{
		"policy":                        sidecarVersion,
		"summarized_through":            s.SummarizedThrough,
		"summary":                       s.Summary,
		"unconsolidated_token_estimate": s.UnconsolidatedTokenEstimate,
		"last_updated":                  s.LastUpdated,
	}
}

// FromMap builds a ConsolidationState from a wire-format map. Missing or
// malformed fields fall back to zero values (matching Python's defensive
// loader).
func ConsolidationStateFromMap(m map[string]any) *ConsolidationState {
	s := &ConsolidationState{}
	if v, ok := m["summarized_through"].(float64); ok {
		s.SummarizedThrough = int(v)
	}
	if v, ok := m["summarized_through"].(int); ok {
		s.SummarizedThrough = v
	}
	if v, ok := m["summary"].(string); ok {
		s.Summary = v
	}
	if v, ok := m["unconsolidated_token_estimate"].(float64); ok {
		s.UnconsolidatedTokenEstimate = int(v)
	}
	if v, ok := m["unconsolidated_token_estimate"].(int); ok {
		s.UnconsolidatedTokenEstimate = v
	}
	if v, ok := m["last_updated"].(string); ok {
		s.LastUpdated = v
	}
	return s
}

// SidecarPath computes the sidecar path for a session key. Mirrors
// SessionManager.getSessionPath so sidecars sit next to their session
// JSONL when stateDir is the sessions dir.
func SidecarPath(stateDir, key string) string {
	safeKey := SafeFilename(strings.ReplaceAll(key, ":", "_"))
	return filepath.Join(stateDir, safeKey+".consolidation.json")
}

// legacySessionPath is the path the JSONL session file would live at, if
// stateDir is the sessions directory (the recommended layout). Used only
// by the migration shim in LoadState.
func legacySessionPath(stateDir, key string) string {
	safeKey := SafeFilename(strings.ReplaceAll(key, ":", "_"))
	return filepath.Join(stateDir, safeKey+".jsonl")
}

// readLegacyBoundary peeks at a session JSONL's metadata header and
// returns (lastConsolidated, summary). Returns (0, "") if the file
// doesn't exist, lacks a metadata line, or fails to parse.
func readLegacyBoundary(jsonlPath string) (int, string) {
	f, err := os.Open(jsonlPath)
	if err != nil {
		return 0, ""
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	if !scanner.Scan() {
		return 0, ""
	}
	line := strings.TrimSpace(scanner.Text())
	if line == "" || !strings.Contains(line, `"_type"`) {
		return 0, ""
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(line), &data); err != nil {
		return 0, ""
	}
	if t, _ := data["_type"].(string); t != "metadata" {
		return 0, ""
	}
	lastCons := 0
	switch v := data["last_consolidated"].(type) {
	case float64:
		lastCons = int(v)
	case int:
		lastCons = v
	}
	summary := ""
	if meta, ok := data["metadata"].(map[string]any); ok {
		if s, ok := meta["summary"].(string); ok {
			summary = s
		}
	}
	return lastCons, summary
}

// LoadState loads the policy sidecar for key.
//
// Migration: if the sidecar does not exist and a legacy session JSONL
// sits next to it with a non-zero last_consolidated (or a
// metadata.summary), seed a fresh sidecar from those values and persist
// it. After this seeding, last_consolidated is never read again — the
// sidecar owns the boundary.
func LoadState(stateDir, key string, log *slog.Logger) *ConsolidationState {
	if log == nil {
		log = slog.Default()
	}
	path := SidecarPath(stateDir, key)
	if data, err := os.ReadFile(path); err == nil {
		var m map[string]any
		if jerr := json.Unmarshal(data, &m); jerr == nil {
			return ConsolidationStateFromMap(m)
		}
		log.Warn("consolidation_sidecar_load_failed", "sidecar.path", path)
		return &ConsolidationState{}
	} else if !errors.Is(err, os.ErrNotExist) {
		log.Warn("consolidation_sidecar_load_failed", "sidecar.path", path, "err", err)
	}

	lastCons, summary := readLegacyBoundary(legacySessionPath(stateDir, key))
	state := &ConsolidationState{
		SummarizedThrough: lastCons,
		Summary:           summary,
	}
	if lastCons > 0 || summary != "" {
		if err := SaveState(stateDir, key, state); err == nil {
			log.Info("consolidation_sidecar_migrated",
				"session.key", key,
				"sidecar.summarized_through", lastCons,
				"sidecar.has_summary", summary != "",
			)
		} else {
			log.Warn("consolidation_sidecar_migrate_failed", "session.key", key, "err", err)
		}
	}
	return state
}

// SaveState writes the sidecar atomically (write-then-rename).
func SaveState(stateDir, key string, state *ConsolidationState) error {
	if _, err := EnsureDir(stateDir); err != nil {
		return err
	}
	state.LastUpdated = time.Now().Format(time.RFC3339)
	shape := sidecarShape{
		Policy:                      sidecarVersion,
		SummarizedThrough:           state.SummarizedThrough,
		Summary:                     state.Summary,
		UnconsolidatedTokenEstimate: state.UnconsolidatedTokenEstimate,
		LastUpdated:                 state.LastUpdated,
	}
	body, err := json.MarshalIndent(shape, "", "  ")
	if err != nil {
		return err
	}
	path := SidecarPath(stateDir, key)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// DeleteState removes the sidecar — used when a session is cleared or
// deleted. No-op when the sidecar doesn't exist.
func DeleteState(stateDir, key string, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	path := SidecarPath(stateDir, key)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Warn("consolidation_sidecar_delete_failed", "session.key", key, "err", err)
	}
}
