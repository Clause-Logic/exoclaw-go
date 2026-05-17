// Package session contains the file-backed Session and SessionManager.
//
// Ported from exoclaw_conversation/session/manager.py. Sessions are stored
// as JSONL files in the sessions directory. Only the unconsolidated tail
// of each session is loaded into RAM. Saves append new messages to the
// JSONL file rather than rewriting it.
package session

import (
	"bufio"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// RepairAndProject repairs orphan tool references and projects to
// LLM-input shape.
//
// Two cleanups, in order:
//
//   - Drop tool messages whose tool_call_id has no matching
//     assistant.tool_calls[].id earlier in the list, and strip tool_calls
//     entries with no matching tool response.
//   - Project each entry to the minimal LLM-input dict — keep role /
//     content plus the tool-pair fields, drop timestamp and any other
//     persistence-only metadata.
//
// Pure transform — no slicing, no leading-non-user peel. Callers own
// those concerns.
func RepairAndProject(messages []map[string]any) []map[string]any {
	declaredIDs := map[string]struct{}{}
	respondedIDs := map[string]struct{}{}
	for _, m := range messages {
		role, _ := m["role"].(string)
		switch role {
		case "assistant":
			if tcs, ok := m["tool_calls"].([]any); ok {
				for _, tc := range tcs {
					if tcm, ok := tc.(map[string]any); ok {
						if id, ok := tcm["id"].(string); ok && id != "" {
							declaredIDs[id] = struct{}{}
						}
					}
				}
			}
		case "tool":
			if id, ok := m["tool_call_id"].(string); ok && id != "" {
				respondedIDs[id] = struct{}{}
			}
		}
	}
	validIDs := map[string]struct{}{}
	for id := range declaredIDs {
		if _, ok := respondedIDs[id]; ok {
			validIDs[id] = struct{}{}
		}
	}

	differs := len(declaredIDs) != len(validIDs) || len(respondedIDs) != len(validIDs)
	if differs {
		repaired := make([]map[string]any, 0, len(messages))
		for _, m := range messages {
			role, _ := m["role"].(string)
			switch role {
			case "tool":
				if id, _ := m["tool_call_id"].(string); id != "" {
					if _, ok := validIDs[id]; ok {
						repaired = append(repaired, m)
					}
				}
			case "assistant":
				tcs, _ := m["tool_calls"].([]any)
				if len(tcs) > 0 {
					kept := make([]any, 0, len(tcs))
					for _, tc := range tcs {
						tcm, ok := tc.(map[string]any)
						if !ok {
							continue
						}
						id, _ := tcm["id"].(string)
						if _, ok := validIDs[id]; ok {
							kept = append(kept, tc)
						}
					}
					if len(kept) > 0 {
						merged := make(map[string]any, len(m))
						for k, v := range m {
							merged[k] = v
						}
						merged["tool_calls"] = kept
						repaired = append(repaired, merged)
					} else if content, _ := m["content"].(string); content != "" {
						stripped := make(map[string]any, len(m)-1)
						for k, v := range m {
							if k == "tool_calls" {
								continue
							}
							stripped[k] = v
						}
						repaired = append(repaired, stripped)
					}
				} else {
					repaired = append(repaired, m)
				}
			default:
				repaired = append(repaired, m)
			}
		}
		messages = repaired
	}

	out := make([]map[string]any, 0, len(messages))
	for _, m := range messages {
		entry := map[string]any{"role": m["role"], "content": ""}
		if c, ok := m["content"]; ok {
			entry["content"] = c
		}
		for _, k := range []string{"tool_calls", "tool_call_id", "name"} {
			if v, ok := m[k]; ok {
				entry[k] = v
			}
		}
		out = append(out, entry)
	}
	return out
}

// NormalizeHistory applies LLM-input cleanup to a slice of messages.
//
// Slice to maxMessages, drop leading non-user messages, then delegate to
// RepairAndProject for orphan repair + dict projection. Pass maxMessages
// < 0 for "no cap — use the full slice".
func NormalizeHistory(messages []map[string]any, maxMessages int) []map[string]any {
	sliced := messages
	if maxMessages > 0 && len(messages) > maxMessages {
		sliced = messages[len(messages)-maxMessages:]
	} else {
		sliced = append([]map[string]any{}, messages...)
	}
	for i, m := range sliced {
		if role, _ := m["role"].(string); role == "user" {
			sliced = sliced[i:]
			break
		}
	}
	return RepairAndProject(sliced)
}

// Session is a handle to an append-only message log.
//
// Messages mirrors the on-disk JSONL log when StreamingHistory=false (the
// default). Streaming-aware HistoryStore backends keep Messages empty and
// serve reads from disk via HistoryStore.Reader. The consolidation policy
// owns boundary state in its own per-session sidecar.
type Session struct {
	mu             sync.Mutex
	KeyStr         string // channel:chat_id
	MessagesSlice  []map[string]any
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Metadata       map[string]any
	totalMessages  int // explicit total; 0 = derive from messages
}

// NewSession constructs a fresh session for key.
func NewSession(key string) *Session {
	now := time.Now()
	return &Session{
		KeyStr:    key,
		CreatedAt: now,
		UpdatedAt: now,
		Metadata:  map[string]any{},
	}
}

// Key returns the session key.
func (s *Session) Key() string { return s.KeyStr }

// Messages returns the in-memory message slice.
func (s *Session) Messages() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.MessagesSlice
}

// TotalMessages returns the explicit total or len(MessagesSlice).
func (s *Session) TotalMessages() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.totalMessages > 0 {
		return s.totalMessages
	}
	return len(s.MessagesSlice)
}

// SetTotalMessages sets the explicit total — typically used by the
// streaming-history backend after counting JSONL lines.
func (s *Session) SetTotalMessages(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalMessages = n
}

// AddMessage appends a single role/content message with a timestamp and any
// extra fields. Increments the total counter.
func (s *Session) AddMessage(role, content string, extra map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	newTotal := s.totalMessages
	if newTotal == 0 {
		newTotal = len(s.MessagesSlice)
	}
	newTotal++
	msg := map[string]any{
		"role":      role,
		"content":   content,
		"timestamp": time.Now().Format(time.RFC3339),
	}
	for k, v := range extra {
		msg[k] = v
	}
	s.MessagesSlice = append(s.MessagesSlice, msg)
	s.totalMessages = newTotal
	s.UpdatedAt = time.Now()
}

// Append adds pre-built messages to the in-memory log.
func (s *Session) Append(messages []map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MessagesSlice = append(s.MessagesSlice, messages...)
	if s.totalMessages > 0 {
		s.totalMessages += len(messages)
	}
	s.UpdatedAt = time.Now()
}

// GetHistory returns the normalised tail for LLM input. maxMessages < 0
// returns the full normalised log.
func (s *Session) GetHistory(maxMessages int) []map[string]any {
	s.mu.Lock()
	msgs := append([]map[string]any{}, s.MessagesSlice...)
	s.mu.Unlock()
	if maxMessages == 0 {
		maxMessages = 500
	}
	return NormalizeHistory(msgs, maxMessages)
}

// Clear empties the session log.
func (s *Session) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MessagesSlice = nil
	s.totalMessages = 0
	s.UpdatedAt = time.Now()
}

// ----------------------------------------------------------------------
// SessionManager — JSONL-backed HistoryStore.
// ----------------------------------------------------------------------

// SessionManager manages conversation sessions stored as JSONL files.
//
// Sessions are weakly cached: GetOrCreate returns an existing in-memory
// Session while any caller holds a reference. Go has no native weak
// references, so we use a simple sync.Map with explicit Invalidate.
type SessionManager struct {
	Workspace        string
	SessionsDir      string
	StreamingHistory bool // when true, _load does not populate session.Messages

	mu    sync.RWMutex
	cache map[string]*Session
	log   *slog.Logger
}

// NewSessionManager constructs a SessionManager rooted at workspace. The
// sessions directory (workspace/sessions) is created if absent.
func NewSessionManager(workspace string, streamingHistory bool, log *slog.Logger) (*SessionManager, error) {
	if log == nil {
		log = slog.Default()
	}
	sessionsDir := filepath.Join(workspace, "sessions")
	if err := os.MkdirAll(sessionsDir, 0o755); err != nil {
		return nil, err
	}
	return &SessionManager{
		Workspace:        workspace,
		SessionsDir:      sessionsDir,
		StreamingHistory: streamingHistory,
		cache:            map[string]*Session{},
		log:              log,
	}, nil
}

func (m *SessionManager) getSessionPath(key string) string {
	safe := safeFilename(strings.ReplaceAll(key, ":", "_"))
	return filepath.Join(m.SessionsDir, safe+".jsonl")
}

// GetOrCreate returns an existing session or creates a new one. The
// in-memory cache keeps the session alive across calls; use Invalidate to
// evict.
func (m *SessionManager) GetOrCreate(key string) *Session {
	m.mu.RLock()
	if s, ok := m.cache[key]; ok {
		m.mu.RUnlock()
		return s
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.cache[key]; ok {
		return s
	}
	s := m.loadLocked(key)
	if s == nil {
		s = NewSession(key)
	}
	m.cache[key] = s
	return s
}

// loadLocked reads a session from disk. Returns nil when the file doesn't
// exist or fails to parse.
func (m *SessionManager) loadLocked(key string) *Session {
	path := m.getSessionPath(key)
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	var (
		metadata     = map[string]any{}
		createdAt    time.Time
		updatedAt    time.Time
		total        int
		buffered     []string
		scanner      = bufio.NewScanner(f)
	)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if total == 0 && strings.Contains(line, `"_type"`) {
			var data map[string]any
			if json.Unmarshal([]byte(line), &data) == nil {
				if t, _ := data["_type"].(string); t == "metadata" {
					if md, ok := data["metadata"].(map[string]any); ok {
						metadata = md
					}
					if cs, ok := data["created_at"].(string); ok {
						createdAt, _ = time.Parse(time.RFC3339, cs)
					}
					if us, ok := data["updated_at"].(string); ok {
						updatedAt, _ = time.Parse(time.RFC3339, us)
					}
					continue
				}
			}
		}
		if !m.StreamingHistory {
			buffered = append(buffered, line)
		}
		total++
	}
	if err := scanner.Err(); err != nil {
		m.log.Warn("session_load_failed", "session.key", key, "err", err)
		return nil
	}

	messages := make([]map[string]any, 0, len(buffered))
	for _, line := range buffered {
		var msg map[string]any
		if err := json.Unmarshal([]byte(line), &msg); err == nil {
			messages = append(messages, msg)
		}
	}
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	if updatedAt.IsZero() {
		updatedAt = createdAt
	}
	s := &Session{
		KeyStr:        key,
		MessagesSlice: messages,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
		Metadata:      metadata,
	}
	s.totalMessages = total
	return s
}

// ReadHistory reads the message log from disk and applies LLM-input
// cleanup. maxMessages < 0 returns the full normalised log.
func (m *SessionManager) ReadHistory(key string, maxMessages int) []map[string]any {
	var tail []map[string]any
	if m.StreamingHistory {
		tail, _ = m.LoadRange(key, 0, 1<<30)
	} else {
		s := m.GetOrCreate(key)
		tail = s.Messages()
	}
	return NormalizeHistory(tail, maxMessages)
}

// LoadRange loads a range of messages from disk by index.
//
// Useful for consolidation which needs to read messages in
// [last_consolidated : -keep_count] without holding them all in RAM.
func (m *SessionManager) LoadRange(key string, start, end int) ([]map[string]any, error) {
	path := m.getSessionPath(key)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var (
		messages []map[string]any
		idx      int
		scanner  = bufio.NewScanner(f)
	)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.Contains(line, `"_type"`) {
			var data map[string]any
			if json.Unmarshal([]byte(line), &data) == nil {
				if t, _ := data["_type"].(string); t == "metadata" {
					continue
				}
			}
		}
		if idx >= end {
			break
		}
		if idx >= start {
			var msg map[string]any
			if err := json.Unmarshal([]byte(line), &msg); err == nil {
				messages = append(messages, msg)
			}
		}
		idx++
	}
	return messages, scanner.Err()
}

// Save rewrites the full session to disk.
//
// Used after Clear or any operation that restructures the file. For
// normal turn recording, prefer SaveAppend.
//
// Under StreamingHistory the in-memory MessagesSlice is empty, so we
// re-read from disk before rewriting to preserve the persisted log.
func (m *SessionManager) Save(s *Session) error {
	path := m.getSessionPath(s.Key())

	var msgs []map[string]any
	if m.StreamingHistory && s.TotalMessages() > 0 {
		var err error
		msgs, err = m.LoadRange(s.Key(), 0, s.TotalMessages())
		if err != nil {
			return err
		}
	} else {
		msgs = s.Messages()
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	if err := enc.Encode(map[string]any{
		"_type":      "metadata",
		"key":        s.Key(),
		"created_at": s.CreatedAt.Format(time.RFC3339),
		"updated_at": s.UpdatedAt.Format(time.RFC3339),
		"metadata":   s.Metadata,
	}); err != nil {
		return err
	}
	for _, msg := range msgs {
		if err := enc.Encode(msg); err != nil {
			return err
		}
	}
	return nil
}

// SaveAppend appends new messages to the JSONL file. O(newMessages) — does
// not read or rewrite existing content. Creates the file with a metadata
// header if absent.
func (m *SessionManager) SaveAppend(s *Session, newMessages []map[string]any) error {
	path := m.getSessionPath(s.Key())
	_, statErr := os.Stat(path)
	exists := statErr == nil

	flag := os.O_APPEND | os.O_WRONLY
	if !exists {
		flag = os.O_CREATE | os.O_WRONLY
	}
	f, err := os.OpenFile(path, flag, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	if !exists {
		if err := enc.Encode(map[string]any{
			"_type":      "metadata",
			"key":        s.Key(),
			"created_at": s.CreatedAt.Format(time.RFC3339),
			"updated_at": s.UpdatedAt.Format(time.RFC3339),
			"metadata":   s.Metadata,
		}); err != nil {
			return err
		}
	}
	for _, msg := range newMessages {
		if err := enc.Encode(msg); err != nil {
			return err
		}
	}
	return nil
}

// Invalidate removes a session from the cache.
func (m *SessionManager) Invalidate(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.cache, key)
}

// ListSessions lists all sessions on disk, sorted newest-first by
// updated_at.
func (m *SessionManager) ListSessions() []map[string]any {
	entries, err := os.ReadDir(m.SessionsDir)
	if err != nil {
		return nil
	}
	var sessions []map[string]any
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		path := filepath.Join(m.SessionsDir, e.Name())
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		var data map[string]any
		if scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				_ = json.Unmarshal([]byte(line), &data)
			}
		}
		f.Close()
		if t, _ := data["_type"].(string); t != "metadata" {
			continue
		}
		key, _ := data["key"].(string)
		if key == "" {
			stem := strings.TrimSuffix(e.Name(), ".jsonl")
			key = strings.Replace(stem, "_", ":", 1)
		}
		sessions = append(sessions, map[string]any{
			"key":        key,
			"created_at": data["created_at"],
			"updated_at": data["updated_at"],
			"path":       path,
		})
	}
	sort.SliceStable(sessions, func(i, j int) bool {
		a, _ := sessions[i]["updated_at"].(string)
		b, _ := sessions[j]["updated_at"].(string)
		return a > b
	})
	return sessions
}

// Reader returns a streaming reader backed by the on-disk JSONL log.
func (m *SessionManager) Reader(key string) *JSONLSessionReader {
	return &JSONLSessionReader{manager: m, key: key}
}

// ----------------------------------------------------------------------
// JSONLSessionReader — streaming reader over a JSONL session log.
// ----------------------------------------------------------------------

// JSONLSessionReader streams messages line-by-line from disk. Never holds
// the full log in RAM. Stream is restartable: each call reopens the file.
type JSONLSessionReader struct {
	manager *SessionManager
	key     string
}

// Key returns the session key.
func (r *JSONLSessionReader) Key() string { return r.key }

// Count returns the cached/persisted total_messages when the session is
// live in cache, falling back to a file scan otherwise.
func (r *JSONLSessionReader) Count() (int, error) {
	r.manager.mu.RLock()
	if s, ok := r.manager.cache[r.key]; ok {
		r.manager.mu.RUnlock()
		return s.TotalMessages(), nil
	}
	r.manager.mu.RUnlock()

	path := r.manager.getSessionPath(r.key)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	total := 0
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if total == 0 && strings.Contains(line, `"_type"`) {
			var data map[string]any
			if json.Unmarshal([]byte(line), &data) == nil {
				if t, _ := data["_type"].(string); t == "metadata" {
					continue
				}
			}
		}
		total++
	}
	return total, scanner.Err()
}

// Stream yields messages in [start, end). end < 0 means "to the tail".
// The returned channel is closed when iteration completes.
type StreamItem struct {
	Message map[string]any
	Err     error
}

func (r *JSONLSessionReader) Stream(start, end int) <-chan StreamItem {
	out := make(chan StreamItem)
	go func() {
		defer close(out)
		stop := end
		if stop < 0 {
			stop = 1 << 30
		}
		path := r.manager.getSessionPath(r.key)
		f, err := os.Open(path)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				out <- StreamItem{Err: err}
			}
			return
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
		idx := 0
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			if strings.Contains(line, `"_type"`) {
				var data map[string]any
				if json.Unmarshal([]byte(line), &data) == nil {
					if t, _ := data["_type"].(string); t == "metadata" {
						continue
					}
				}
			}
			if idx >= stop {
				break
			}
			if idx >= start {
				var msg map[string]any
				if err := json.Unmarshal([]byte(line), &msg); err == nil {
					out <- StreamItem{Message: msg}
				}
			}
			idx++
		}
		if err := scanner.Err(); err != nil {
			out <- StreamItem{Err: err}
		}
	}()
	return out
}

// At does a random-access read of one message via Stream.
func (r *JSONLSessionReader) At(index int) (map[string]any, error) {
	for item := range r.Stream(index, index+1) {
		if item.Err != nil {
			return nil, item.Err
		}
		return item.Message, nil
	}
	return nil, nil
}

// ----------------------------------------------------------------------
// safeFilename (local copy of helpers.SafeFilename to avoid an import
// cycle through the parent package).
// ----------------------------------------------------------------------

func safeFilename(name string) string {
	out := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch c {
		case '<', '>', ':', '"', '/', '\\', '|', '?', '*':
			out = append(out, '_')
		default:
			out = append(out, c)
		}
	}
	return strings.TrimSpace(string(out))
}
