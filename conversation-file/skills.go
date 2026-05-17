package conversationfile

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// Ported from exoclaw_conversation/skills.py.

// splitFrontmatter splits a YAML/JSON frontmatter header off the front of
// content. Returns ("", content, false) when no valid frontmatter is
// present.
//
// Frontmatter shape:
//
//	---
//	...
//	---
//	body...
func splitFrontmatter(content string) (frontmatter, body string, present bool) {
	if !strings.HasPrefix(content, "---\n") {
		return "", content, false
	}
	end := strings.Index(content[4:], "\n---")
	if end == -1 {
		return "", content, false
	}
	frontmatter = content[4 : 4+end]
	restStart := 4 + end + len("\n---")
	if restStart < len(content) && content[restStart] == '\n' {
		restStart++
	}
	return frontmatter, content[restStart:], true
}

// LoadSkillToolDef is the standard load_skill tool definition. Consumers
// can include this in their tool list so the agent can dynamically
// activate skills listed in <skills>.
var LoadSkillToolDef = map[string]any{
	"type": "function",
	"function": map[string]any{
		"name": "load_skill",
		"description": "Load a skill by name. Returns the skill instructions and " +
			"activates its tools for use in this conversation. Call this " +
			"when a skill from <skills> is relevant to the user's request.",
		"parameters": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "The skill name from the <skills> summary.",
				},
			},
			"required": []any{"name"},
		},
	},
}

// LoadSkillResult is the result of activating a skill via
// SkillsLoader.ActivateSkill.
type LoadSkillResult struct {
	Content   string
	ToolNames []string
}

// AgentHook is an agent hook defined by a markdown file in a skill's hooks
// directory. Agent hooks are .md files at
// skills/{name}/hooks/exoclaw/{hook_name}.md. The markdown content is the
// prompt for a fire-and-forget agent turn that reacts to the lifecycle
// event. Frontmatter controls which tools / skills the hook turn has
// access to.
type AgentHook struct {
	SkillName string
	HookName  string
	Prompt    string
	Tools     []string
	Skills    []string
}

// SkillsLoader loads markdown skills (SKILL.md) from several sources:
// workspace skills directory, optional builtin directory, and registered
// package-loaded skills.
type SkillsLoader struct {
	Workspace         string
	WorkspaceSkills   string
	BuiltinSkillsDir  string
	packageSkills     map[string]string
	packageSkillDirs  map[string]string
	allowed           map[string]struct{} // nil = no allow-list
	hasAllowed        bool
}

// SkillsLoaderOptions bundles optional inputs.
type SkillsLoaderOptions struct {
	BuiltinSkillsDir string
	// PackageSkills is a map of name → SKILL.md content for skills
	// registered by the host application (Go equivalent of Python's
	// importlib.metadata entry-point discovery, which doesn't exist
	// at runtime in Go).
	PackageSkills map[string]string
	// PackageSkillDirs is a map of name → directory containing
	// hooks/SKILL.md/etc.
	PackageSkillDirs map[string]string
	// AllowedNames is the optional allow-list. nil leaves every
	// discovered skill visible; a non-nil (even empty) slice means
	// "only these are allowed".
	AllowedNames []string
}

// NewSkillsLoader constructs a SkillsLoader.
func NewSkillsLoader(workspace string, opts SkillsLoaderOptions) *SkillsLoader {
	l := &SkillsLoader{
		Workspace:        workspace,
		WorkspaceSkills:  filepath.Join(workspace, "skills"),
		BuiltinSkillsDir: opts.BuiltinSkillsDir,
		packageSkills:    map[string]string{},
		packageSkillDirs: map[string]string{},
	}
	for k, v := range opts.PackageSkills {
		l.packageSkills[k] = v
	}
	for k, v := range opts.PackageSkillDirs {
		l.packageSkillDirs[k] = v
	}
	if opts.AllowedNames != nil {
		l.allowed = make(map[string]struct{}, len(opts.AllowedNames))
		for _, n := range opts.AllowedNames {
			l.allowed[n] = struct{}{}
		}
		l.hasAllowed = true
	}
	return l
}

func (l *SkillsLoader) isAllowed(name string) bool {
	if !l.hasAllowed {
		return true
	}
	_, ok := l.allowed[name]
	return ok
}

// allSkillDirs returns all skill directories as (dir, source) pairs for
// hook scanning. Honors allowedNames — disallowed skills are excluded.
func (l *SkillsLoader) allSkillDirs() []skillDirEntry {
	var dirs []skillDirEntry
	if entries, err := os.ReadDir(l.WorkspaceSkills); err == nil {
		for _, e := range entries {
			if e.IsDir() && l.isAllowed(e.Name()) {
				dirs = append(dirs, skillDirEntry{path: filepath.Join(l.WorkspaceSkills, e.Name()), source: "workspace"})
			}
		}
	}
	if l.BuiltinSkillsDir != "" {
		if entries, err := os.ReadDir(l.BuiltinSkillsDir); err == nil {
			for _, e := range entries {
				if e.IsDir() && l.isAllowed(e.Name()) {
					dirs = append(dirs, skillDirEntry{path: filepath.Join(l.BuiltinSkillsDir, e.Name()), source: "builtin"})
				}
			}
		}
	}
	for name, path := range l.packageSkillDirs {
		if info, err := os.Stat(path); err == nil && info.IsDir() && l.isAllowed(name) {
			dirs = append(dirs, skillDirEntry{path: path, source: "package"})
		}
	}
	return dirs
}

type skillDirEntry struct {
	path   string
	source string
}

// ListSkills lists all available skills.
//
// Honors allowedNames — disallowed skills are omitted entirely so callers
// cannot reference them.
//
// filterUnavailable=true filters out skills with unmet requirements.
func (l *SkillsLoader) ListSkills(filterUnavailable bool) []map[string]string {
	var skills []map[string]string
	seen := map[string]struct{}{}
	add := func(name, path, source string) {
		if _, dup := seen[name]; dup {
			return
		}
		seen[name] = struct{}{}
		skills = append(skills, map[string]string{"name": name, "path": path, "source": source})
	}

	if entries, err := os.ReadDir(l.WorkspaceSkills); err == nil {
		for _, e := range entries {
			if !e.IsDir() || !l.isAllowed(e.Name()) {
				continue
			}
			skillFile := filepath.Join(l.WorkspaceSkills, e.Name(), "SKILL.md")
			if _, err := os.Stat(skillFile); err == nil {
				add(e.Name(), skillFile, "workspace")
			}
		}
	}
	if l.BuiltinSkillsDir != "" {
		if entries, err := os.ReadDir(l.BuiltinSkillsDir); err == nil {
			for _, e := range entries {
				if !e.IsDir() || !l.isAllowed(e.Name()) {
					continue
				}
				skillFile := filepath.Join(l.BuiltinSkillsDir, e.Name(), "SKILL.md")
				if _, err := os.Stat(skillFile); err == nil {
					add(e.Name(), skillFile, "builtin")
				}
			}
		}
	}
	for name := range l.packageSkills {
		if !l.isAllowed(name) {
			continue
		}
		path := "package:" + name
		if dir, ok := l.packageSkillDirs[name]; ok {
			path = filepath.Join(dir, "SKILL.md")
		}
		add(name, path, "package")
	}

	if !filterUnavailable {
		return skills
	}
	var filtered []map[string]string
	for _, s := range skills {
		if l.checkRequirements(l.getSkillMeta(s["name"])) {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

// LoadSkill loads a skill by name. Returns "" when not found / not allowed.
func (l *SkillsLoader) LoadSkill(name string) string {
	if !l.isAllowed(name) {
		return ""
	}
	wp := filepath.Join(l.WorkspaceSkills, name, "SKILL.md")
	if data, err := os.ReadFile(wp); err == nil {
		return string(data)
	}
	if l.BuiltinSkillsDir != "" {
		bp := filepath.Join(l.BuiltinSkillsDir, name, "SKILL.md")
		if data, err := os.ReadFile(bp); err == nil {
			return string(data)
		}
	}
	if content, ok := l.packageSkills[name]; ok {
		return content
	}
	return ""
}

// ActivateSkill loads a skill's content and resolves its declared tool names.
//
// This is the handler behind the load_skill tool. Consumers should call
// this when the LLM invokes load_skill and then merge the returned
// ToolNames into the active optional tools set.
func (l *SkillsLoader) ActivateSkill(name string) LoadSkillResult {
	content := l.LoadSkill(name)
	if content == "" {
		return LoadSkillResult{Content: "Skill '" + name + "' not found."}
	}
	toolSet := l.GetToolsForSkills([]string{name})
	tools := make([]string, 0, len(toolSet))
	for t := range toolSet {
		tools = append(tools, t)
	}
	sort.Strings(tools)
	return LoadSkillResult{Content: content, ToolNames: tools}
}

// LoadSkillsForContext returns the concatenated bodies of skillNames
// (frontmatter stripped) joined with separators.
func (l *SkillsLoader) LoadSkillsForContext(skillNames []string) string {
	var parts []string
	for _, name := range skillNames {
		content := l.LoadSkill(name)
		if content == "" {
			continue
		}
		body := l.stripFrontmatter(content)
		parts = append(parts, "### Skill: "+name+"\n\n"+body)
	}
	return strings.Join(parts, "\n\n---\n\n")
}

// BuildSkillsSummary builds an XML summary of skills (name, description,
// path, availability). `only`, when non-nil, restricts the summary to
// skills whose name is in the set.
func (l *SkillsLoader) BuildSkillsSummary() string {
	return l.BuildSkillsSummaryWithFilter(nil)
}

// BuildSkillsSummaryWithFilter is the variant that accepts a name filter.
func (l *SkillsLoader) BuildSkillsSummaryWithFilter(only map[string]struct{}) string {
	all := l.ListSkills(false)
	if only != nil {
		var filtered []map[string]string
		for _, s := range all {
			if _, ok := only[s["name"]]; ok {
				filtered = append(filtered, s)
			}
		}
		all = filtered
	}
	if len(all) == 0 {
		return ""
	}

	lines := []string{"<skills>"}
	for _, s := range all {
		name := escapeXML(s["name"])
		path := s["path"]
		desc := escapeXML(l.getSkillDescription(s["name"]))
		meta := l.getSkillMeta(s["name"])
		available := l.checkRequirements(meta)
		availStr := "false"
		if available {
			availStr = "true"
		}
		lines = append(lines, `  <skill available="`+availStr+`">`)
		lines = append(lines, "    <name>"+name+"</name>")
		lines = append(lines, "    <description>"+desc+"</description>")
		lines = append(lines, "    <location>"+path+"</location>")
		if !available {
			if missing := l.getMissingRequirements(meta); missing != "" {
				lines = append(lines, "    <requires>"+escapeXML(missing)+"</requires>")
			}
		}
		lines = append(lines, "  </skill>")
	}
	lines = append(lines, "</skills>")
	return strings.Join(lines, "\n")
}

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

func (l *SkillsLoader) getMissingRequirements(meta map[string]any) string {
	var missing []string
	requires, _ := meta["requires"].(map[string]any)
	for _, b := range listOfStrings(requires["bins"]) {
		if _, err := exec.LookPath(b); err != nil {
			missing = append(missing, "CLI: "+b)
		}
	}
	for _, env := range listOfStrings(requires["env"]) {
		if os.Getenv(env) == "" {
			missing = append(missing, "ENV: "+env)
		}
	}
	return strings.Join(missing, ", ")
}

func listOfStrings(v any) []string {
	if v == nil {
		return nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func (l *SkillsLoader) getSkillDescription(name string) string {
	meta := l.GetSkillMetadata(name)
	if meta == nil {
		return name
	}
	if d, ok := meta["description"].(string); ok && d != "" {
		return d
	}
	return name
}

func (l *SkillsLoader) stripFrontmatter(content string) string {
	_, body, ok := splitFrontmatter(content)
	if !ok {
		return content
	}
	return strings.TrimSpace(body)
}

// parseExoclawMetadata parses skill metadata JSON from the frontmatter's
// metadata: field. Supports exoclaw, nanobot, and openclaw keys.
func (l *SkillsLoader) parseExoclawMetadata(raw string) map[string]any {
	if raw == "" {
		return map[string]any{}
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return map[string]any{}
	}
	for _, k := range []string{"exoclaw", "nanobot", "openclaw"} {
		if v, ok := data[k].(map[string]any); ok {
			return v
		}
	}
	return map[string]any{}
}

func (l *SkillsLoader) checkRequirements(meta map[string]any) bool {
	requires, _ := meta["requires"].(map[string]any)
	for _, b := range listOfStrings(requires["bins"]) {
		if _, err := exec.LookPath(b); err != nil {
			return false
		}
	}
	for _, env := range listOfStrings(requires["env"]) {
		if os.Getenv(env) == "" {
			return false
		}
	}
	return true
}

func (l *SkillsLoader) getSkillMeta(name string) map[string]any {
	meta := l.GetSkillMetadata(name)
	if meta == nil {
		return map[string]any{}
	}
	raw, _ := meta["metadata"].(string)
	return l.parseExoclawMetadata(raw)
}

// GetBootstrapInjections returns content from hooks/exoclaw/bootstrap.md
// (or hooks/nanobot/bootstrap.md) for all installed skills.
func (l *SkillsLoader) GetBootstrapInjections() []string {
	var results []string
	seen := map[string]struct{}{}
	for _, sd := range l.allSkillDirs() {
		name := filepath.Base(sd.path)
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		for _, sub := range []string{"exoclaw", "nanobot"} {
			hp := filepath.Join(sd.path, "hooks", sub, "bootstrap.md")
			if data, err := os.ReadFile(hp); err == nil {
				content := strings.TrimSpace(string(data))
				if content != "" {
					results = append(results, content)
				}
				break
			}
		}
	}
	return results
}

// GetSkillHookScripts returns paths to executable hook scripts named
// hookName across all installed skills.
func (l *SkillsLoader) GetSkillHookScripts(hookName string) []string {
	var results []string
	seen := map[string]struct{}{}
	dirs := l.allSkillDirs()
	sort.Slice(dirs, func(i, j int) bool { return filepath.Base(dirs[i].path) < filepath.Base(dirs[j].path) })
	for _, sd := range dirs {
		name := filepath.Base(sd.path)
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		for _, sub := range []string{"exoclaw", "nanobot"} {
			hp := filepath.Join(sd.path, "hooks", sub, hookName)
			if info, err := os.Stat(hp); err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
				results = append(results, hp)
				break
			}
		}
	}
	return results
}

// GetAgentHooks returns agent hooks for a lifecycle event across all
// installed skills.
//
// Agent hooks are .md files at hooks/exoclaw/{hook_name}.md inside a
// skill directory. Each file becomes a fire-and-forget agent turn when
// the event fires. Frontmatter `tools:` and `skills:` control what the
// hook turn has access to (empty = inherit from parent).
func (l *SkillsLoader) GetAgentHooks(hookName string) []AgentHook {
	var results []AgentHook
	seen := map[string]struct{}{}
	for _, sd := range l.allSkillDirs() {
		name := filepath.Base(sd.path)
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		for _, sub := range []string{"exoclaw", "nanobot"} {
			hp := filepath.Join(sd.path, "hooks", sub, hookName+".md")
			data, err := os.ReadFile(hp)
			if err != nil {
				continue
			}
			raw := strings.TrimSpace(string(data))
			if raw == "" {
				break
			}
			var tools, skills []string
			prompt := raw
			if fm, body, ok := splitFrontmatter(raw); ok {
				prompt = strings.TrimSpace(body)
				for _, line := range strings.Split(fm, "\n") {
					if idx := strings.Index(line, ":"); idx > 0 {
						key := strings.TrimSpace(line[:idx])
						value := line[idx+1:]
						switch key {
						case "tools":
							tools = splitAndTrim(value, ",")
						case "skills":
							skills = splitAndTrim(value, ",")
						}
					}
				}
			}
			results = append(results, AgentHook{
				SkillName: name,
				HookName:  hookName,
				Prompt:    prompt,
				Tools:     tools,
				Skills:    skills,
			})
			break
		}
	}
	return results
}

func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// GetToolsForSkills returns the union of tool names declared by the given
// skills.
//
// Skills list required optional tools in their frontmatter:
//
//	tools: mcp_sentry_get_issues, mcp_sentry_resolve_issue
//
// The value is a comma-separated list of tool names.
func (l *SkillsLoader) GetToolsForSkills(skillNames []string) map[string]struct{} {
	result := map[string]struct{}{}
	for _, name := range skillNames {
		meta := l.GetSkillMetadata(name)
		if meta == nil {
			continue
		}
		raw, _ := meta["tools"].(string)
		for _, t := range splitAndTrim(raw, ",") {
			result[t] = struct{}{}
		}
	}
	return result
}

// GetAlwaysSkills returns skills marked as always=true that meet
// requirements.
func (l *SkillsLoader) GetAlwaysSkills() []string {
	var result []string
	for _, s := range l.ListSkills(true) {
		meta := l.GetSkillMetadata(s["name"])
		if meta == nil {
			continue
		}
		skillMeta := l.parseExoclawMetadata(asString(meta["metadata"]))
		if isTruthy(skillMeta["always"]) || isTruthy(meta["always"]) {
			result = append(result, s["name"])
		}
	}
	return result
}

func asString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func isTruthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		switch strings.ToLower(x) {
		case "true", "1", "yes":
			return true
		}
	}
	return false
}

// GetSkillMetadata parses the frontmatter of a skill's SKILL.md and
// returns it as a map. Returns nil when the skill doesn't exist or has
// no frontmatter.
func (l *SkillsLoader) GetSkillMetadata(name string) map[string]any {
	content := l.LoadSkill(name)
	if content == "" {
		return nil
	}
	fm, _, ok := splitFrontmatter(content)
	if !ok {
		return nil
	}
	meta := map[string]any{}
	for _, line := range strings.Split(fm, "\n") {
		if idx := strings.Index(line, ":"); idx > 0 {
			key := strings.TrimSpace(line[:idx])
			value := strings.TrimSpace(line[idx+1:])
			value = strings.Trim(value, "\"'")
			meta[key] = value
		}
	}
	return meta
}
