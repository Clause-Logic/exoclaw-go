package workspace

import _ "embed"

// Ported from exoclaw_tools_workspace/skills.py.
//
// The Python original is an `entry_points["exoclaw.skills"]` callable;
// Go has no entry-point discovery, so we expose Workspace() as a plain
// function. Callers wire it into the conversation's SkillPackages map at
// construction time.

//go:embed SKILL.md
var skillBody string

// Workspace returns the file-tools skill payload — {name, content} — for
// agent context injection. Mirrors the Python `workspace()` function:
// content-only (no path), so the conversation loader writes just
// SKILL.md into context without copying neighbouring source files.
func Workspace() map[string]string {
	return map[string]string{
		"name":    "workspace",
		"content": skillBody,
	}
}
