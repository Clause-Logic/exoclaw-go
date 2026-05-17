package cron

import _ "embed"

// Ported from exoclaw_tools_cron/skills.py.

//go:embed SKILL.md
var skillBody string

// Cron returns the cron scheduling skill payload — {name, content} —
// for agent context injection. The Python original also exposes a
// `path` field for skills with auxiliary files (hooks, scripts); the
// Go port embeds SKILL.md via //go:embed so there is no auxiliary
// directory to reference. Callers wire this into the conversation
// loader's SkillPackages map.
func Cron() map[string]string {
	return map[string]string{
		"name":    "cron",
		"content": skillBody,
	}
}
