package conversationfile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Ported from tests/test_skills_allow_list.py.
//
// Verifies allowed_names is enforced uniformly: ListSkills, LoadSkill,
// ActivateSkill, BuildSkillsSummary, and hook discovery should all
// pretend disallowed skills don't exist.

func writeSkill(t *testing.T, root, name, body string) {
	t.Helper()
	dir := filepath.Join(root, "skills", name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "SKILL.md"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestAllowList_ListSkillsFiltersDisallowed(t *testing.T) {
	tmp := t.TempDir()
	writeSkill(t, tmp, "alpha", "alpha body")
	writeSkill(t, tmp, "bravo", "bravo body")
	l := NewSkillsLoader(tmp, SkillsLoaderOptions{AllowedNames: []string{"alpha"}})
	got := l.ListSkills(false)
	if len(got) != 1 || got[0]["name"] != "alpha" {
		t.Fatalf("got %v", got)
	}
}

func TestAllowList_LoadSkillReturnsEmptyForDisallowed(t *testing.T) {
	tmp := t.TempDir()
	writeSkill(t, tmp, "alpha", "alpha body")
	writeSkill(t, tmp, "bravo", "bravo body")
	l := NewSkillsLoader(tmp, SkillsLoaderOptions{AllowedNames: []string{"alpha"}})
	if l.LoadSkill("bravo") != "" {
		t.Fatal("disallowed skill returned content")
	}
	if !strings.Contains(l.LoadSkill("alpha"), "alpha body") {
		t.Fatal("allowed skill missing")
	}
}

func TestAllowList_ActivateSkillRefusesDisallowed(t *testing.T) {
	tmp := t.TempDir()
	writeSkill(t, tmp, "alpha", "alpha body")
	writeSkill(t, tmp, "bravo", "bravo body")
	l := NewSkillsLoader(tmp, SkillsLoaderOptions{AllowedNames: []string{"alpha"}})
	res := l.ActivateSkill("bravo")
	if !strings.Contains(res.Content, "not found") {
		t.Fatalf("got %q", res.Content)
	}
	res = l.ActivateSkill("alpha")
	if !strings.Contains(res.Content, "alpha body") {
		t.Fatalf("got %q", res.Content)
	}
}

func TestAllowList_SummaryOmitsDisallowed(t *testing.T) {
	tmp := t.TempDir()
	writeSkill(t, tmp, "alpha", "alpha body")
	writeSkill(t, tmp, "bravo", "bravo body")
	l := NewSkillsLoader(tmp, SkillsLoaderOptions{AllowedNames: []string{"alpha"}})
	summary := l.BuildSkillsSummary()
	if strings.Contains(summary, "bravo") {
		t.Fatalf("summary contains disallowed: %q", summary)
	}
	if !strings.Contains(summary, "alpha") {
		t.Fatalf("summary missing allowed: %q", summary)
	}
}

func TestAllowList_NilAllowsEverything(t *testing.T) {
	tmp := t.TempDir()
	writeSkill(t, tmp, "alpha", "alpha body")
	writeSkill(t, tmp, "bravo", "bravo body")
	l := NewSkillsLoader(tmp, SkillsLoaderOptions{})
	got := l.ListSkills(false)
	if len(got) != 2 {
		t.Fatalf("got %d", len(got))
	}
}
