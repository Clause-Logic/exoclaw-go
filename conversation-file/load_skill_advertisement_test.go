package conversationfile

import (
	"strings"
	"testing"
)

// By default a ContextBuilder advertises the load_skill tool (so the exoclaw
// CLI / openclaw can dynamically load skills). When SuppressLoadSkillAdvertisement
// is set, that block is dropped — for deployments (e.g. agent-core-go runs with
// pre-activated skills) that never wire load_skill into the tool list, so the
// model isn't told to call a tool that would fail with ToolNotFound.
func TestBuildSystemPrompt_LoadSkillAdvertisementGate(t *testing.T) {
	tmp := t.TempDir()
	writeSkill(t, tmp, "alpha", "alpha body")
	skills := NewSkillsLoader(tmp, SkillsLoaderOptions{})

	b := NewContextBuilder(tmp, nil, skills, 0)

	got := b.BuildSystemPrompt(BuildSystemPromptOptions{})
	if !strings.Contains(got, "call the load_skill tool") {
		t.Fatalf("default prompt should advertise load_skill; got:\n%s", got)
	}
	if !strings.Contains(got, "# Skills") {
		t.Fatalf("default prompt should include the skills summary; got:\n%s", got)
	}

	b.SuppressLoadSkillAdvertisement = true
	got = b.BuildSystemPrompt(BuildSystemPromptOptions{})
	if strings.Contains(got, "call the load_skill tool") {
		t.Fatalf("suppressed prompt must not advertise load_skill; got:\n%s", got)
	}
	if strings.Contains(got, "# Skills") {
		t.Fatalf("suppressed prompt must omit the skills summary block; got:\n%s", got)
	}
}
