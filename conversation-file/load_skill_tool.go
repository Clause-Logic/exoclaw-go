package conversationfile

import (
	"context"

	"github.com/Clause-Logic/exoclaw-go/exoclaw/agent/tools"
)

// Ported from exoclaw_conversation/load_skill_tool.py.
//
// LoadSkillTool — bridges the LoadSkillToolDef schema with
// SkillsLoader.ActivateSkill.

// LoadSkillTool lets the agent dynamically activate skills listed in the
// system prompt's <skills> block.
//
// Activating a skill merges its content into context AND merges any tool
// names the skill declares into the agent's active-tools set, so
// subsequent LLM calls see those tools available.
type LoadSkillTool struct {
	skills      *SkillsLoader
	activeTools map[string]struct{}
}

// NewLoadSkillTool constructs a LoadSkillTool. activeTools is the shared
// optional-tools set that the agent loop reads via
// Conversation.ActiveTools — calls to ActivateSkill mutate it in place so
// the loop's next iteration sees the newly activated tools.
func NewLoadSkillTool(skills *SkillsLoader, activeTools map[string]struct{}) *LoadSkillTool {
	if activeTools == nil {
		activeTools = map[string]struct{}{}
	}
	return &LoadSkillTool{skills: skills, activeTools: activeTools}
}

// Name returns the tool name from the canonical LoadSkillToolDef.
func (t *LoadSkillTool) Name() string {
	fn := LoadSkillToolDef["function"].(map[string]any)
	return fn["name"].(string)
}

// Description returns the tool description.
func (t *LoadSkillTool) Description() string {
	fn := LoadSkillToolDef["function"].(map[string]any)
	return fn["description"].(string)
}

// Parameters returns the JSON-schema parameters.
func (t *LoadSkillTool) Parameters() map[string]any {
	fn := LoadSkillToolDef["function"].(map[string]any)
	return fn["parameters"].(map[string]any)
}

// Execute resolves the requested skill, merges its activated tool names
// into the shared active-tools set, and returns the skill content.
func (t *LoadSkillTool) Execute(ctx context.Context, params map[string]any) (string, error) {
	name, _ := params["name"].(string)
	if name == "" {
		return "Error: missing 'name' parameter", nil
	}
	result := t.skills.ActivateSkill(name)
	for _, n := range result.ToolNames {
		t.activeTools[n] = struct{}{}
	}
	return result.Content, nil
}

// Compile-time check that LoadSkillTool satisfies the Tool interface.
var _ tools.Tool = (*LoadSkillTool)(nil)
