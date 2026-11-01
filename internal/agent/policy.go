package agent

import (
	"fmt"
	"strings"

	"app/internal/task"
)

type permission struct {
	mode         string
	allowedTools []string
	systemPrompt string
}

var permissions = map[task.Policy]permission{
	task.PolicyReadOnly: {
		mode:         "dontAsk",
		allowedTools: []string{"Read", "Grep", "Glob"},
	},
	task.PolicyScoped: {
		mode:         "acceptEdits",
		allowedTools: []string{"Read", "Grep", "Glob", "Edit", "Write"},
	},
	task.PolicyApproval: {
		mode:         "dontAsk",
		allowedTools: []string{"Read", "Grep", "Glob"},
		systemPrompt: "任何寫入、修改或執行（本機檔案、指令、資料庫）都需要人類允許。" +
			"需要時直接呼叫工具，系統會攔下並交給人類審核；被拒絕後不要重試。",
	},
	task.PolicyDecision: {
		mode:         "dontAsk",
		allowedTools: []string{"Read", "Grep", "Glob", "WebSearch", "WebFetch"},
		systemPrompt: "這個任務的決定必須由人類做出，你不能自行決定或執行。" +
			"請蒐集資訊後整理成請示：問題是什麼、提供充足的資訊上下文輔助用戶做出判斷",
	},
}

func permissionFor(policy task.Policy) (permission, error) {
	p, ok := permissions[policy]
	if !ok {
		return permission{}, fmt.Errorf("unknown policy %q", policy)
	}
	return p, nil
}

func (p permission) args() []string {
	args := []string{
		"-p",
		"--output-format", "json",
		"--permission-mode", p.mode,
		"--permission-prompts", "none",
		"--allowedTools", strings.Join(p.allowedTools, ","),
	}
	if p.systemPrompt != "" {
		args = append(args, "--append-system-prompt", p.systemPrompt)
	}

	return args
}
