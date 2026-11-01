package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"app/internal/task"
)

const runTimeout = 30 * time.Minute

type cliResult struct {
	Subtype           string   `json:"subtype"`
	IsError           bool     `json:"is_error"`
	Result            string   `json:"result"`
	SessionID         string   `json:"session_id"`
	TotalCostUSD      float64  `json:"total_cost_usd"`
	NumTurns          int      `json:"num_turns"`
	PermissionDenials []Denial `json:"permission_denials"`
}

type Result struct {
	SessionID string
	Text      string
	CostUSD   float64
	NumTurns  int
	Denials   []Denial
}

type Denial struct {
	ToolName  string          `json:"tool_name"`
	ToolInput json.RawMessage `json:"tool_input"`
}

func Run(ctx context.Context, input task.Task, policy task.Policy) (Result, error) {
	perm, err := permissionFor(policy)
	if err != nil {
		return Result{}, err
	}

	// 每個任務建立一個獨立目錄，避免agent碰到 /app代碼
	workDir := filepath.Join(os.TempDir(), "jetrescue", input.ID)
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return Result{}, fmt.Errorf("create work dir: %w", err)
	}

	runCtx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, "claude", perm.args()...)
	cmd.Dir = workDir
	cmd.Stdin = strings.NewReader(buildPrompt(input))

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	runErr := cmd.Run()

	var out cliResult
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		if runErr != nil {
			return Result{}, fmt.Errorf("run claude: %w: %s", runErr, strings.TrimSpace(stderr.String()))
		}

		return Result{}, fmt.Errorf("decode claude output: %w", err)
	}

	if runErr != nil || out.IsError {
		return Result{}, fmt.Errorf("claude run failed (subtype=%s): %s", out.Subtype, out.Result)
	}

	return Result{
		SessionID: out.SessionID,
		Text:      out.Result,
		CostUSD:   out.TotalCostUSD,
		NumTurns:  out.NumTurns,
		Denials:   out.PermissionDenials,
	}, nil
}

func buildPrompt(input task.Task) string {
	if len(input.Attach) == 0 {
		return input.Prompt
	}

	var b strings.Builder
	b.WriteString(input.Prompt)
	b.WriteString("\n\n參考資料:\n")
	for _, a := range input.Attach {
		fmt.Fprintf(&b, "- %s: %s\n", a.Name, a.URI)
	}

	return b.String()
}
