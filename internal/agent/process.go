package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"app/internal/messaging"
	"app/internal/task"
	"github.com/nats-io/nats.go/jetstream"
)

var validTaskID = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func Process(ctx context.Context, msg jetstream.Msg, heartbeat time.Duration) error {
	var input task.Task

	if err := json.Unmarshal(msg.Data(), &input); err != nil {
		return fmt.Errorf("decode mission payload: %w", err)
	}

	if !validTaskID.MatchString(input.ID) {
		return fmt.Errorf("invalid task id %q", input.ID)
	}

	if strings.TrimSpace(input.ID) == "" {
		return errors.New("task id is required")
	}

	if strings.TrimSpace(input.Prompt) == "" {
		return errors.New("no prompt what are you doing")
	}

	policy, err := messaging.PolicyFromSubject(msg.Subject())
	if err != nil {
		return err
	}

	// 定期回報server, task仍在處理
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat()
	go keepAlive(heartbeatCtx, msg, heartbeat)

	slog.Info("processing task", "task_id", input.ID, "policy", policy)

	result, err := Run(ctx, input, policy)
	if err != nil {
		return fmt.Errorf("run agent for task %s: %w", input.ID, err)
	}

	slog.Info(
		"agent finished",
		"task_id", input.ID,
		"session_id", result.SessionID,
		"turns", result.NumTurns,
		"cost_usd", result.CostUSD,
		"result_chars", len(result.Text),
		"pending_approvals", len(result.Denials),
	)

	return nil
}

func keepAlive(ctx context.Context, msg jetstream.Msg, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := msg.InProgress(); err != nil {
				slog.Warn("send in-progress heartbeat", "error", err)
			}
		}
	}
}
