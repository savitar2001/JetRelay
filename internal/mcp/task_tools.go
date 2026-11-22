package mcp

import (
	"context"
	"fmt"

	"app/internal/messaging"
	"app/internal/task"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

type SubmitTaskInput struct {
	Prompt string             `json:"prompt"`
	Policy string             `json:"policy"`
	Attach []task.Attachement `json:"attachment,omitempty"`
}

type SubmitTaskOutput struct {
	TaskID   string `json:"task_id"`
	Stream   string `json:"stream"`
	Sequence uint64 `json:"sequence"`
}

func submitTask(js jetstream.JetStream) mcpsdk.ToolHandlerFor[SubmitTaskInput, SubmitTaskOutput] {
	return func(
		ctx context.Context,
		_ *mcpsdk.CallToolRequest,
		input SubmitTaskInput,
	) (*mcpsdk.CallToolResult, SubmitTaskOutput, error) {
		taskID := nuid.Next()

		ack, err := messaging.PublishTask(ctx, js, task.Task{
			ID:     taskID,
			Prompt: input.Prompt,
			Attach: input.Attach,
		}, task.Policy(input.Policy))
		if err != nil {
			return nil, SubmitTaskOutput{}, fmt.Errorf("publish task: %w", err)
		}

		return nil, SubmitTaskOutput{
			TaskID:   taskID,
			Stream:   ack.Stream,
			Sequence: ack.Sequence,
		}, nil
	}
}

func RegisterTools(server *mcpsdk.Server, js jetstream.JetStream) {
	mcpsdk.AddTool(server, &mcpsdk.Tool{
		Name:        "submitTask",
		Description: "提交任務，policy 為 decision、approval、scoped、readonly 其中之一",
	}, submitTask(js))
}
