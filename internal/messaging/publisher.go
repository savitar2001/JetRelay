package messaging

import (
	"context"
	"encoding/json"
	"fmt"

	"app/internal/task"
	"github.com/nats-io/nats.go/jetstream"
)

func SubjectFor(policy task.Policy) (string, error) {
	switch policy {
	case task.PolicyDecision:
		return MissionDecisionSubject, nil
	case task.PolicyApproval:
		return MissionApprovalSubject, nil
	case task.PolicyScoped:
		return MissionScopedSubject, nil
	case task.PolicyReadOnly:
		return MissionReadOnlySubject, nil
	default:
		return "", fmt.Errorf("unknown policy %q", policy)
	}
}

func PublishTask(
	ctx context.Context,
	js jetstream.JetStream,
	input task.Task,
	policy task.Policy,
) (*jetstream.PubAck, error) {
	subject, err := SubjectFor(policy)
	if err != nil {
		return nil, err
	}

	payload, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("encode task: %w", err)
	}

	ack, err := js.Publish(ctx, subject, payload, jetstream.WithMsgID(input.ID))
	if err != nil {
		return nil, fmt.Errorf("publish task to %s: %w", subject, err)
	}
	return ack, nil
}
