package messaging

import (
	"fmt"

	"app/internal/task"
)

const (
	MissionDecisionSubject = "mission.created.decision"
	MissionApprovalSubject = "mission.created.approval"
	MissionScopedSubject   = "mission.created.scoped"
	MissionReadOnlySubject = "mission.created.readonly"
)

func PolicyFromSubject(subject string) (task.Policy, error) {
	switch subject {
	case MissionDecisionSubject:
		return task.PolicyDecision, nil
	case MissionApprovalSubject:
		return task.PolicyApproval, nil
	case MissionScopedSubject:
		return task.PolicyScoped, nil
	case MissionReadOnlySubject:
		return task.PolicyReadOnly, nil
	default:
		return "", fmt.Errorf("no policy for subject %q", subject)
	}
}
