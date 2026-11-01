package task

type Policy string

const (
	PolicyDecision Policy = "decision"
	PolicyApproval Policy = "approval"
	PolicyScoped   Policy = "scoped"
	PolicyReadOnly Policy = "readonly"
)
