package task

type Task struct {
	ID     string        `json:"task_id"`
	Prompt string        `json:"prompt"`
	Attach []Attachement `json:"attachment,omitempty"`
}

type Attachement struct {
	Name string `json:"name"`
	URI  string `json:"uri"`
}
