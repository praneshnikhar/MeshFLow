package workflow

import (
	"fmt"
	"time"
)

type RetryPolicy struct {
	MaxAttempts int    `yaml:"max_attempts" json:"max_attempts"`
	Backoff     string `yaml:"backoff" json:"backoff"`
	InitialWait string `yaml:"initial_wait" json:"initial_wait"`
	MaxWait     string `yaml:"max_wait" json:"max_wait"`
}

func (r *RetryPolicy) Default() {
	if r.MaxAttempts == 0 {
		r.MaxAttempts = 3
	}
	if r.Backoff == "" {
		r.Backoff = "exponential"
	}
	if r.InitialWait == "" {
		r.InitialWait = "1s"
	}
	if r.MaxWait == "" {
		r.MaxWait = "60s"
	}
}

type Trigger struct {
	Cron  string `yaml:"cron" json:"cron"`
	Event string `yaml:"event" json:"event"`
}

type Task struct {
	ID          string                 `yaml:"id" json:"id"`
	Handler     string                 `yaml:"handler" json:"handler"`
	HandlerType string                 `yaml:"handler_type" json:"handler_type"`
	DependsOn   []string               `yaml:"depends_on" json:"depends_on"`
	Retry       *RetryPolicy           `yaml:"retry" json:"retry"`
	Timeout     string                 `yaml:"timeout" json:"timeout"`
	Input       map[string]interface{} `yaml:"input" json:"input"`
	Condition   string                 `yaml:"condition" json:"condition"`
}

func (t *Task) Default() {
	if t.Retry == nil {
		t.Retry = &RetryPolicy{}
	}
	t.Retry.Default()
	if t.HandlerType == "" {
		t.HandlerType = "http"
	}
	if t.Timeout == "" {
		t.Timeout = "30s"
	}
}

type Workflow struct {
	Name        string            `yaml:"name" json:"name"`
	Description string            `yaml:"description" json:"description"`
	Template    bool              `yaml:"template" json:"template"`
	Params      map[string]string `yaml:"params" json:"params"`
	Triggers    []Trigger         `yaml:"triggers" json:"triggers"`
	Tasks       []Task            `yaml:"tasks" json:"tasks"`
}

func (w *Workflow) Validate() error {
	if w.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	if len(w.Tasks) == 0 {
		return fmt.Errorf("workflow %s has no tasks", w.Name)
	}

	taskIDs := make(map[string]bool)
	for _, t := range w.Tasks {
		if t.ID == "" {
			return fmt.Errorf("task id is required in workflow %s", w.Name)
		}
		if taskIDs[t.ID] {
			return fmt.Errorf("duplicate task id %s in workflow %s", t.ID, w.Name)
		}
		taskIDs[t.ID] = true
		if t.Handler == "" {
			return fmt.Errorf("task %s in workflow %s has no handler", t.ID, w.Name)
		}
	}

	for _, t := range w.Tasks {
		for _, dep := range t.DependsOn {
			if !taskIDs[dep] {
				return fmt.Errorf("task %s depends on unknown task %s in workflow %s", t.ID, dep, w.Name)
			}
		}
	}

	return nil
}

func (w *Workflow) Default() {
	for i := range w.Tasks {
		w.Tasks[i].Default()
	}
}

type WorkflowStatus string

const (
	StatusPending   WorkflowStatus = "pending"
	StatusRunning   WorkflowStatus = "running"
	StatusCompleted WorkflowStatus = "completed"
	StatusFailed    WorkflowStatus = "failed"
)

type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskClaimed   TaskStatus = "claimed"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskSkipped   TaskStatus = "skipped"
)

type WorkflowRun struct {
	ID        string             `json:"id"`
	Workflow  string             `json:"workflow"`
	Status    WorkflowStatus     `json:"status"`
	Tasks     map[string]TaskRun `json:"tasks"`
	StartedAt time.Time          `json:"started_at"`
	EndedAt   *time.Time         `json:"ended_at,omitempty"`
	Node      string             `json:"node"`
}

type TaskRun struct {
	TaskID    string     `json:"task_id"`
	Status    TaskStatus `json:"status"`
	ClaimedBy string     `json:"claimed_by,omitempty"`
	Attempt   int        `json:"attempt"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	Error     string     `json:"error,omitempty"`
	Output    string     `json:"output,omitempty"`
}
