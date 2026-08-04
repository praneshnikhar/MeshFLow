package events

import (
	"encoding/json"
	"time"
)

type EventType string

const (
	EventWorkflowTriggered EventType = "workflow.triggered"
	EventTaskClaimed       EventType = "task.claimed"
	EventTaskStarted       EventType = "task.started"
	EventTaskCompleted     EventType = "task.completed"
	EventTaskFailed        EventType = "task.failed"
	EventWorkflowCompleted EventType = "workflow.completed"
	EventNodeJoined        EventType = "node.joined"
	EventNodeLeft          EventType = "node.left"
	EventHeartbeat         EventType = "mesh.heartbeat"
)

type Event struct {
	ID        string                 `json:"id"`
	Type      EventType              `json:"type"`
	Source    string                 `json:"source"`
	Timestamp time.Time              `json:"timestamp"`
	Payload   map[string]interface{} `json:"payload"`
}

func NewEvent(typ EventType, source string, payload map[string]interface{}) Event {
	return Event{
		ID:        generateID(),
		Type:      typ,
		Source:    source,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}

func (e Event) Encode() ([]byte, error) {
	return json.Marshal(e)
}

func Decode(data []byte) (Event, error) {
	var e Event
	err := json.Unmarshal(data, &e)
	return e, err
}

func generateID() string {
	return time.Now().UTC().Format("20060102T150405") + "-" + randString(8)
}

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
	}
	return string(b)
}
