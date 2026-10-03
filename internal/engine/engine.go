package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/pranesh/meshflow/internal/dag"
	"github.com/pranesh/meshflow/internal/executor"
	"github.com/pranesh/meshflow/internal/pubsub"
	"github.com/pranesh/meshflow/internal/store"
	"github.com/pranesh/meshflow/pkg/events"
	"github.com/pranesh/meshflow/pkg/workflow"
	"github.com/pranesh/meshflow/internal/api"
)

type Engine struct {
	name            string
	broker          *pubsub.Broker
	httpExecutor    executor.Executor
	shellExecutor   executor.Executor
	wasmExecutor    executor.Executor
	grpcExecutor    executor.Executor
	store           *store.Store
	mu              sync.RWMutex
	workflows       map[string]*workflow.Workflow
	runs            map[string]*workflow.WorkflowRun
	claimed         map[string]bool
	claimedByNode   map[string]string
	taskOutputs     map[string]map[string]string
	claimArbWindow  time.Duration
	stopCh          chan struct{}
}

func New(name string, broker *pubsub.Broker, st *store.Store) *Engine {
	httpExec := executor.NewHTTPExecutor()
	return &Engine{
		name:           name,
		broker:         broker,
		httpExecutor:   executor.NewCircuitBreakerExecutor(httpExec),
		shellExecutor:  executor.NewShellExecutor(),
		wasmExecutor:   executor.NewWasmExecutor(),
		grpcExecutor:   executor.NewGRPCExecutor(),
		store:          st,
		workflows:      make(map[string]*workflow.Workflow),
		runs:           make(map[string]*workflow.WorkflowRun),
		claimed:        make(map[string]bool),
		claimedByNode:  make(map[string]string),
		taskOutputs:    make(map[string]map[string]string),
		claimArbWindow: 15 * time.Millisecond,
		stopCh:         make(chan struct{}),
	}
}

func (e *Engine) Start() error {
	if err := e.recoverState(); err != nil {
		slog.Warn("state recovery incomplete", "error", err)
	}

	subs := []events.EventType{
		events.EventWorkflowTriggered,
		events.EventTaskClaimed,
		events.EventTaskCompleted,
		events.EventTaskFailed,
	}

	for _, et := range subs {
		err := e.broker.Subscribe(et, e.handleEvent)
		if err != nil {
			return fmt.Errorf("subscribe %s: %w", et, err)
		}
	}

	slog.Info("dag engine started", "node", e.name)
	return nil
}

func (e *Engine) recoverState() error {
	if e.store == nil {
		return nil
	}

	rawWorkflows, err := e.store.LoadWorkflows()
	if err != nil {
		return fmt.Errorf("load workflows: %w", err)
	}

	for name, data := range rawWorkflows {
		var w workflow.Workflow
		if err := json.Unmarshal(data, &w); err != nil {
			slog.Warn("failed to unmarshal persisted workflow", "name", name, "error", err)
			continue
		}
		w.Default()
		e.workflows[name] = &w
	}
	slog.Info("recovered workflows", "count", len(e.workflows))

	runs, err := e.store.LoadRuns()
	if err != nil {
		return fmt.Errorf("load runs: %w", err)
	}

	e.mu.Lock()
	for id, run := range runs {
		e.runs[id] = run
	}
	e.mu.Unlock()

	slog.Info("recovered runs", "count", len(runs))

	go func() {
		if err := e.store.CompactEvents(10000); err != nil {
			slog.Debug("event compaction skipped", "error", err)
		}
	}()

	return nil
}

func (e *Engine) Deploy(w *workflow.Workflow) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.workflows[w.Name]; exists {
		return fmt.Errorf("workflow %s already deployed", w.Name)
	}

	e.workflows[w.Name] = w

	if e.store != nil {
		data, err := json.Marshal(w)
		if err != nil {
			slog.Warn("failed to marshal workflow for storage", "name", w.Name, "error", err)
		} else if saveErr := e.store.SaveWorkflow(w, data); saveErr != nil {
			slog.Warn("failed to persist workflow", "name", w.Name, "error", saveErr)
		}
	}

	slog.Info("workflow deployed", "name", w.Name, "tasks", len(w.Tasks))
	return nil
}

func (e *Engine) Trigger(name string) error {
	e.mu.RLock()
	_, exists := e.workflows[name]
	e.mu.RUnlock()
	if !exists {
		return fmt.Errorf("workflow %s not found", name)
	}

	return e.triggerWorkflow(name)
}

func (e *Engine) triggerWorkflow(name string) error {
	e.mu.RLock()
	w, exists := e.workflows[name]
	e.mu.RUnlock()
	if !exists {
		return fmt.Errorf("workflow %s not found", name)
	}

	runID := fmt.Sprintf("%s-%s", w.Name, time.Now().UTC().Format("20060102T150405"))

	e.publish(events.NewEvent(events.EventWorkflowTriggered, e.name, map[string]interface{}{
		"workflow": w.Name,
		"run_id":   runID,
	}))

	slog.Info("workflow triggered", "name", w.Name, "run_id", runID)
	return nil
}

func (e *Engine) handleEvent(evt events.Event) {
	runID, _ := evt.Payload["run_id"].(string)
	if runID == "" {
		return
	}

	switch evt.Type {
	case events.EventWorkflowTriggered:
		e.handleWorkflowTriggered(evt)
	case events.EventTaskClaimed:
		e.handleTaskClaimed(evt)
	case events.EventTaskCompleted:
		e.handleTaskCompleted(evt)
	case events.EventTaskFailed:
		e.handleTaskFailed(evt)
	}
}

func (e *Engine) handleWorkflowTriggered(evt events.Event) {
	workflowName, _ := evt.Payload["workflow"].(string)
	runID, _ := evt.Payload["run_id"].(string)

	e.mu.RLock()
	w, exists := e.workflows[workflowName]
	_, runExists := e.runs[runID]
	e.mu.RUnlock()

	if !exists {
		return
	}

	if !runExists {
		run := &workflow.WorkflowRun{
			ID:        runID,
			Workflow:  workflowName,
			Status:    workflow.StatusPending,
			Tasks:     make(map[string]workflow.TaskRun),
			StartedAt: time.Now().UTC(),
			Node:      e.name,
		}
		e.mu.Lock()
		e.runs[runID] = run
		e.mu.Unlock()
		e.saveRun(run)
		api.IncWorkflowsRun()
	}

	e.tryClaimAndRun(w, runID)
}

func (e *Engine) handleTaskClaimed(evt events.Event) {
	runID, _ := evt.Payload["run_id"].(string)
	taskID, _ := evt.Payload["task_id"].(string)
	claimedBy, _ := evt.Payload["claimed_by"].(string)

	e.mu.Lock()
	key := runID + ":" + taskID
	e.claimedByNode[key] = claimedBy
	if claimedBy != e.name {
		e.claimed[key] = true
	}
	e.mu.Unlock()
}

func (e *Engine) handleTaskCompleted(evt events.Event) {
	runID, _ := evt.Payload["run_id"].(string)
	taskID, _ := evt.Payload["task_id"].(string)

	e.mu.Lock()
	if run, ok := e.runs[runID]; ok {
		tr := run.Tasks[taskID]
		tr.Status = workflow.TaskCompleted
		now := time.Now().UTC()
		tr.EndedAt = &now
		output, _ := evt.Payload["output"].(string)
		tr.Output = output
		run.Tasks[taskID] = tr

		if e.taskOutputs[runID] == nil {
			e.taskOutputs[runID] = make(map[string]string)
		}
		e.taskOutputs[runID][taskID] = output
	}
	e.mu.Unlock()

	workflowName, _ := evt.Payload["workflow"].(string)
	e.mu.RLock()
	w, exists := e.workflows[workflowName]
	e.mu.RUnlock()

	if !exists {
		return
	}

	completed := e.getCompletedTasks(runID)
	if dag.IsComplete(w, completed) {
		e.mu.Lock()
		if run, ok := e.runs[runID]; ok {
			run.Status = workflow.StatusCompleted
			now := time.Now().UTC()
			run.EndedAt = &now
			e.saveRun(run)
		}
		e.mu.Unlock()

		e.publish(events.NewEvent(events.EventWorkflowCompleted, e.name, map[string]interface{}{
			"workflow": workflowName,
			"run_id":   runID,
		}))

		slog.Info("workflow completed", "name", workflowName, "run_id", runID)
		return
	}

	e.saveRunFromID(runID)
	e.tryClaimAndRun(w, runID)
}

func (e *Engine) handleTaskFailed(evt events.Event) {
	runID, _ := evt.Payload["run_id"].(string)
	taskID, _ := evt.Payload["task_id"].(string)
	claimedBy, _ := evt.Payload["claimed_by"].(string)

	e.mu.Lock()
	key := runID + ":" + taskID
	if claimedBy != e.name {
		e.claimed[key] = true
	}
	e.mu.Unlock()

	e.mu.RLock()
	run, runExists := e.runs[runID]
	e.mu.RUnlock()

	if !runExists {
		return
	}

	workflowName := run.Workflow
	e.mu.RLock()
	w, exists := e.workflows[workflowName]
	e.mu.RUnlock()

	if !exists {
		return
	}

	completed := e.getCompletedTasks(runID)
	failed := e.getFailedCounts(runID)
	if dag.AllFailed(w, completed, failed) {
		e.mu.Lock()
		if run, ok := e.runs[runID]; ok {
			run.Status = workflow.StatusFailed
			now := time.Now().UTC()
			run.EndedAt = &now
			e.saveRun(run)
		}
		e.mu.Unlock()

		slog.Info("workflow failed", "name", workflowName, "run_id", runID)
		return
	}

	if claimedBy != e.name {
		delete(e.claimed, key)
	}

	e.tryClaimAndRun(w, runID)
}

func (e *Engine) getExecutor(task workflow.Task) executor.Executor {
	switch task.HandlerType {
	case "shell":
		return e.shellExecutor
	case "wasm":
		return e.wasmExecutor
	case "grpc":
		return e.grpcExecutor
	default:
		return e.httpExecutor
	}
}

func (e *Engine) evaluateCondition(task workflow.Task, runID string) bool {
	if task.Condition == "" {
		return true
	}
	e.mu.RLock()
	outputs := e.taskOutputs[runID]
	e.mu.RUnlock()

	parts := splitCondition(task.Condition)
	if len(parts) == 3 {
		key := parts[0]
		op := parts[1]
		expected := parts[2]
		val, ok := outputs[key]
		if !ok {
			return false
		}
		switch op {
		case "==":
			return val == expected
		case "!=":
			return val != expected
		case "contains":
			return containsSubstr(val, expected)
		}
	}
	return true
}

func splitCondition(cond string) []string {
	var parts []string
	current := ""
	inQuote := false
	for _, ch := range cond {
		switch {
		case ch == '"':
			inQuote = !inQuote
		case ch == ' ' && !inQuote:
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		default:
			current += string(ch)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func (e *Engine) tryClaimAndRun(w *workflow.Workflow, runID string) {
	completed := e.getCompletedTasks(runID)
	ready := dag.ReadyTasks(w, completed)

	for _, task := range ready {
		if !e.evaluateCondition(task, runID) {
			e.mu.Lock()
			now := time.Now().UTC()
			if e.runs[runID] != nil {
				e.runs[runID].Tasks[task.ID] = workflow.TaskRun{
					TaskID:    task.ID,
					Status:    workflow.TaskSkipped,
					Output:    "condition not met: " + task.Condition,
					StartedAt: &now,
					EndedAt:   &now,
				}
				e.saveRun(e.runs[runID])
			}
			e.mu.Unlock()
			e.publish(events.NewEvent(events.EventTaskCompleted, e.name, map[string]interface{}{
				"run_id":   runID,
				"workflow": w.Name,
				"task_id":  task.ID,
				"output":   "condition not met: " + task.Condition,
			}))
			continue
		}
		key := runID + ":" + task.ID
		e.mu.Lock()
		alreadyClaimed := e.claimed[key]
		if !alreadyClaimed {
			e.claimed[key] = true
			e.claimedByNode[key] = e.name
		}
		e.mu.Unlock()

		if alreadyClaimed {
			continue
		}

		claimMsgID := fmt.Sprintf("claim.%s.%s", runID, task.ID)
		e.publishDeduped(events.NewEvent(events.EventTaskClaimed, e.name, map[string]interface{}{
			"run_id":     runID,
			"workflow":   w.Name,
			"task_id":    task.ID,
			"claimed_by": e.name,
		}), claimMsgID)

		time.Sleep(e.claimArbWindow)

		e.mu.RLock()
		winner := e.claimedByNode[key]
		e.mu.RUnlock()

		if winner != e.name {
			slog.Debug("claim arbitration lost", "task", key, "winner", winner, "self", e.name)
			e.mu.Lock()
			delete(e.claimed, key)
			e.mu.Unlock()
			continue
		}

		go e.executeTask(w, task, runID)
	}
}

func (e *Engine) executeTask(w *workflow.Workflow, task workflow.Task, runID string) {
	e.mu.Lock()
	now := time.Now().UTC()
	if e.runs[runID] == nil {
		e.mu.Unlock()
		return
	}
	e.runs[runID].Tasks[task.ID] = workflow.TaskRun{
		TaskID:    task.ID,
		Status:    workflow.TaskRunning,
		ClaimedBy: e.name,
		Attempt:   1,
		StartedAt: &now,
	}
	e.runs[runID].Status = workflow.StatusRunning
	tr := e.runs[runID].Tasks[task.ID]

	upstreamInputs := make(map[string]interface{})
	for _, dep := range task.DependsOn {
		if outs, ok := e.taskOutputs[runID]; ok {
			if out, ok2 := outs[dep]; ok2 {
				upstreamInputs[dep] = out
			}
		}
	}
	e.saveRun(e.runs[runID])
	e.mu.Unlock()

	e.publish(events.NewEvent(events.EventTaskStarted, e.name, map[string]interface{}{
		"run_id":   runID,
		"workflow": w.Name,
		"task_id":  task.ID,
	}))

	ctx := context.Background()
	exec := e.getExecutor(task)
	result, err := exec.ExecuteWithRetry(ctx, task, &executor.ExecuteRequest{
		WorkflowID: runID,
		TaskID:     task.ID,
		Input:      upstreamInputs,
	})

	now = time.Now().UTC()
	tr.EndedAt = &now

	if err != nil || (result != nil && !result.Success) {
		errMsg := "task failed"
		tr.Attempt = task.Retry.MaxAttempts
		if result != nil {
			tr.Attempt = result.Attempt
			if result.Error != "" {
				errMsg = result.Error
			}
		} else if err != nil {
			errMsg = err.Error()
		}
		tr.Status = workflow.TaskFailed
		tr.Error = errMsg
		api.IncTasksFailed()

		e.mu.Lock()
		e.runs[runID].Tasks[task.ID] = tr
		e.saveRun(e.runs[runID])
		e.mu.Unlock()

		e.publish(events.NewEvent(events.EventTaskFailed, e.name, map[string]interface{}{
			"run_id":     runID,
			"workflow":   w.Name,
			"task_id":    task.ID,
			"error":      errMsg,
			"claimed_by": e.name,
		}))

		slog.Error("task failed", "workflow", w.Name, "task", task.ID, "error", errMsg)
		return
	}

	tr.Status = workflow.TaskCompleted
	tr.Output = result.Output
	tr.Attempt = result.Attempt
	api.IncTasksCompleted()
	e.mu.Lock()
	e.runs[runID].Tasks[task.ID] = tr
	e.saveRun(e.runs[runID])
	e.mu.Unlock()

	e.publish(events.NewEvent(events.EventTaskCompleted, e.name, map[string]interface{}{
		"run_id":   runID,
		"workflow": w.Name,
		"task_id":  task.ID,
		"output":   result.Output,
	}))

	slog.Info("task completed", "workflow", w.Name, "task", task.ID)
}

func (e *Engine) publish(evt events.Event) {
	e.broker.Publish(evt)
	if e.store != nil {
		e.store.AppendEvent(evt)
	}
}

func (e *Engine) publishDeduped(evt events.Event, msgID string) {
	e.broker.PublishWithID(evt, msgID)
	if e.store != nil {
		e.store.AppendEvent(evt)
	}
}

func (e *Engine) saveRun(run *workflow.WorkflowRun) {
	if e.store == nil || run == nil {
		return
	}
	if err := e.store.SaveRun(run); err != nil {
		slog.Warn("failed to persist run", "id", run.ID, "error", err)
	}
}

func (e *Engine) saveRunFromID(runID string) {
	e.mu.RLock()
	run, ok := e.runs[runID]
	e.mu.RUnlock()
	if ok {
		e.saveRun(run)
	}
}

func (e *Engine) getCompletedTasks(runID string) map[string]bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	completed := make(map[string]bool)
	if run, ok := e.runs[runID]; ok {
		for id, tr := range run.Tasks {
			if tr.Status == workflow.TaskCompleted {
				completed[id] = true
			}
		}
	}
	return completed
}

func (e *Engine) getFailedCounts(runID string) map[string]int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	counts := make(map[string]int)
	if run, ok := e.runs[runID]; ok {
		for id, tr := range run.Tasks {
			if tr.Status == workflow.TaskFailed {
				counts[id]++
			}
		}
	}
	return counts
}

func (e *Engine) GetWorkflows() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	names := make([]string, 0, len(e.workflows))
	for name := range e.workflows {
		names = append(names, name)
	}
	return names
}

func (e *Engine) GetWorkflow(name string) *workflow.Workflow {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.workflows[name]
}

func (e *Engine) GetRuns(workflowName string) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var ids []string
	for id, run := range e.runs {
		if workflowName == "" || run.Workflow == workflowName {
			ids = append(ids, id)
		}
	}
	return ids
}

func (e *Engine) GetRun(runID string) *workflow.WorkflowRun {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.runs[runID]
}

func (e *Engine) Shutdown() {
	close(e.stopCh)
	if e.store != nil {
		if err := e.store.Close(); err != nil {
			slog.Error("store close error", "error", err)
		}
	}
	slog.Info("dag engine stopped", "node", e.name)
}
