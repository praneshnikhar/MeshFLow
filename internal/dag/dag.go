package dag

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/pranesh/meshflow/pkg/workflow"
	"gopkg.in/yaml.v3"
)

func Parse(data []byte) (*workflow.Workflow, error) {
	var w workflow.Workflow
	if err := yaml.Unmarshal(data, &w); err != nil {
		return nil, fmt.Errorf("parse workflow yaml: %w", err)
	}

	w.Default()

	if err := w.Validate(); err != nil {
		return nil, err
	}

	if err := checkCycles(&w); err != nil {
		return nil, err
	}

	return &w, nil
}

func checkCycles(w *workflow.Workflow) error {
	graph := make(map[string][]string)
	for _, t := range w.Tasks {
		graph[t.ID] = t.DependsOn
	}

	visited := make(map[string]int)
	var path []string

	var dfs func(id string) error
	dfs = func(id string) error {
		if visited[id] == 1 {
			path = append(path, id)
			return fmt.Errorf("cycle detected: %s", strings.Join(path, " -> "))
		}
		if visited[id] == 2 {
			return nil
		}

		visited[id] = 1
		path = append(path, id)

		for _, dep := range graph[id] {
			if err := dfs(dep); err != nil {
				return err
			}
		}

		path = path[:len(path)-1]
		visited[id] = 2
		return nil
	}

	for _, t := range w.Tasks {
		if visited[t.ID] == 0 {
			if err := dfs(t.ID); err != nil {
				return err
			}
		}
	}

	return nil
}

func ReadyTasks(w *workflow.Workflow, completed map[string]bool) []workflow.Task {
	if completed == nil {
		completed = make(map[string]bool)
	}

	var ready []workflow.Task
	for _, t := range w.Tasks {
		if completed[t.ID] {
			continue
		}
		allDepsDone := true
		for _, dep := range t.DependsOn {
			if !completed[dep] {
				allDepsDone = false
				break
			}
		}
		if allDepsDone {
			ready = append(ready, t)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		return ready[i].ID < ready[j].ID
	})
	return ready
}

func IsComplete(w *workflow.Workflow, completed map[string]bool) bool {
	for _, t := range w.Tasks {
		if !completed[t.ID] {
			return false
		}
	}
	return true
}

func AllFailed(w *workflow.Workflow, completed map[string]bool, failed map[string]int) bool {
	total := len(w.Tasks)
	done := len(completed)
	terminalFailed := 0
	for _, t := range w.Tasks {
		if completed[t.ID] {
			continue
		}
		maxAttempts := 3
		if t.Retry != nil {
			maxAttempts = t.Retry.MaxAttempts
		}
		if f, ok := failed[t.ID]; ok && f >= maxAttempts {
			terminalFailed++
		}
	}
	return (done + terminalFailed) >= total
}

var ErrCycleDetected = errors.New("workflow contains a cycle")
