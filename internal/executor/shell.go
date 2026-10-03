package executor

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"os/exec"
	"strings"
	"time"

	"github.com/pranesh/meshflow/pkg/workflow"
)

type ShellExecutor struct {
	defaultTimeout time.Duration
}

func NewShellExecutor() *ShellExecutor {
	return &ShellExecutor{defaultTimeout: 30 * time.Second}
}

func (s *ShellExecutor) Execute(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error) {
	timeout := s.defaultTimeout
	if task.Timeout != "" {
		if d, err := time.ParseDuration(task.Timeout); err == nil {
			timeout = d
		}
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sh", "-c", task.Handler)
	cmd.Env = s.buildEnv(req)
	cmd.Stdin = strings.NewReader(s.buildStdin(req))

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return &ExecuteResult{Success: false, Error: "command timed out"}, nil
		}
		return &ExecuteResult{
			Success: false,
			Output:  stdout.String(),
			Error:   fmt.Sprintf("%s: %s", err.Error(), stderr.String()),
		}, nil
	}

	output := stdout.String()
	if output == "" {
		output = stderr.String()
	}

	return &ExecuteResult{
		Success: true,
		Output:  output,
	}, nil
}

func (s *ShellExecutor) ExecuteWithRetry(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error) {
	policy := task.Retry
	initial, _ := time.ParseDuration(policy.InitialWait)
	max, _ := time.ParseDuration(policy.MaxWait)

	var lastErr error
	var result *ExecuteResult

	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if attempt > 1 {
			backoff := s.backoffDuration(policy.Backoff, attempt-1, initial, max)
			slog.Debug("retrying shell task", "task", task.ID, "attempt", attempt, "backoff", backoff)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		var err error
		result, err = s.Execute(ctx, task, req)
		if err != nil {
			lastErr = err
			continue
		}
		if result.Success {
			result.Attempt = attempt
			return result, nil
		}
		lastErr = fmt.Errorf("%s", result.Error)
	}

	if result != nil {
		result.Attempt = policy.MaxAttempts
	}
	return result, lastErr
}

func (s *ShellExecutor) backoffDuration(backoffType string, attempt int, initial, max time.Duration) time.Duration {
	switch backoffType {
	case "fixed":
		return initial
	case "exponential":
		d := initial * time.Duration(int(math.Pow(2, float64(attempt-1))))
		if d > max {
			return max
		}
		return d
	default:
		return initial
	}
}

func (s *ShellExecutor) buildEnv(req *ExecuteRequest) []string {
	env := []string{
		fmt.Sprintf("MESHFLOW_WORKFLOW=%s", req.WorkflowID),
		fmt.Sprintf("MESHFLOW_TASK=%s", req.TaskID),
	}
	for k, v := range req.Input {
		env = append(env, fmt.Sprintf("MESHFLOW_%s=%v", strings.ToUpper(k), v))
	}
	return env
}

func (s *ShellExecutor) buildStdin(req *ExecuteRequest) string {
	if req.Input == nil {
		return ""
	}
	if v, ok := req.Input["stdin"]; ok {
		return fmt.Sprintf("%v", v)
	}
	return ""
}
