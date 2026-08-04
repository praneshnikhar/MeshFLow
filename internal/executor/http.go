package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/pranesh/meshflow/pkg/workflow"
)

type HTTPExecutor struct {
	client *http.Client
}

func NewHTTPExecutor() *HTTPExecutor {
	return &HTTPExecutor{
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				IdleConnTimeout:     30 * time.Second,
				DisableKeepAlives:   false,
			},
		},
	}
}

type ExecuteRequest struct {
	WorkflowID string                 `json:"workflow_id"`
	TaskID     string                 `json:"task_id"`
	Input      map[string]interface{} `json:"input"`
}

type ExecuteResult struct {
	Success bool   `json:"success"`
	Output  string `json:"output"`
	Error   string `json:"error,omitempty"`
	Attempt int    `json:"attempt"`
}

func (e *HTTPExecutor) Execute(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error) {
	body := ExecuteRequest{
		WorkflowID: req.WorkflowID,
		TaskID:     task.ID,
		Input:      task.Input,
	}
	if req.Input != nil {
		for k, v := range req.Input {
			body.Input[k] = v
		}
	}

	data, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	timeout, _ := time.ParseDuration(task.Timeout)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, task.Handler, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Meshflow-Workflow", req.WorkflowID)
	httpReq.Header.Set("X-Meshflow-Task", task.ID)

	if timeout > 0 {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		httpReq = httpReq.WithContext(ctx)
	}

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return &ExecuteResult{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return &ExecuteResult{
			Success: false,
			Error:   fmt.Sprintf("read response: %v", err),
		}, nil
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return &ExecuteResult{
			Success: true,
			Output:  string(respBody),
		}, nil
	}

	return &ExecuteResult{
		Success: false,
		Error:   fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(respBody)),
	}, nil
}

func BackoffDuration(backoffType string, attempt int, initial, max time.Duration) time.Duration {
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

func (e *HTTPExecutor) ExecuteWithRetry(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error) {
	policy := task.Retry
	initial, _ := time.ParseDuration(policy.InitialWait)
	max, _ := time.ParseDuration(policy.MaxWait)

	var lastErr error
	var result *ExecuteResult

	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if attempt > 1 {
			backoff := BackoffDuration(policy.Backoff, attempt-1, initial, max)
			slog.Debug("retrying task",
				"task", task.ID,
				"attempt", attempt,
				"backoff", backoff,
			)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}

		var err error
		result, err = e.Execute(ctx, task, req)
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
