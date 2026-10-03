package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"sync"
	"time"

	"github.com/pranesh/meshflow/pkg/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type GRPCExecutor struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	timeout time.Duration
}

func NewGRPCExecutor() *GRPCExecutor {
	return &GRPCExecutor{
		conns:   make(map[string]*grpc.ClientConn),
		timeout: 30 * time.Second,
	}
}

func (g *GRPCExecutor) Execute(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error) {
	timeout := g.timeout
	if task.Timeout != "" {
		if d, err := time.ParseDuration(task.Timeout); err == nil {
			timeout = d
		}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	host, _, err := net.SplitHostPort(task.Handler)
	if err != nil {
		host = task.Handler
	}

	g.mu.Lock()
	conn, ok := g.conns[host]
	if !ok {
		conn, err = grpc.NewClient(host, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			g.mu.Unlock()
			return &ExecuteResult{Success: false, Error: fmt.Sprintf("grpc dial: %v", err)}, nil
		}
		g.conns[host] = conn
	}
	g.mu.Unlock()

	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"meshflow-workflow", req.WorkflowID,
		"meshflow-task", req.TaskID,
	))

	var method, payload string
	if m, ok := req.Input["_grpc_method"].(string); ok {
		method = m
	}
	if p, ok := req.Input["_grpc_payload"].(string); ok {
		payload = p
	}

	in := bytes.NewReader([]byte(payload))
	out := &bytes.Buffer{}

	err = conn.Invoke(ctx, method, in, out)
	if err != nil {
		return &ExecuteResult{Success: false, Error: fmt.Sprintf("grpc invoke: %v", err)}, nil
	}

	output, _ := io.ReadAll(out)
	return &ExecuteResult{Success: true, Output: string(output)}, nil
}

func (g *GRPCExecutor) ExecuteWithRetry(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error) {
	policy := task.Retry
	initial, _ := time.ParseDuration(policy.InitialWait)
	max, _ := time.ParseDuration(policy.MaxWait)

	var lastErr error
	var result *ExecuteResult
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if attempt > 1 {
			backoff := backoffDur(policy.Backoff, attempt-1, initial, max)
			slog.Debug("retrying grpc task", "task", task.ID, "attempt", attempt, "backoff", backoff)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
		var err error
		result, err = g.Execute(ctx, task, req)
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

func backoffDur(backoffType string, attempt int, initial, max time.Duration) time.Duration {
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
