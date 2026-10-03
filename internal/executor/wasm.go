package executor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/pranesh/meshflow/pkg/workflow"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

type WasmExecutor struct {
	runtime wazero.Runtime
}

func NewWasmExecutor() *WasmExecutor {
	return &WasmExecutor{runtime: wazero.NewRuntime(context.Background())}
}

func (w *WasmExecutor) Execute(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error) {
	wasmBytes, ok := req.Input["_wasm_bytes"].([]byte)
	if !ok {
		if _, ok := req.Input["_wasm_file"].(string); !ok {
			return &ExecuteResult{Success: false, Error: "no _wasm_bytes or _wasm_file in input"}, nil
		}
		return &ExecuteResult{Success: false, Error: "wasm file loading not supported; pass _wasm_bytes"}, nil
	}

	timeout := 30 * time.Second
	if task.Timeout != "" {
		if d, err := time.ParseDuration(task.Timeout); err == nil {
			timeout = d
		}
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cfg := wazero.NewModuleConfig().WithSysNanotime().WithSysWalltime().WithStdout(noopWriter{}).WithStderr(noopWriter{})
	for k, v := range req.Input {
		if k == "_wasm_bytes" || k == "_wasm_file" {
			continue
		}
		cfg = cfg.WithEnv(k, fmt.Sprintf("%v", v))
	}
	cfg = cfg.WithEnv("MESHFLOW_WORKFLOW", req.WorkflowID).WithEnv("MESHFLOW_TASK", req.TaskID)

	wasmMod, err := w.runtime.InstantiateWithConfig(ctx, wasmBytes, cfg)
	if err != nil {
		return &ExecuteResult{Success: false, Error: fmt.Sprintf("wasm instantiate: %v", err)}, nil
	}
	defer wasmMod.Close(ctx)

	wasi_snapshot_preview1.MustInstantiate(ctx, w.runtime)
	_ = wasmMod

	run := wasmMod.ExportedFunction("run")
	if run == nil {
		run = wasmMod.ExportedFunction("_start")
	}
	if run == nil {
		return &ExecuteResult{Success: false, Error: "no exported 'run' or '_start' function"}, nil
	}

	results, err := run.Call(ctx)
	if err != nil {
		return &ExecuteResult{Success: false, Error: fmt.Sprintf("wasm execution: %v", err)}, nil
	}

	output := "ok"
	if len(results) > 0 {
		output = fmt.Sprintf("%d", api.DecodeI32(results[0]))
	}

	return &ExecuteResult{Success: true, Output: output}, nil
}

func (w *WasmExecutor) ExecuteWithRetry(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error) {
	policy := task.Retry
	initial, _ := time.ParseDuration(policy.InitialWait)
	max, _ := time.ParseDuration(policy.MaxWait)

	var lastErr error
	var result *ExecuteResult
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		if attempt > 1 {
			backoff := backoffDur(policy.Backoff, attempt-1, initial, max)
			slog.Debug("retrying wasm task", "task", task.ID, "attempt", attempt, "backoff", backoff)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
		}
		var err error
		result, err = w.Execute(ctx, task, req)
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

type noopWriter struct{}

func (noopWriter) Write(p []byte) (int, error) { return len(p), nil }

var wasmFile string
