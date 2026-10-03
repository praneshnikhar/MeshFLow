package executor

import (
	"context"

	"github.com/pranesh/meshflow/pkg/workflow"
)

type Executor interface {
	Execute(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error)
	ExecuteWithRetry(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error)
}
