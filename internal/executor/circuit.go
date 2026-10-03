package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pranesh/meshflow/pkg/workflow"
)

type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitOpen
	CircuitHalfOpen
)

type CircuitBreaker struct {
	mu            sync.Mutex
	state         CircuitState
	failures      int
	lastFailTime  time.Time
	threshold     int
	resetTimeout  time.Duration
	halfOpenMax   int
	halfOpenCount int
	onStateChange func(string, CircuitState)
}

func NewCircuitBreaker(threshold int, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:        CircuitClosed,
		threshold:    threshold,
		resetTimeout: resetTimeout,
		halfOpenMax:  1,
	}
}

func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == CircuitOpen && time.Since(cb.lastFailTime) > cb.resetTimeout {
		cb.state = CircuitHalfOpen
		cb.halfOpenCount = 0
	}
	return cb.state
}

func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitHalfOpen:
		if cb.halfOpenCount < cb.halfOpenMax {
			cb.halfOpenCount++
			return true
		}
		return false
	default:
		return false
	}
}

func (cb *CircuitBreaker) Success() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	if cb.state == CircuitHalfOpen {
		cb.state = CircuitClosed
		cb.halfOpenCount = 0
	}
}

func (cb *CircuitBreaker) Failure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures++
	cb.lastFailTime = time.Now()
	if cb.failures >= cb.threshold || cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		cb.halfOpenCount = 0
	}
}

type CircuitBreakerExecutor struct {
	inner    Executor
	breakers map[string]*CircuitBreaker
	mu       sync.Mutex
}

func NewCircuitBreakerExecutor(inner Executor) *CircuitBreakerExecutor {
	return &CircuitBreakerExecutor{
		inner:    inner,
		breakers: make(map[string]*CircuitBreaker),
	}
}

func (c *CircuitBreakerExecutor) getBreaker(handler string) *CircuitBreaker {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b, ok := c.breakers[handler]; ok {
		return b
	}
	b := NewCircuitBreaker(5, 30*time.Second)
	c.breakers[handler] = b
	return b
}

func (c *CircuitBreakerExecutor) Execute(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error) {
	cb := c.getBreaker(task.Handler)
	if !cb.Allow() {
		return &ExecuteResult{Success: false, Error: fmt.Sprintf("circuit breaker open for %s", task.Handler)}, nil
	}
	result, err := c.inner.Execute(ctx, task, req)
	if err != nil || (result != nil && !result.Success) {
		cb.Failure()
	} else {
		cb.Success()
	}
	return result, err
}

func (c *CircuitBreakerExecutor) ExecuteWithRetry(ctx context.Context, task workflow.Task, req *ExecuteRequest) (*ExecuteResult, error) {
	cb := c.getBreaker(task.Handler)
	if !cb.Allow() {
		return &ExecuteResult{Success: false, Error: fmt.Sprintf("circuit breaker open for %s", task.Handler)}, nil
	}
	result, err := c.inner.ExecuteWithRetry(ctx, task, req)
	if err != nil || (result != nil && !result.Success) {
		cb.Failure()
	} else {
		cb.Success()
	}
	return result, err
}
