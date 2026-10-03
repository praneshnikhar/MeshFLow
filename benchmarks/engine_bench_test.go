package benchmarks

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pranesh/meshflow/internal/engine"
	"github.com/pranesh/meshflow/internal/pubsub"
	"github.com/pranesh/meshflow/internal/store"
	"github.com/pranesh/meshflow/pkg/events"
	"github.com/pranesh/meshflow/pkg/workflow"
)

type benchCtx struct {
	engine   *engine.Engine
	store    *store.Store
	broker   *pubsub.Broker
	workflow *workflow.Workflow
}

func makeBroker(b *testing.B, name string) *pubsub.Broker {
	b.Helper()
	broker, err := pubsub.StartEmbedded(
		fmt.Sprintf("%s-%d", name, time.Now().UnixNano()),
		"127.0.0.1",
		-1,
	)
	if err != nil {
		b.Fatal(err)
	}
	return broker
}

func setupEngine(b *testing.B, taskCount int) *benchCtx {
	b.Helper()

	st, err := store.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}

	broker := makeBroker(b, "bench-engine")

	eng := engine.New("bench-node", broker, st)
	if err := eng.Start(); err != nil {
		b.Fatal(err)
	}

	wf := &workflow.Workflow{Name: "bench-wf"}
	for i := 0; i < taskCount; i++ {
		task := workflow.Task{
			ID:      fmt.Sprintf("task-%d", i),
			Handler: "http://localhost:19999/bench",
			Retry:   &workflow.RetryPolicy{MaxAttempts: 1},
			Timeout: "5s",
		}
		if i > 0 {
			task.DependsOn = []string{fmt.Sprintf("task-%d", i-1)}
		}
		wf.Tasks = append(wf.Tasks, task)
	}
	wf.Default()
	eng.Deploy(wf)

	return &benchCtx{
		engine:   eng,
		store:    st,
		broker:   broker,
		workflow: wf,
	}
}

func BenchmarkSingleNodeTrigger(b *testing.B) {
	ctx := setupEngine(b, 1)
	defer ctx.broker.Shutdown()
	defer ctx.store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ctx.engine.Trigger(ctx.workflow.Name); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLinearDAG3Triggers(b *testing.B) {
	ctx := setupEngine(b, 3)
	defer ctx.broker.Shutdown()
	defer ctx.store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ctx.engine.Trigger(ctx.workflow.Name); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLinearDAG10Triggers(b *testing.B) {
	ctx := setupEngine(b, 10)
	defer ctx.broker.Shutdown()
	defer ctx.store.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ctx.engine.Trigger(ctx.workflow.Name); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParallelFanOut(b *testing.B) {
	st, err := store.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()

	broker := makeBroker(b, "bench-fanout")
	defer broker.Shutdown()

	eng := engine.New("bench-node", broker, st)
	if err := eng.Start(); err != nil {
		b.Fatal(err)
	}

	wf := &workflow.Workflow{Name: "fan-out"}
	wf.Tasks = append(wf.Tasks, workflow.Task{ID: "start", Handler: "http://localhost:19999/bench"})
	for i := 0; i < 10; i++ {
		wf.Tasks = append(wf.Tasks, workflow.Task{
			ID:        fmt.Sprintf("worker-%d", i),
			Handler:   "http://localhost:19999/bench",
			DependsOn: []string{"start"},
			Retry:     &workflow.RetryPolicy{MaxAttempts: 1},
		})
	}
	wf.Default()
	eng.Deploy(wf)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := eng.Trigger(wf.Name); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEventThroughput(b *testing.B) {
	st, _ := store.Open(b.TempDir())
	broker := makeBroker(b, "bench-events")
	defer broker.Shutdown()
	defer st.Close()

	received := atomic.Int64{}
	broker.Subscribe(events.EventHeartbeat, func(evt events.Event) {
		received.Add(1)
	})

	evt := events.NewEvent(events.EventHeartbeat, "bench", map[string]interface{}{"seq": 0})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			broker.Publish(evt)
		}
	})
	b.StopTimer()

	time.Sleep(100 * time.Millisecond)
	b.ReportMetric(float64(received.Load()), "received")
}

func BenchmarkStoreWrite(b *testing.B) {
	st, err := store.Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()

	run := &workflow.WorkflowRun{
		ID:        "bench-run",
		Workflow:  "bench",
		Status:    workflow.StatusRunning,
		StartedAt: time.Now(),
		Node:      "bench-node",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		run.ID = fmt.Sprintf("bench-run-%d", i)
		if err := st.SaveRun(run); err != nil {
			b.Fatal(err)
		}
	}
}

func TestEngineStartStop(t *testing.T) {
	st, _ := store.Open(t.TempDir())
	broker, _ := pubsub.StartEmbedded(
		fmt.Sprintf("test-startstop-%d", time.Now().UnixNano()),
		"127.0.0.1",
		-1,
	)
	defer broker.Shutdown()
	defer st.Close()

	eng := engine.New("test-node", broker, st)
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}

	err := eng.Trigger("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent workflow")
	}
}

func TestWorkflowDeployTrigger(t *testing.T) {
	st, _ := store.Open(t.TempDir())
	broker, _ := pubsub.StartEmbedded(
		fmt.Sprintf("test-deploy-%d", time.Now().UnixNano()),
		"127.0.0.1",
		-1,
	)
	defer broker.Shutdown()
	defer st.Close()

	eng := engine.New("test-node", broker, st)
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}

	wf := &workflow.Workflow{
		Name: "test-wf",
		Tasks: []workflow.Task{
			{ID: "t1", Handler: "http://localhost:19999/echo", Retry: &workflow.RetryPolicy{MaxAttempts: 1}},
		},
	}
	wf.Default()
	if err := eng.Deploy(wf); err != nil {
		t.Fatal(err)
	}

	if err := eng.Trigger("test-wf"); err != nil {
		t.Fatal(err)
	}

	runs := eng.GetRuns("test-wf")
	if len(runs) == 0 {
		t.Error("expected at least one run after trigger")
	}
}

var sinkCtx context.Context

func init() {
	sinkCtx = context.Background()
}
