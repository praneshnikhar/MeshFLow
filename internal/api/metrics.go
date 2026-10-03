package api

import (
	"net/http"
	"sync/atomic"
	"time"
)

var (
	tasksCompleted  atomic.Int64
	tasksFailed     atomic.Int64
	workflowsRun    atomic.Int64
	claimsPublished atomic.Int64
	nodeStartTime   int64
)

func init() {
	nodeStartTime = time.Now().Unix()
}

func IncTasksCompleted() { tasksCompleted.Add(1) }
func IncTasksFailed()    { tasksFailed.Add(1) }
func IncWorkflowsRun()   { workflowsRun.Add(1) }
func IncClaimsPublished() { claimsPublished.Add(1) }

func prometheusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)

	metrics := []string{
		"# HELP meshflow_tasks_completed_total Total number of completed tasks",
		"# TYPE meshflow_tasks_completed_total counter",
		"meshflow_tasks_completed_total " + itoa(tasksCompleted.Load()),
		"# HELP meshflow_tasks_failed_total Total number of failed tasks",
		"# TYPE meshflow_tasks_failed_total counter",
		"meshflow_tasks_failed_total " + itoa(tasksFailed.Load()),
		"# HELP meshflow_workflows_run_total Total number of workflow runs",
		"# TYPE meshflow_workflows_run_total counter",
		"meshflow_workflows_run_total " + itoa(workflowsRun.Load()),
		"# HELP meshflow_claims_published_total Total claim events published",
		"# TYPE meshflow_claims_published_total counter",
		"meshflow_claims_published_total " + itoa(claimsPublished.Load()),
		"# HELP meshflow_node_uptime_seconds Node uptime in seconds",
		"# TYPE meshflow_node_uptime_seconds gauge",
		"meshflow_node_uptime_seconds " + itoa(time.Now().Unix()-nodeStartTime),
	}
	for _, m := range metrics {
		w.Write([]byte(m + "\n"))
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	s := ""
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	if neg {
		s = "-" + s
	}
	return s
}
