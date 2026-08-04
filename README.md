# MeshFlow

**Decentralized, event-driven workflow orchestration mesh. No central scheduler. No single point of failure. Single ~25MB binary.**

MeshFlow is a peer-to-peer workflow engine. Run the same binary on N machines, they auto-discover each other via gossip, and workflows execute across the mesh as events. Tasks are claimed and executed wherever capacity exists. If a node dies, another picks up the work.

Think: Temporal's workflow engine + NATS's pub/sub mesh + Consul's peer discovery — all in one binary.

---

## Why

Every major workflow orchestrator (Airflow, Temporal, Dagster, Prefect) has a central scheduler. That scheduler is a single point of failure. If it goes down, nothing runs. You need Redis, a database, a message queue, and careful HA setup just to get basic resilience.

MeshFlow eliminates the scheduler entirely. The mesh IS the scheduler. Nodes gossip to discover peers, events flow through the pub/sub mesh, and tasks are claimed on a first-come basis. No central database. No queue to configure. No SPOF.

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│                    Node                          │
│  ┌──────────┐  ┌───────────┐  ┌──────────────┐ │
│  │  Gossip   │  │  Pub/Sub   │  │  DAG Engine  │ │
│  │ (SWIM)   │  │  (NATS)    │  │  + Executor  │ │
│  └────┬─────┘  └─────┬─────┘  └──────┬───────┘ │
│       │              │               │          │
│       └──────────────┼───────────────┘          │
│                      │                          │
│   ┌──────────┐  ┌────┴────┐  ┌───────────┐     │
│   │ BoltDB   │  │  HTTP   │  │  Dashboard│     │
│   │ (store)  │  │  API    │  │  (SPA)    │     │
│   └──────────┘  └─────────┘  └───────────┘     │
└──────────────────────┬───────────────────────────┘
                       │  gossip + pub/sub mesh
         ┌─────────────┼─────────────┐
    ┌────┴────┐   ┌────┴────┐   ┌────┴────┐
    │ Node B  │   │ Node C  │   │ Node D  │
    └─────────┘   └─────────┘   └─────────┘
```

### Components

| Layer | Technology | Role |
|-------|-----------|------|
| Peer discovery | hashicorp/memberlist (SWIM gossip) | Auto-discovers nodes, failure detection |
| Event mesh | Embedded NATS + JetStream | Durable pub/sub, at-least-once delivery |
| Persistence | BoltDB | Workflows, runs, task state, event log |
| Task execution | HTTP handler (extensible to gRPC/shell) | Runs user-defined task handlers |
| DAG engine | In-process Go | Parses YAML DAGs, resolves dependencies, claims tasks |
| Web dashboard | Embedded SPA | Real-time DAG viewer, deploy, trigger, live events |
| CLI | Cobra-style stdlib | Node management, workflow CRUD, event streaming |

---

## Quick Start

### Prerequisites

- Go 1.23+ (to build from source)
- Or Docker (to run the pre-built container)

### Build

```bash
git clone https://github.com/pranesh/meshflow
cd meshflow
go build -o meshflow ./cmd/meshflow/

# Optional: build the echo handler for local testing
go build -o echohandler ./cmd/echohandler/
```

### Run a single node

```bash
# Start the echo handler (in a separate terminal)
./echohandler

# Start MeshFlow
./meshflow node start
```

Open `http://localhost:8080` for the dashboard.

### Run a 3-node mesh (Docker Compose)

```bash
docker compose up --build
```

---

## Commands

### `meshflow node start`

Start a MeshFlow node. The node joins an existing mesh if seed nodes are configured, or becomes the first node.

```bash
meshflow node start
```

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `MESHFLOW_NAME` | hostname | Node name |
| `MESHFLOW_BIND_ADDR` | `0.0.0.0` | Bind address for all services |
| `MESHFLOW_GOSSIP_PORT` | `7947` | Gossip protocol port |
| `MESHFLOW_PUBSUB_PORT` | `4222` | Embedded NATS port |
| `MESHFLOW_API_PORT` | `8080` | HTTP API + dashboard port |
| `MESHFLOW_SEEDS` | (none) | Comma-separated seed nodes (`host:7947,host2:7947`) |
| `MESHFLOW_DATA_DIR` | `./data` | BoltDB data directory |

### `meshflow workflow deploy <file>`

Deploy a workflow from a YAML file to the running node.

```bash
meshflow workflow deploy pipeline.yaml
```

### `meshflow workflow list`

List all deployed workflows.

```bash
meshflow workflow list
# Output:
# sample-etl
# linear-pipeline
```

### `meshflow workflow trigger <name>`

Trigger a workflow run. The engine publishes a `workflow.triggered` event and any available node claims and executes the tasks.

```bash
meshflow workflow trigger sample-etl
# Output: Triggered: sample-etl
```

### `meshflow workflow status [run-id]`

Show workflow run status. Without arguments, lists all runs in a table. With a run ID, shows detailed task-level status.

```bash
meshflow workflow status
# RUN ID                                WORKFLOW        STATUS
# ----------------------------------------------------------------------
# sample-etl-20260804T034428            sample-etl      completed

meshflow workflow status sample-etl-20260804T034428
# Run ID:    sample-etl-20260804T034428
# Workflow:  sample-etl
# Status:    completed
# Tasks:
#   extract     completed   attempt=1  output={"success":true,...}
#   transform   completed   attempt=1  output={"success":true,...}
#   load        completed   attempt=1  output={"success":true,...}
```

### `meshflow events tail`

Stream live events from the mesh via SSE. Shows every event: node joins, workflow triggers, task claims, task execution, completions, failures, heartbeats.

```bash
meshflow events tail
# data: {"id":"...","type":"workflow.triggered","source":"node1","payload":{...}}
# data: {"id":"...","type":"task.claimed","source":"node1","payload":{...}}
# data: {"id":"...","type":"task.started","source":"node1","payload":{...}}
# data: {"id":"...","type":"task.completed","source":"node1","payload":{...}}
# data: {"id":"...","type":"workflow.completed","source":"node1","payload":{...}}
```

### `meshflow mesh status`

Show the current mesh topology — all connected nodes with their addresses.

```bash
meshflow mesh status
# Mesh members:
#   node1 (192.168.1.10:7947)
#   node2 (192.168.1.11:7947)
#   node3 (192.168.1.12:7947)
```

### `meshflow version`

Print version and build time.

```bash
meshflow version
# meshflow 0.1.0 (built 2026-08-04)
```

---

## Workflow Definition (YAML)

Workflows are defined as YAML files. Each workflow has a name, optional triggers, and a list of tasks with dependencies and retry policies.

### Full example

```yaml
name: etl-pipeline
description: "Hourly ETL: extract from API, transform, load to warehouse"
triggers:
  - cron: "0 * * * *"
tasks:
  - id: extract
    handler: http://extractor:8080/run
    retry:
      max_attempts: 3
      backoff: exponential
      initial_wait: 1s
      max_wait: 60s
    timeout: 30s

  - id: transform
    handler: http://transformer:8080/run
    depends_on:
      - extract
    retry:
      max_attempts: 2
      backoff: fixed
      initial_wait: 5s

  - id: load
    handler: http://loader:8080/run
    depends_on:
      - transform
```

### Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique workflow name |
| `description` | string | No | Human-readable description |
| `triggers` | list | No | Cron and/or event triggers |
| `triggers[].cron` | string | No | Cron expression |
| `triggers[].event` | string | No | Event name to listen for |
| `tasks` | list | Yes | Task definitions |
| `tasks[].id` | string | Yes | Unique task ID within the workflow |
| `tasks[].handler` | string | Yes | HTTP URL for the task handler |
| `tasks[].handler_type` | string | No | Handler type (default: `http`) |
| `tasks[].depends_on` | list | No | IDs of tasks that must complete first |
| `tasks[].retry` | object | No | Retry policy |
| `tasks[].retry.max_attempts` | int | No | Max retry attempts (default: 3) |
| `tasks[].retry.backoff` | string | No | `fixed` or `exponential` (default: `exponential`) |
| `tasks[].retry.initial_wait` | string | No | Wait before first retry (default: `1s`) |
| `tasks[].retry.max_wait` | string | No | Max wait between retries (default: `60s`) |
| `tasks[].timeout` | string | No | HTTP request timeout (default: `30s`) |
| `tasks[].input` | map | No | Input data passed to the handler |

### Task handler contract

Each task handler receives a POST request with JSON body:

```json
{
  "workflow_id": "etl-pipeline-20260804T034428",
  "task_id": "extract",
  "input": {}
}
```

Headers:
- `X-Meshflow-Workflow`: the run ID
- `X-Meshflow-Task`: the task ID

Expected response (200):

```json
{
  "success": true,
  "output": "Task completed successfully"
}
```

---

## API Reference

The node exposes an HTTP API on the configured API port (default `8080`).

### `GET /health`

Health check. Returns `ok`.

### `POST /deploy`

Deploy a workflow. Body is raw YAML.

```bash
curl -X POST http://localhost:8080/deploy \
  -d @pipeline.yaml \
  -H "Content-Type: application/octet-stream"
```

### `GET /workflows`

List deployed workflow names.

```json
{"workflows": ["etl-pipeline", "linear-pipeline"]}
```

### `GET /workflows/<name>`

Get full workflow definition with tasks and dependencies.

### `POST /trigger/<name>`

Trigger a workflow run.

```bash
curl -X POST http://localhost:8080/trigger/etl-pipeline
```

### `GET /runs`

List all workflow runs.

```json
{
  "runs": [
    {"id": "etl-pipeline-20260804T034428", "workflow": "etl-pipeline", "status": "completed"}
  ]
}
```

### `GET /runs/<id>`

Get detailed run status including per-task state.

### `GET /mesh`

Get cluster topology.

```json
{"members": ["node1 (192.168.1.10:7947)", "node2 (192.168.1.11:7947)"]}
```

### `GET /events`

Server-sent events (SSE) stream of all mesh events in real time.

### `GET /`

Web dashboard (single-page app).

---

## Web Dashboard

Open `http://localhost:8080` in a browser.

- **Overview** — Stats (workflows, runs, running/completed/failed), recent runs table
- **Workflows** — Deploy new workflows via YAML editor, trigger workflows, browse runs
- **DAG Viewer** — Select a run to see the dependency graph as an interactive SVG with color-coded task status (gray= pending, blue pulse=running, green=completed, red=failed). Shows parallel execution branches.
- **Mesh** — Topology graph showing all connected nodes, member list with addresses
- **Live Events** — Sidebar with real-time event stream (heartbeats, triggers, claims, executions, completions)

The dashboard updates in real time via SSE. Task status changes appear immediately.

---

## Event Flow

When a workflow is triggered, events flow through the mesh like this:

```
1. workflow.triggered    → published by the triggering node
2. task.claimed          → first available node claims an unblocked task
3. task.started          → node begins executing the task
4. task.completed        → task finishes successfully
5. (repeat 2-4 for each task in dependency order)
6. workflow.completed    → all tasks done
```

On task failure:
```
task.failed → retried locally (up to max_attempts) → if exhausted, re-opened for other nodes
```

On node failure:
```
Gossip detects dead node → orphaned tasks are re-opened → other nodes claim them
```

---

## How It Works

### Peer Discovery (Gossip)

Nodes use the SWIM protocol (via hashicorp/memberlist) to discover each other. Configure one or more seed nodes; new nodes join the mesh by contacting any seed. Failure detection takes ~2 seconds. Dead nodes are pruned after 60 seconds.

### Event Propagation

Events are published to the embedded NATS server (with JetStream for durability) and broadcast to all peer nodes via the gossip layer. Each node relays incoming gossip events back into its local NATS so local subscribers (engine, dashboard) see them. Duplicate suppression via event ID dedup prevents echo loops.

### Task Claiming

When a task's dependencies are satisfied, every node evaluates whether it can claim the task. The first node to publish a `task.claimed` event wins. Other nodes see the claim and skip that task. This is a first-claim-wins consensus mechanism — no leader election needed.

### Retries

Tasks are retried locally up to their configured `max_attempts`. Backoff can be `fixed` (constant wait) or `exponential` (doubles each attempt). If all retries are exhausted, the task is marked failed and other nodes are free to claim it.

### Persistence

All state is stored in an embedded BoltDB database:
- **Workflows** — stored as JSON on deploy
- **Runs** — saved on every state change (pending → running → completed/failed)
- **Task states** — updated atomically with run state
- **Event log** — append-only, compacted to last 10K events

On restart, the engine recovers all workflows and runs from disk.

---

## Project Structure

```
meshflow/
├── cmd/
│   ├── meshflow/main.go          # CLI entrypoint
│   └── echohandler/main.go       # Echo server for local testing
├── internal/
│   ├── api/
│   │   ├── server.go             # HTTP API server
│   │   └── static/index.html     # Web dashboard (embedded)
│   ├── config/config.go          # Environment-based configuration
│   ├── dag/dag.go                # YAML parser, cycle detection, DAG resolution
│   ├── engine/engine.go          # Orchestration loop, task scheduling, persistence
│   ├── executor/http.go          # HTTP task executor with retry
│   ├── gossip/mesh.go            # SWIM gossip layer (memberlist)
│   ├── node/node.go              # Node: wires gossip, NATS, engine, store, relay
│   ├── pubsub/broker.go          # Embedded NATS with JetStream
│   └── store/store.go            # BoltDB persistence layer
├── pkg/
│   ├── events/event.go           # Event type definitions
│   └── workflow/types.go         # Workflow DAG types
├── configs/
│   ├── sample-etl.yaml           # 3-task ETL with retry policies
│   └── linear-pipeline.yaml      # Simple 3-step linear pipeline
├── docker-compose.yml            # 3-node mesh demo
├── Dockerfile                    # Multi-stage build (~20MB Alpine)
├── go.mod
└── go.sum
```

---

## Use Cases

- **Data pipelines** — ETL/ELT workflows distributed across worker nodes
- **CI/CD orchestration** — Build → test → deploy pipelines without a central server
- **Cron replacement** — Event-driven scheduled tasks with retries and failure handling
- **Microservice orchestration** — Coordinate API calls across services in dependency order
- **Batch processing** — Fan-out work across nodes and aggregate results

---

## Future

See `meshflow-design.md` for the full roadmap. Planned:

- Cron scheduler (actual cron execution, not just placeholder)
- Shell command handler (run any binary as a task)
- gRPC handler (streaming, bidirectional for internal services)
- WebAssembly task sandbox (run untrusted code safely)
- Kubernetes operator (auto-scale nodes)
- Multi-tenancy (isolated workflow namespaces)
- Integration with data connectors (S3, Postgres, Snowflake, etc.)
