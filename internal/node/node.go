package node

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pranesh/meshflow/internal/config"
	"github.com/pranesh/meshflow/internal/dag"
	"github.com/pranesh/meshflow/internal/engine"
	"github.com/pranesh/meshflow/internal/gossip"
	"github.com/pranesh/meshflow/internal/pubsub"
	"github.com/pranesh/meshflow/internal/store"
	"github.com/pranesh/meshflow/pkg/events"
	"github.com/pranesh/meshflow/pkg/workflow"
)

type 	Node struct {
	cfg     *config.Config
	mesh    *gossip.Mesh
	broker  *pubsub.Broker
	engine  *engine.Engine
	stopCh  chan struct{}

	mu         sync.Mutex
	seenEvents map[string]bool
	seenSeq    int
}

func New(cfg *config.Config) *Node {
	return &Node{
		cfg:        cfg,
		stopCh:     make(chan struct{}),
		seenEvents: make(map[string]bool),
	}
}

func (n *Node) Start() error {
	slog.Info("starting meshflow node",
		"name", n.cfg.NodeName,
		"bind_addr", n.cfg.BindAddr,
		"port", n.cfg.BindPort,
	)

	if err := os.MkdirAll(n.cfg.DataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	broker, err := pubsub.StartEmbedded(
		n.cfg.NodeName,
		n.cfg.BindAddr,
		n.cfg.PubSubPort,
	)
	if err != nil {
		return fmt.Errorf("start pub/sub broker: %w", err)
	}
	n.broker = broker

	mesh, err := gossip.New(
		n.cfg.NodeName,
		n.cfg.BindAddr,
		n.cfg.GossipPort,
		n.onNodeJoin,
		n.onNodeLeave,
		n.onNodeUpdate,
		n.onGossipMsg,
	)
	if err != nil {
		broker.Shutdown()
		return fmt.Errorf("start gossip mesh: %w", err)
	}
	n.mesh = mesh

	n.mesh.SetMeta("pubsub_addr", fmt.Sprintf("%s:%d", n.cfg.BindAddr, n.cfg.PubSubPort))
	n.mesh.SetMeta("started_at", time.Now().UTC().Format(time.RFC3339))

	if err := n.broker.Subscribe(events.EventNodeJoined, n.handleNodeJoined); err != nil {
		slog.Warn("failed to subscribe to node.joined events", "error", err)
	}
	if err := n.broker.Subscribe(events.EventNodeLeft, n.handleNodeLeft); err != nil {
		slog.Warn("failed to subscribe to node.left events", "error", err)
	}

	st, err := store.Open(n.cfg.DataDir)
	if err != nil {
		mesh.Shutdown()
		broker.Shutdown()
		return fmt.Errorf("open store: %w", err)
	}

	n.engine = engine.New(n.cfg.NodeName, broker, st)
	if err := n.engine.Start(); err != nil {
		mesh.Shutdown()
		broker.Shutdown()
		return fmt.Errorf("start dag engine: %w", err)
	}

	if len(n.cfg.SeedNodes) > 0 {
		slog.Info("joining mesh via seeds", "seeds", n.cfg.SeedNodes)
		joined, err := n.mesh.Join(n.cfg.SeedNodes)
		if err != nil {
			slog.Warn("mesh join had issues", "error", err, "joined", joined)
		} else {
			slog.Info("joined mesh", "peers_contacted", joined)
		}
	} else {
		slog.Info("no seed nodes configured, running as first node")
	}

	n.broker.Publish(events.NewEvent(events.EventNodeJoined, n.cfg.NodeName, map[string]interface{}{
		"name":    n.cfg.NodeName,
		"address": n.cfg.AdvertiseAddr(),
	}))

	go n.heartbeatLoop()
	go n.gossipRelay()
	go n.cronLoop()
	go n.topicSyncLoop()

	slog.Info("meshflow node ready",
		"name", n.cfg.NodeName,
		"gossip_addr", n.cfg.GossipAddr(),
		"pubsub_addr", broker.ClientURL(),
		"peers", n.mesh.NumMembers(),
	)

	return nil
}

func (n *Node) gossipRelay() {
	for _, et := range allEventTypes() {
		err := n.broker.Subscribe(et, n.onLocalEvent)
		if err != nil {
			slog.Debug("relay subscription skipped", "event", et, "reason", err)
		}
	}
}

func allEventTypes() []events.EventType {
	return []events.EventType{
		events.EventWorkflowTriggered,
		events.EventTaskClaimed,
		events.EventTaskStarted,
		events.EventTaskCompleted,
		events.EventTaskFailed,
		events.EventWorkflowCompleted,
		events.EventNodeJoined,
		events.EventNodeLeft,
		events.EventHeartbeat,
	}
}

func (n *Node) onLocalEvent(evt events.Event) {
	n.mu.Lock()
	key := evt.ID
	if n.seenEvents[key] {
		n.mu.Unlock()
		return
	}
	n.seenEvents[key] = true
	n.seenSeq++
	if n.seenSeq > 10000 {
		n.seenEvents = make(map[string]bool)
		n.seenSeq = 0
	}
	n.mu.Unlock()

	data, err := evt.Encode()
	if err != nil {
		return
	}
	data = append([]byte("EVT:"), data...)
	n.mesh.BroadcastEvent(data)
}

func (n *Node) onGossipMsg(data []byte) {
	if len(data) < 4 || string(data[:4]) != "EVT:" {
		return
	}

	evt, err := events.Decode(data[4:])
	if err != nil {
		slog.Debug("failed to decode gossip event", "error", err)
		return
	}

	n.mu.Lock()
	if n.seenEvents[evt.ID] {
		n.mu.Unlock()
		return
	}
	n.seenEvents[evt.ID] = true
	n.mu.Unlock()

	if err := n.broker.Publish(evt); err != nil {
		slog.Warn("failed to publish relayed event", "type", evt.Type, "error", err)
	}
}

func (n *Node) Wait() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	n.Shutdown()
}

func (n *Node) Shutdown() {
	slog.Info("shutting down meshflow node", "name", n.cfg.NodeName)

	n.broker.Publish(events.NewEvent(events.EventNodeLeft, n.cfg.NodeName, map[string]interface{}{
		"name": n.cfg.NodeName,
	}))

	close(n.stopCh)
	time.Sleep(300 * time.Millisecond)

	if n.engine != nil {
		n.engine.Shutdown()
	}
	if n.mesh != nil {
		if err := n.mesh.Shutdown(); err != nil {
			slog.Error("gossip shutdown error", "error", err)
		}
	}
	if n.broker != nil {
		n.broker.Shutdown()
	}
	slog.Info("meshflow node stopped", "name", n.cfg.NodeName)
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.broker.Publish(events.NewEvent(events.EventHeartbeat, n.cfg.NodeName, map[string]interface{}{
				"name":      n.cfg.NodeName,
				"peers":     n.mesh.NumMembers(),
				"timestamp": time.Now().UTC().Format(time.RFC3339),
			}))
		}
	}
}

func (n *Node) onNodeJoin(name string) {
	n.broker.Publish(events.NewEvent(events.EventNodeJoined, n.cfg.NodeName, map[string]interface{}{
		"name": name,
	}))
}

func (n *Node) onNodeLeave(name string) {
	n.broker.Publish(events.NewEvent(events.EventNodeLeft, n.cfg.NodeName, map[string]interface{}{
		"name": name,
	}))
}

func (n *Node) onNodeUpdate(name string) {}

func (n *Node) handleNodeJoined(evt events.Event) {
	name, _ := evt.Payload["name"].(string)
	if name != n.cfg.NodeName {
		slog.Info("mesh event: node joined", "node", name)
	}
}

func (n *Node) handleNodeLeft(evt events.Event) {
	name, _ := evt.Payload["name"].(string)
	if name != n.cfg.NodeName {
		slog.Info("mesh event: node left", "node", name)
	}
}

func (n *Node) cronLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			if n.engine == nil {
				continue
			}
			now := time.Now()
			for _, name := range n.engine.GetWorkflows() {
				w := n.engine.GetWorkflow(name)
				if w == nil {
					continue
				}
				for _, trigger := range w.Triggers {
					if trigger.Cron != "" && cronMatches(trigger.Cron, now) {
						slog.Info("cron trigger fired", "workflow", w.Name, "cron", trigger.Cron)
						n.engine.Trigger(w.Name)
					}
				}
			}
		}
	}
}

func cronMatches(expr string, t time.Time) bool {
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return false
	}
	return matchCronField(parts[0], t.Minute()) &&
		matchCronField(parts[1], t.Hour()) &&
		matchCronField(parts[2], t.Day()) &&
		matchCronField(parts[3], int(t.Month())) &&
		matchCronField(parts[4], int(t.Weekday()))
}

func matchCronField(field string, value int) bool {
	if field == "*" {
		return true
	}
	for _, part := range strings.Split(field, ",") {
		if strings.Contains(part, "/") {
			split := strings.SplitN(part, "/", 2)
			step, err := strconv.Atoi(split[1])
			if err != nil {
				continue
			}
			start := 0
			if split[0] != "*" {
				start, _ = strconv.Atoi(split[0])
			}
			if value >= start && (value-start)%step == 0 {
				return true
			}
		} else if strings.Contains(part, "-") {
			split := strings.SplitN(part, "-", 2)
			start, err1 := strconv.Atoi(split[0])
			end, err2 := strconv.Atoi(split[1])
			if err1 == nil && err2 == nil && value >= start && value <= end {
				return true
			}
		} else {
			v, err := strconv.Atoi(part)
			if err == nil && v == value {
				return true
			}
		}
	}
	return false
}

func (n *Node) topicSyncLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			topics := n.gatherTopics()
			if len(topics) > 0 {
				n.mesh.SetMeta("topics", strings.Join(topics, ","))
			}
		}
	}
}

func (n *Node) gatherTopics() []string {
	seen := make(map[string]bool)
	for _, name := range n.engine.GetWorkflows() {
		w := n.engine.GetWorkflow(name)
		if w == nil {
			continue
		}
		for _, t := range w.Tasks {
			for k := range t.Input {
				seen[k] = true
			}
		}
	}
	topics := make([]string, 0, len(seen))
	for k := range seen {
		topics = append(topics, k)
	}
	return topics
}

func (n *Node) DeployWorkflow(data []byte) error {
	w, err := dag.Parse(data)
	if err != nil {
		return fmt.Errorf("parse workflow: %w", err)
	}
	return n.engine.Deploy(w)
}

func (n *Node) InstantiateTemplate(data []byte, params map[string]string) error {
	w, err := dag.Parse(data)
	if err != nil {
		return fmt.Errorf("parse workflow: %w", err)
	}
	if !w.Template {
		return fmt.Errorf("workflow %s is not a template", w.Name)
	}

	rendered := renderTemplate(w, params)
	return n.engine.Deploy(rendered)
}

func renderTemplate(w *workflow.Workflow, params map[string]string) *workflow.Workflow {
	instance := *w
	instance.Template = false
	instance.Name = replaceParams(w.Name, params)
	instance.Description = replaceParams(w.Description, params)

	tasks := make([]workflow.Task, len(w.Tasks))
	for i, t := range w.Tasks {
		tasks[i] = t
		tasks[i].ID = replaceParams(t.ID, params)
		tasks[i].Handler = replaceParams(t.Handler, params)
		newDeps := make([]string, len(t.DependsOn))
		for j, d := range t.DependsOn {
			newDeps[j] = replaceParams(d, params)
		}
		tasks[i].DependsOn = newDeps
		if tasks[i].Condition != "" {
			tasks[i].Condition = replaceParams(t.Condition, params)
		}
	}
	instance.Tasks = tasks
	return &instance
}

func replaceParams(s string, params map[string]string) string {
	result := s
	for k, v := range params {
		result = strings.ReplaceAll(result, "{{"+k+"}}", v)
	}
	return result
}

func (n *Node) TriggerWorkflow(name string) error {
	return n.engine.Trigger(name)
}

func (n *Node) ListWorkflows() []string {
	return n.engine.GetWorkflows()
}

func (n *Node) ListRuns(workflowName string) []string {
	return n.engine.GetRuns(workflowName)
}

func (n *Node) GetRun(runID string) interface{} {
	return n.engine.GetRun(runID)
}

func (n *Node) GetWorkflowDef(name string) interface{} {
	return n.engine.GetWorkflow(name)
}

func (n *Node) MeshMembers() []string {
	return n.mesh.Members()
}

func (n *Node) SubscribeEvents() (chan []byte, func(), error) {
	ch, sub, err := n.broker.SubscribeRaw("meshflow.*.*")
	if err != nil {
		return nil, nil, err
	}
	cancel := func() {
		sub.Unsubscribe()
	}
	return ch, cancel, nil
}

func (n *Node) Mesh() *gossip.Mesh {
	return n.mesh
}

func (n *Node) Broker() *pubsub.Broker {
	return n.broker
}

func (n *Node) Config() *config.Config {
	return n.cfg
}
