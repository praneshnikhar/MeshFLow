package node

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
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

func (n *Node) DeployWorkflow(data []byte) error {
	w, err := dag.Parse(data)
	if err != nil {
		return fmt.Errorf("parse workflow: %w", err)
	}
	return n.engine.Deploy(w)
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
