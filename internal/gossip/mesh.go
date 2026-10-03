package gossip

import (
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

type Delegate struct {
	mu       sync.RWMutex
	meta     map[string]string
	onJoin   func(string)
	onLeave  func(string)
	onUpdate func(string)
	onMsg    func([]byte)
}

func NewDelegate(onJoin, onLeave, onUpdate func(string), onMsg func([]byte)) *Delegate {
	return &Delegate{
		meta:     make(map[string]string),
		onJoin:   onJoin,
		onLeave:  onLeave,
		onUpdate: onUpdate,
		onMsg:    onMsg,
	}
}

func (d *Delegate) SetMeta(key, value string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.meta[key] = value
}

func (d *Delegate) NodeMeta(limit int) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return []byte(fmt.Sprintf("%v", d.meta))
}

func (d *Delegate) nodeMeta(node *memberlist.Node) []byte {
	return node.Meta
}

func (d *Delegate) NotifyMsg(msg []byte) {
	if d.onMsg != nil && len(msg) > 0 {
		d.onMsg(msg)
	}
}

func (d *Delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (d *Delegate) LocalState(join bool) []byte {
	return nil
}

func (d *Delegate) MergeRemoteState(buf []byte, join bool) {}

func (d *Delegate) NotifyJoin(node *memberlist.Node) {
	slog.Info("node joined mesh", "name", node.Name, "addr", node.Addr.String())
	if d.onJoin != nil {
		d.onJoin(node.Name)
	}
}

func (d *Delegate) NotifyLeave(node *memberlist.Node) {
	slog.Info("node left mesh", "name", node.Name, "addr", node.Addr.String())
	if d.onLeave != nil {
		d.onLeave(node.Name)
	}
}

func (d *Delegate) NotifyUpdate(node *memberlist.Node) {
	slog.Info("node updated", "name", node.Name, "addr", node.Addr.String())
	if d.onUpdate != nil {
		d.onUpdate(node.Name)
	}
}

type Mesh struct {
	list     *memberlist.Memberlist
	delegate *Delegate
	config   *memberlist.Config
	mu       sync.RWMutex
}

func New(name, bindAddr string, bindPort int, onJoin, onLeave, onUpdate func(string), onMsg func([]byte)) (*Mesh, error) {
	conf := memberlist.DefaultLocalConfig()
	conf.Name = name
	conf.BindAddr = bindAddr
	conf.BindPort = bindPort
	conf.AdvertiseAddr = bindAddr
	conf.AdvertisePort = bindPort
	conf.LogOutput = nil
	conf.TCPTimeout = 10 * time.Second
	conf.ProbeInterval = 1 * time.Second
	conf.ProbeTimeout = 500 * time.Millisecond
	conf.SuspicionMult = 4
	conf.PushPullInterval = 30 * time.Second
	conf.GossipInterval = 200 * time.Millisecond
	conf.GossipNodes = 3
	conf.GossipToTheDeadTime = 30 * time.Second
	conf.DeadNodeReclaimTime = 60 * time.Second

	del := NewDelegate(onJoin, onLeave, onUpdate, onMsg)
	conf.Delegate = del
	conf.Events = del

	list, err := memberlist.Create(conf)
	if err != nil {
		return nil, fmt.Errorf("create memberlist: %w", err)
	}

	return &Mesh{
		list:     list,
		delegate: del,
		config:   conf,
	}, nil
}

func (m *Mesh) Join(seeds []string) (int, error) {
	if len(seeds) == 0 {
		return 0, nil
	}

	parsed := make([]string, 0, len(seeds))
	for _, s := range seeds {
		host, port, err := net.SplitHostPort(s)
		if err != nil {
			host = s
			port = fmt.Sprintf("%d", m.config.BindPort)
		}
		ips, err := net.LookupIP(host)
		if err != nil {
			slog.Warn("failed to resolve seed", "seed", s, "error", err)
			continue
		}
		for _, ip := range ips {
			if ip.To4() != nil {
				parsed = append(parsed, fmt.Sprintf("%s:%s", ip.String(), port))
				break
			}
		}
	}

	n, err := m.list.Join(parsed)
	if err != nil {
		return n, fmt.Errorf("join mesh: %w", err)
	}
	return n, nil
}

func (m *Mesh) BroadcastEvent(data []byte) {
	m.BroadcastEventToTopics(data, nil)
}

func (m *Mesh) BroadcastEventToTopics(data []byte, topics []string) {
	for _, node := range m.list.Members() {
		if node.Name == m.list.LocalNode().Name {
			continue
		}
		if len(topics) > 0 {
			metaBytes := m.delegate.nodeMeta(node)
			nodeTopics := parseNodeTopics(metaBytes)
			if !hasAnyTopic(nodeTopics, topics) {
				continue
			}
		}
		m.list.SendReliable(node, data)
	}
}

func (m *Mesh) Members() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := m.list.Members()
	names := make([]string, 0, len(nodes))
	for _, n := range nodes {
		names = append(names, fmt.Sprintf("%s (%s:%d)", n.Name, n.Addr.String(), n.Port))
	}
	return names
}

func (m *Mesh) NumMembers() int {
	return m.list.NumMembers()
}

func (m *Mesh) LocalNode() *memberlist.Node {
	return m.list.LocalNode()
}

func (m *Mesh) SetMeta(key, value string) {
	m.delegate.SetMeta(key, value)
}

func (m *Mesh) Shutdown() error {
	return m.list.Shutdown()
}

func parseNodeTopics(meta []byte) []string {
	metaStr := string(meta)
	start := strings.Index(metaStr, "topics:[")
	if start < 0 {
		return nil
	}
	start += len("topics:[")
	end := strings.Index(metaStr[start:], "]")
	if end < 0 {
		return nil
	}
	return strings.Split(metaStr[start:start+end], " ")
}

func hasAnyTopic(nodeTopics, eventTopics []string) bool {
	for _, et := range eventTopics {
		for _, nt := range nodeTopics {
			if et == nt {
				return true
			}
		}
	}
	return false
}
