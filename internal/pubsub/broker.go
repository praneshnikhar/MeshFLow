package pubsub

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/pranesh/meshflow/pkg/events"
)

type Broker struct {
	ns   *server.Server
	nc   *nats.Conn
	js   nats.JetStreamContext
	mu   sync.RWMutex
	subs map[string]*nats.Subscription
	name string
}

func StartEmbedded(name, bindAddr string, port int) (*Broker, error) {
	opts := &server.Options{
		ServerName: name,
		Host:       bindAddr,
		Port:       port,
		JetStream:  true,
		StoreDir:   fmt.Sprintf("./data/nats-%s", name),
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("create nats server: %w", err)
	}

	slog.Info("starting embedded pub/sub", "host", bindAddr, "port", port)
	ns.Start()

	if !ns.ReadyForConnections(15 * time.Second) {
		ns.Shutdown()
		return nil, fmt.Errorf("nats server not ready after 15s")
	}
	slog.Info("nats server ready")

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		return nil, fmt.Errorf("connect to embedded nats: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		ns.Shutdown()
		return nil, fmt.Errorf("create jetstream context: %w", err)
	}

	slog.Info("embedded pub/sub started", "addr", ns.ClientURL())

	return &Broker{
		ns:   ns,
		nc:   nc,
		js:   js,
		subs: make(map[string]*nats.Subscription),
		name: name,
	}, nil
}

func (b *Broker) Publish(evt events.Event) error {
	data, err := evt.Encode()
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	subject := fmt.Sprintf("meshflow.%s", evt.Type)
	_, err = b.js.Publish(subject, data)
	if err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	slog.Debug("event published", "type", evt.Type, "id", evt.ID)
	return nil
}

func (b *Broker) PublishWithID(evt events.Event, msgID string) error {
	data, err := evt.Encode()
	if err != nil {
		return fmt.Errorf("encode event: %w", err)
	}
	subject := fmt.Sprintf("meshflow.%s", evt.Type)
	_, err = b.js.PublishMsg(&nats.Msg{
		Subject: subject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": []string{msgID}},
	})
	if err != nil {
		return fmt.Errorf("publish event with id: %w", err)
	}
	slog.Debug("event published with dedup", "type", evt.Type, "msg_id", msgID)
	return nil
}

func (b *Broker) Subscribe(eventType events.EventType, handler func(events.Event)) error {
	subject := fmt.Sprintf("meshflow.%s", eventType)

	streamName := fmt.Sprintf("EVENTS_%s", sanitizeStreamName(string(eventType)))
	_, err := b.js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
		MaxAge:   time.Hour,
		Storage:  nats.FileStorage,
	})
	if err != nil {
		return fmt.Errorf("add stream: %w", err)
	}

	sub, err := b.js.Subscribe(subject, func(msg *nats.Msg) {
		evt, decErr := events.Decode(msg.Data)
		if decErr != nil {
			slog.Error("failed to decode event", "error", decErr)
			return
		}
		handler(evt)
	}, nats.Durable(fmt.Sprintf("%s-%s", sanitizeStreamName(b.name), sanitizeStreamName(string(eventType)))))
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}

	b.mu.Lock()
	b.subs[string(eventType)] = sub
	b.mu.Unlock()

	return nil
}

func (b *Broker) ClientURL() string {
	return b.ns.ClientURL()
}

func (b *Broker) Shutdown() {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, sub := range b.subs {
		sub.Unsubscribe()
	}
	b.nc.Close()
	b.ns.Shutdown()
}

func (b *Broker) SubscribeRaw(subject string) (chan []byte, *nats.Subscription, error) {
	ch := make(chan []byte, 256)
	sub, err := b.nc.Subscribe(subject, func(msg *nats.Msg) {
		data := make([]byte, len(msg.Data))
		copy(data, msg.Data)
		ch <- data
	})
	if err != nil {
		return nil, nil, err
	}
	return ch, sub, nil
}

func sanitizeStreamName(s string) string {
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			result = append(result, c)
		} else {
			result = append(result, '_')
		}
	}
	return string(result)
}
