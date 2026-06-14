package cluster

import (
	"encoding/json"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/wavelog/wavelog_worker/internal/registry"
	"github.com/wavelog/wavelog_worker/internal/sub"
)

// recvSub collects payloads delivered to it.
type recvSub struct {
	mu   sync.Mutex
	msgs []json.RawMessage
}

func (s *recvSub) Send(p json.RawMessage) {
	s.mu.Lock()
	s.msgs = append(s.msgs, p)
	s.mu.Unlock()
}

func (s *recvSub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

// waitCount polls until the subscriber reached n messages or the deadline hits.
func waitCount(t *testing.T, s *recvSub, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.count() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d messages, got %d", n, s.count())
}

// waitNodes polls until the publisher reports n subscribed cluster nodes.
func waitNodes(t *testing.T, rp *RedisPublisher, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rp.ClusterNodes() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d cluster nodes, got %d", n, rp.ClusterNodes())
}

func newMiniredis(t *testing.T) string {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return "redis://" + mr.Addr()
}

func TestNoopPublisher(t *testing.T) {
	mgr := sub.NewManager()
	s := &recvSub{}
	mgr.Subscribe("t", s)

	p := NewNoopPublisher(mgr)
	p.Publish("t", json.RawMessage(`{"x":1}`))

	if s.count() != 1 {
		t.Fatalf("expected local delivery, got %d", s.count())
	}
	if p.ClusterNodes() != -1 {
		t.Fatalf("NoopPublisher.ClusterNodes(): got %d, want -1", p.ClusterNodes())
	}
}

func TestNewRedisPublisherBadURL(t *testing.T) {
	if _, err := NewRedisPublisher("not-a-redis-url", sub.NewManager()); err == nil {
		t.Fatal("expected error for invalid redis URL")
	}
}

func TestRedisPublisherLocalDelivery(t *testing.T) {
	url := newMiniredis(t)
	mgr := sub.NewManager()
	s := &recvSub{}
	mgr.Subscribe("t", s)

	rp, err := NewRedisPublisher(url, mgr)
	if err != nil {
		t.Fatalf("NewRedisPublisher: %v", err)
	}
	defer rp.Close()

	rp.Publish("t", json.RawMessage(`{"x":1}`))
	// Local delivery is synchronous, no need to wait.
	if s.count() != 1 {
		t.Fatalf("expected immediate local delivery, got %d", s.count())
	}
}

func TestRedisPublisherCrossInstance(t *testing.T) {
	url := newMiniredis(t)

	mgrA := sub.NewManager()
	mgrB := sub.NewManager()

	rpA, err := NewRedisPublisher(url, mgrA)
	if err != nil {
		t.Fatalf("NewRedisPublisher A: %v", err)
	}
	defer rpA.Close()
	rpB, err := NewRedisPublisher(url, mgrB)
	if err != nil {
		t.Fatalf("NewRedisPublisher B: %v", err)
	}
	defer rpB.Close()

	sA := &recvSub{}
	sB := &recvSub{}
	mgrA.Subscribe("t", sA)
	mgrB.Subscribe("t", sB)

	// Pub/Sub has no persistence: wait until both subscriber goroutines are
	// actually subscribed to the Redis channel before publishing, otherwise the
	// message can be dropped before B is listening.
	waitNodes(t, rpA, 2)

	// Publish on A: A's subscriber gets it locally, B's gets it via Redis.
	rpA.Publish("t", json.RawMessage(`{"from":"a"}`))

	waitCount(t, sB, 1) // arrived across instances
	if sA.count() != 1 {
		t.Fatalf("A subscriber: got %d, want 1", sA.count())
	}

	// Give the subscriber goroutine a moment; A must NOT receive its own
	// message a second time via the Redis loop (OriginID skip).
	time.Sleep(100 * time.Millisecond)
	if sA.count() != 1 {
		t.Fatalf("A received its own message twice: got %d", sA.count())
	}
	if sB.count() != 1 {
		t.Fatalf("B subscriber: got %d, want 1", sB.count())
	}
}

func TestRedisRegistry(t *testing.T) {
	url := newMiniredis(t)
	rp, err := NewRedisPublisher(url, sub.NewManager())
	if err != nil {
		t.Fatalf("NewRedisPublisher: %v", err)
	}
	defer rp.Close()

	reg := NewRedisRegistry(rp.Client(), rp.Context())

	if _, ok := reg.Lookup("unknown"); ok {
		t.Fatal("Lookup of unknown topic should be false")
	}

	reg.Register("a", registry.TopicMeta{RequireToken: true})
	reg.Register("b", registry.TopicMeta{RequireToken: false})

	meta, ok := reg.Lookup("a")
	if !ok || !meta.RequireToken {
		t.Fatalf("Lookup a: ok=%v meta=%+v", ok, meta)
	}

	topics := reg.Topics()
	sort.Strings(topics)
	if len(topics) != 2 || topics[0] != "a" || topics[1] != "b" {
		t.Fatalf("Topics (prefix should be stripped): %v", topics)
	}

	reg.Unregister("a")
	if _, ok := reg.Lookup("a"); ok {
		t.Fatal("topic a should be gone after Unregister")
	}
}
