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

// testSelf is a node identity with a fixed name and zero stats.
func testSelf(name string) Self {
	s := NewSelf("test", time.Now(), func() (int, int, int) { return 0, 0, 0 })
	s.Name = name
	return s
}

// waitPresence polls until ok(rp.Nodes()) holds or the deadline hits.
func waitPresence(t *testing.T, rp *RedisPublisher, what string, ok func([]NodeInfo) bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok(rp.Nodes()) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s, got %+v", what, rp.Nodes())
}

func aliveCount(nodes []NodeInfo) int {
	n := 0
	for _, x := range nodes {
		if x.Alive {
			n++
		}
	}
	return n
}

// fastPresence shortens the presence timings for the duration of the test.
func fastPresence(t *testing.T, hb, alive, forget time.Duration) {
	t.Helper()
	oh, oa, of := heartbeatInterval, nodeAliveAfter, nodeForgetAfter
	heartbeatInterval, nodeAliveAfter, nodeForgetAfter = hb, alive, forget
	t.Cleanup(func() { heartbeatInterval, nodeAliveAfter, nodeForgetAfter = oh, oa, of })
}

// crash stops a publisher's goroutines without removing its roster entry.
func crash(rp *RedisPublisher) {
	rp.cancel()
	<-rp.monitorDone
	<-rp.hbDone
}

func TestNoopPublisher(t *testing.T) {
	mgr := sub.NewManager()
	s := &recvSub{}
	mgr.Subscribe("t", s)

	p := NewNoopPublisher(mgr, testSelf("a"))
	p.Publish("t", json.RawMessage(`{"x":1}`))

	if s.count() != 1 {
		t.Fatalf("expected local delivery, got %d", s.count())
	}
	if p.ClusterNodes() != -1 {
		t.Fatalf("NoopPublisher.ClusterNodes(): got %d, want -1", p.ClusterNodes())
	}
	nodes := p.Nodes()
	if len(nodes) != 1 || nodes[0].Name != "a" || !nodes[0].Alive || nodes[0].Version != "test" {
		t.Fatalf("NoopPublisher.Nodes(): got %+v, want self alive", nodes)
	}
}

func TestNewRedisPublisherBadURL(t *testing.T) {
	if _, err := NewRedisPublisher("not-a-redis-url", sub.NewManager(), testSelf("a")); err == nil {
		t.Fatal("expected error for invalid redis URL")
	}
}

func TestRedisPublisherLocalDelivery(t *testing.T) {
	url := newMiniredis(t)
	mgr := sub.NewManager()
	s := &recvSub{}
	mgr.Subscribe("t", s)

	rp, err := NewRedisPublisher(url, mgr, testSelf("a"))
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

	rpA, err := NewRedisPublisher(url, mgrA, testSelf("a"))
	if err != nil {
		t.Fatalf("NewRedisPublisher A: %v", err)
	}
	defer rpA.Close()
	rpB, err := NewRedisPublisher(url, mgrB, testSelf("b"))
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
	rp, err := NewRedisPublisher(url, sub.NewManager(), testSelf("a"))
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

// waitReady polls until Ready() reports want or the deadline hits.
func waitReady(t *testing.T, rp *RedisPublisher, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rp.Ready() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for Ready()==%v", want)
}

// Redis unreachable at startup, reachable later; then lost and back again.
// The publisher must never fall back permanently and must resume cluster
// delivery on its own each time Redis returns.
func TestRedisPublisherReconnect(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	url := "redis://" + mr.Addr()
	mr.Close() // Redis down before the workers start
	t.Cleanup(mr.Close)

	origMin, origMax, origHealth := reconnectMin, reconnectMax, healthInterval
	reconnectMin, reconnectMax, healthInterval = 20*time.Millisecond, 100*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { reconnectMin, reconnectMax, healthInterval = origMin, origMax, origHealth })

	mgrA, mgrB := sub.NewManager(), sub.NewManager()
	rpA, err := NewRedisPublisher(url, mgrA, testSelf("a"))
	if err != nil {
		t.Fatalf("NewRedisPublisher A must not fail on unreachable redis: %v", err)
	}
	defer rpA.Close()
	rpB, err := NewRedisPublisher(url, mgrB, testSelf("b"))
	if err != nil {
		t.Fatalf("NewRedisPublisher B must not fail on unreachable redis: %v", err)
	}
	defer rpB.Close()

	waitReady(t, rpA, false)
	waitReady(t, rpB, false)

	sB := &recvSub{}
	mgrB.Subscribe("t", sB)

	for round := 1; round <= 2; round++ {
		if err := mr.Restart(); err != nil {
			t.Fatalf("round %d: miniredis restart: %v", round, err)
		}
		waitReady(t, rpA, true)
		waitReady(t, rpB, true)
		waitNodes(t, rpA, 2) // both resubscribed to the channel

		rpA.Publish("t", json.RawMessage(`{"round":1}`))
		waitCount(t, sB, round) // arrived cross-instance after reconnect

		mr.Close()
		waitReady(t, rpA, false)
		waitReady(t, rpB, false)
	}
}

func TestRedisPresenceTwoNodes(t *testing.T) {
	url := newMiniredis(t)
	rpA, err := NewRedisPublisher(url, sub.NewManager(), testSelf("b"))
	if err != nil {
		t.Fatalf("NewRedisPublisher A: %v", err)
	}
	defer rpA.Close()
	rpB, err := NewRedisPublisher(url, sub.NewManager(), testSelf("a"))
	if err != nil {
		t.Fatalf("NewRedisPublisher B: %v", err)
	}

	// Both wrote their entry synchronously in the constructor.
	nodes := rpA.Nodes()
	if len(nodes) != 2 || nodes[0].Name != "a" || nodes[1].Name != "b" {
		t.Fatalf("Nodes: got %+v, want a,b sorted by name", nodes)
	}
	if aliveCount(nodes) != 2 {
		t.Fatalf("both nodes should be alive: %+v", nodes)
	}

	// Clean shutdown removes the entry immediately: never "degraded".
	rpB.Close()
	nodes = rpA.Nodes()
	if len(nodes) != 1 || nodes[0].Name != "b" {
		t.Fatalf("after Close: got %+v, want only b", nodes)
	}
}

func TestRedisPresenceCrash(t *testing.T) {
	fastPresence(t, 20*time.Millisecond, 100*time.Millisecond, 300*time.Millisecond)
	url := newMiniredis(t)
	rpA, err := NewRedisPublisher(url, sub.NewManager(), testSelf("a"))
	if err != nil {
		t.Fatalf("NewRedisPublisher A: %v", err)
	}
	defer rpA.Close()
	rpB, err := NewRedisPublisher(url, sub.NewManager(), testSelf("b"))
	if err != nil {
		t.Fatalf("NewRedisPublisher B: %v", err)
	}
	t.Cleanup(func() { rpB.client.Close() })

	crash(rpB)

	// Dead but still listed: Wavelog shows 1/2 degraded.
	waitPresence(t, rpA, "b dead", func(n []NodeInfo) bool { return len(n) == 2 && aliveCount(n) == 1 })
	for _, n := range rpA.Nodes() {
		if n.Name == "b" && n.Alive {
			t.Fatalf("b should be dead: %+v", n)
		}
	}
	// Forgotten after the grace window, pruned by A's heartbeat.
	waitPresence(t, rpA, "b forgotten", func(n []NodeInfo) bool { return len(n) == 1 && n[0].Name == "a" })
}

func TestRedisPresenceSameNameReplaces(t *testing.T) {
	fastPresence(t, 20*time.Millisecond, 100*time.Millisecond, time.Hour)
	url := newMiniredis(t)
	rpOld, err := NewRedisPublisher(url, sub.NewManager(), testSelf("pod-1"))
	if err != nil {
		t.Fatalf("NewRedisPublisher old: %v", err)
	}
	t.Cleanup(func() { rpOld.client.Close() })
	crash(rpOld)

	// Same hostname comes back (crash loop): the dead entry must go at once,
	// long before the grace window.
	rpNew, err := NewRedisPublisher(url, sub.NewManager(), testSelf("pod-1"))
	if err != nil {
		t.Fatalf("NewRedisPublisher new: %v", err)
	}
	defer rpNew.Close()
	waitPresence(t, rpNew, "old pod-1 replaced", func(n []NodeInfo) bool {
		return len(n) == 1 && n[0].ID == rpNew.self.ID && n[0].Alive
	})
}

// A topic literally named "nodes" lives under wavelog:topic: and must not
// interfere with the wavelog:nodes roster hash.
func TestPresenceAndTopicKeysDisjoint(t *testing.T) {
	url := newMiniredis(t)
	rp, err := NewRedisPublisher(url, sub.NewManager(), testSelf("a"))
	if err != nil {
		t.Fatalf("NewRedisPublisher: %v", err)
	}
	defer rp.Close()
	reg := NewRedisRegistry(rp.Client(), rp.Context())
	reg.Register("nodes", registry.TopicMeta{})

	if n := rp.Nodes(); len(n) != 1 || n[0].Name != "a" {
		t.Fatalf("Nodes: got %+v, want only a", n)
	}
	if topics := reg.Topics(); len(topics) != 1 || topics[0] != "nodes" {
		t.Fatalf("Topics: got %v, want [nodes]", topics)
	}
}
