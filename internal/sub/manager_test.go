package sub

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

// recvSub collects every payload it is sent.
type recvSub struct {
	mu   sync.Mutex
	msgs []json.RawMessage
}

func (s *recvSub) Send(payload json.RawMessage) {
	s.mu.Lock()
	s.msgs = append(s.msgs, payload)
	s.mu.Unlock()
}

func (s *recvSub) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.msgs)
}

// blockingSub never drains, simulating a slow client with a full buffer.
type blockingSub struct{ ch chan struct{} }

func (b *blockingSub) Send(json.RawMessage) { <-b.ch } // would block forever

func TestSubscribePublish(t *testing.T) {
	m := NewManager()
	s := &recvSub{}
	m.Subscribe("topic", s)

	m.Publish("topic", json.RawMessage(`{"x":1}`))
	if s.count() != 1 {
		t.Fatalf("expected 1 message, got %d", s.count())
	}

	// Publishing to a topic with no subscribers is a no-op.
	m.Publish("other", json.RawMessage(`{}`))
	if s.count() != 1 {
		t.Fatalf("subscriber received unrelated message")
	}
}

func TestUnsubscribe(t *testing.T) {
	m := NewManager()
	s := &recvSub{}
	m.Subscribe("topic", s)
	m.Unsubscribe("topic", s)

	if m.HasSubscribers("topic") {
		t.Fatal("topic should have no subscribers")
	}
	// Topic map entry should be cleaned up.
	if len(m.Topics()) != 0 {
		t.Fatalf("expected no topics, got %v", m.Topics())
	}
	m.Publish("topic", json.RawMessage(`{}`))
	if s.count() != 0 {
		t.Fatal("unsubscribed client received message")
	}
}

func TestUnsubscribeAll(t *testing.T) {
	m := NewManager()
	s := &recvSub{}
	m.Subscribe("a", s)
	m.Subscribe("b", s)

	m.UnsubscribeAll(s)
	if len(m.Topics()) != 0 {
		t.Fatalf("expected all topics removed, got %v", m.Topics())
	}
}

func TestHasSubscribersAndStats(t *testing.T) {
	m := NewManager()
	s1 := &recvSub{}
	s2 := &recvSub{}
	m.Subscribe("a", s1)
	m.Subscribe("a", s2)
	m.Subscribe("b", s1)

	if !m.HasSubscribers("a") || !m.HasSubscribers("b") {
		t.Fatal("expected subscribers on a and b")
	}
	if m.HasSubscribers("c") {
		t.Fatal("topic c should have no subscribers")
	}

	topics, clients := m.Stats()
	if topics != 2 {
		t.Errorf("Stats topics: got %d, want 2", topics)
	}
	// a has 2 subscribers, b has 1 → 3 total.
	if clients != 3 {
		t.Errorf("Stats clients: got %d, want 3", clients)
	}
}

func TestPublishNonBlocking(t *testing.T) {
	m := NewManager()
	b := &blockingSub{ch: make(chan struct{})}
	defer close(b.ch)

	// A real ws.Client never blocks in Send (drops frames), but verify the
	// Manager itself does not hold the lock across a slow Send by ensuring a
	// concurrent operation can proceed while one Send is in-flight.
	done := make(chan struct{})
	go func() {
		m.Subscribe("t", b)
		m.Publish("t", json.RawMessage(`{}`)) // will block in blockingSub.Send
		close(done)
	}()

	// Manager operations on a different topic must not deadlock.
	m.Subscribe("other", &recvSub{})
	m.HasSubscribers("other")
	m.Topics()
	m.Stats()

	b.ch <- struct{}{} // unblock the in-flight Send
	<-done
}

func TestManagerConcurrent(t *testing.T) {
	m := NewManager()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			s := &recvSub{}
			topic := fmt.Sprintf("t%d", n%5)
			m.Subscribe(topic, s)
			m.Publish(topic, json.RawMessage(`{}`))
			m.Stats()
			m.Topics()
			m.Unsubscribe(topic, s)
		}(i)
	}
	wg.Wait()
}
