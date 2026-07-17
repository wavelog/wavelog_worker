package sub

import (
	"encoding/json"
	"sync"
)

// Subscriber is anything that can receive a push payload.
// Implemented by ws.Client — kept as interface to avoid circular imports.
type Subscriber interface {
	Send(payload json.RawMessage)
}

type Manager struct {
	mu   sync.RWMutex
	subs map[string]map[Subscriber]struct{}
}

func NewManager() *Manager {
	return &Manager{subs: make(map[string]map[Subscriber]struct{})}
}

func (m *Manager) Subscribe(topic string, s Subscriber) {
	m.mu.Lock()
	if m.subs[topic] == nil {
		m.subs[topic] = make(map[Subscriber]struct{})
	}
	m.subs[topic][s] = struct{}{}
	m.mu.Unlock()
}

func (m *Manager) Unsubscribe(topic string, s Subscriber) {
	m.mu.Lock()
	delete(m.subs[topic], s)
	if len(m.subs[topic]) == 0 {
		delete(m.subs, topic)
	}
	m.mu.Unlock()
}

// UnsubscribeAll removes a subscriber from every topic it joined.
func (m *Manager) UnsubscribeAll(s Subscriber) {
	m.mu.Lock()
	for topic, set := range m.subs {
		delete(set, s)
		if len(set) == 0 {
			delete(m.subs, topic)
		}
	}
	m.mu.Unlock()
}

// Publish sends payload to all subscribers of topic.
// Non-blocking: slow clients drop frames.
func (m *Manager) Publish(topic string, payload json.RawMessage) {
	// Snapshot the subscribers under the lock — the inner map may be mutated
	// concurrently by Subscribe/Unsubscribe, so we must not iterate it directly.
	// Send is called outside the lock to keep Publish non-blocking.
	m.mu.RLock()
	set := m.subs[topic]
	subs := make([]Subscriber, 0, len(set))
	for s := range set {
		subs = append(subs, s)
	}
	m.mu.RUnlock()
	for _, s := range subs {
		s.Send(payload)
	}
}

func (m *Manager) HasSubscribers(topic string) bool {
	m.mu.RLock()
	n := len(m.subs[topic])
	m.mu.RUnlock()
	return n > 0
}

// Topics returns all topics that have at least one subscriber.
func (m *Manager) Topics() []string {
	m.mu.RLock()
	out := make([]string, 0, len(m.subs))
	for t := range m.subs {
		out = append(out, t)
	}
	m.mu.RUnlock()
	return out
}

// Stats returns the number of active topics, raw socket connections, and
// distinct connected clients. A single user may hold several sockets (one per
// topic); clients deduplicates those by user_id, while sockets is the raw count.
// Subscribers that expose a UserID() are grouped by that ID; anonymous ones
// (no UserID, or user_id 0) cannot be deduplicated and each count as a client.
func (m *Manager) Stats() (topics, sockets, clients int) {
	m.mu.RLock()
	topics = len(m.subs)
	users := make(map[int]struct{})
	for _, set := range m.subs {
		for s := range set {
			sockets++
			if u, ok := s.(interface{ UserID() int }); ok && u.UserID() > 0 {
				users[u.UserID()] = struct{}{}
			} else {
				clients++ // anonymous / no token → not deduplicable, count individually
			}
		}
	}
	clients += len(users)
	m.mu.RUnlock()
	return
}
