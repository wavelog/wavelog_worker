package registry

import (
	"sync"
	"time"
)

// TopicMeta holds the metadata PHP sends when registering a topic.
type TopicMeta struct {
	RequireToken bool `json:"require_token"`
}

// Registry is the interface for topic storage — in-memory (single-instance)
// or Redis-backed (cluster mode).
type Registry interface {
	Register(topic string, meta TopicMeta)
	Unregister(topic string)
	Lookup(topic string) (TopicMeta, bool)
	Topics() []string
}

// DefaultTTL is the idle window after which a registered topic expires. Both
// backends (MemRegistry here and the Redis-backed registry in internal/cluster)
// read this single value so they stay in sync; it is a var so it can later be
// made configurable.
var DefaultTTL = 24 * time.Hour

// reapInterval is how often the janitor sweeps expired topics from the map.
const reapInterval = time.Hour

// entry is a registered topic with its expiry time.
type entry struct {
	meta      TopicMeta
	expiresAt time.Time
}

// MemRegistry is a thread-safe in-memory Registry.
// State is lost on Worker restart; PHP re-registers via publish 404-retry or page reload.
//
// Topics expire after ttl (refreshed on re-register) and are swept by a
// background janitor. Without this, a long-running single-instance worker would
// accumulate every topic it ever saw — PHP has no reliable unregister path — and
// leak memory. The Redis backend already self-heals via key TTL; this matches it.
type MemRegistry struct {
	mu     sync.RWMutex
	topics map[string]entry
	ttl    time.Duration
}

func New() *MemRegistry {
	r := &MemRegistry{
		topics: make(map[string]entry),
		ttl:    DefaultTTL,
	}
	go r.janitor()
	return r
}

func (r *MemRegistry) Register(topic string, meta TopicMeta) {
	r.mu.Lock()
	r.topics[topic] = entry{meta: meta, expiresAt: time.Now().Add(r.ttl)}
	r.mu.Unlock()
}

func (r *MemRegistry) Unregister(topic string) {
	r.mu.Lock()
	delete(r.topics, topic)
	r.mu.Unlock()
}

func (r *MemRegistry) Lookup(topic string) (TopicMeta, bool) {
	r.mu.RLock()
	e, ok := r.topics[topic]
	r.mu.RUnlock()
	// Lazy expiry: an expired topic is treated as gone even before the janitor
	// reaps it, so publishes/connects don't see a stale registration.
	if !ok || time.Now().After(e.expiresAt) {
		return TopicMeta{}, false
	}
	return e.meta, true
}

func (r *MemRegistry) Topics() []string {
	now := time.Now()
	r.mu.RLock()
	out := make([]string, 0, len(r.topics))
	for t, e := range r.topics {
		if now.After(e.expiresAt) {
			continue
		}
		out = append(out, t)
	}
	r.mu.RUnlock()
	return out
}

// reap deletes all topics that expired at or before now. Split out so tests can
// drive eviction deterministically instead of waiting on the janitor ticker.
func (r *MemRegistry) reap(now time.Time) {
	r.mu.Lock()
	for t, e := range r.topics {
		if now.After(e.expiresAt) {
			delete(r.topics, t)
		}
	}
	r.mu.Unlock()
}

// janitor periodically reaps expired topics for the lifetime of the process.
func (r *MemRegistry) janitor() {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()
	for range ticker.C {
		r.reap(time.Now())
	}
}
