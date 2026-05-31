package registry

import "sync"

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

// MemRegistry is a thread-safe in-memory Registry.
// State is lost on Worker restart; PHP re-registers via publish 404-retry or page reload.
type MemRegistry struct {
	mu     sync.RWMutex
	topics map[string]TopicMeta
}

func New() *MemRegistry {
	return &MemRegistry{topics: make(map[string]TopicMeta)}
}

func (r *MemRegistry) Register(topic string, meta TopicMeta) {
	r.mu.Lock()
	r.topics[topic] = meta
	r.mu.Unlock()
}

func (r *MemRegistry) Unregister(topic string) {
	r.mu.Lock()
	delete(r.topics, topic)
	r.mu.Unlock()
}

func (r *MemRegistry) Lookup(topic string) (TopicMeta, bool) {
	r.mu.RLock()
	meta, ok := r.topics[topic]
	r.mu.RUnlock()
	return meta, ok
}

func (r *MemRegistry) Topics() []string {
	r.mu.RLock()
	out := make([]string, 0, len(r.topics))
	for t := range r.topics {
		out = append(out, t)
	}
	r.mu.RUnlock()
	return out
}
