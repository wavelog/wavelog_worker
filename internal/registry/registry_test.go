package registry

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestMemRegistryRegisterLookup(t *testing.T) {
	r := New()

	if _, ok := r.Lookup("unknown"); ok {
		t.Fatal("Lookup of unknown topic should return ok=false")
	}

	r.Register("topic-a", TopicMeta{RequireToken: true})
	meta, ok := r.Lookup("topic-a")
	if !ok {
		t.Fatal("expected topic-a to be registered")
	}
	if !meta.RequireToken {
		t.Error("expected RequireToken=true")
	}
}

func TestMemRegistryOverwrite(t *testing.T) {
	r := New()
	r.Register("t", TopicMeta{RequireToken: true})
	r.Register("t", TopicMeta{RequireToken: false})

	meta, ok := r.Lookup("t")
	if !ok {
		t.Fatal("expected topic registered")
	}
	if meta.RequireToken {
		t.Error("expected overwrite to set RequireToken=false")
	}
}

func TestMemRegistryUnregister(t *testing.T) {
	r := New()
	r.Register("t", TopicMeta{})
	r.Unregister("t")
	if _, ok := r.Lookup("t"); ok {
		t.Fatal("topic should be gone after Unregister")
	}
	// Unregistering an unknown topic must not panic.
	r.Unregister("never-existed")
}

func TestMemRegistryTopics(t *testing.T) {
	r := New()
	if got := r.Topics(); len(got) != 0 {
		t.Fatalf("expected empty topics, got %v", got)
	}
	r.Register("a", TopicMeta{})
	r.Register("b", TopicMeta{})

	got := r.Topics()
	sort.Strings(got)
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("unexpected topics: %v", got)
	}
}

func TestMemRegistryLookupExpired(t *testing.T) {
	r := New()
	r.Register("t", TopicMeta{RequireToken: true})

	// Force the entry to have expired in the past.
	r.mu.Lock()
	e := r.topics["t"]
	e.expiresAt = time.Now().Add(-time.Minute)
	r.topics["t"] = e
	r.mu.Unlock()

	if _, ok := r.Lookup("t"); ok {
		t.Fatal("expired topic must not be found by Lookup")
	}
	if got := r.Topics(); len(got) != 0 {
		t.Fatalf("expired topic must not appear in Topics, got %v", got)
	}
}

func TestMemRegistryReap(t *testing.T) {
	r := New()
	r.ttl = time.Hour
	r.Register("fresh", TopicMeta{})

	// A stale entry whose expiry is already in the past.
	r.mu.Lock()
	r.topics["stale"] = entry{expiresAt: time.Now().Add(-time.Hour)}
	r.mu.Unlock()

	r.reap(time.Now())

	r.mu.RLock()
	_, staleOK := r.topics["stale"]
	_, freshOK := r.topics["fresh"]
	r.mu.RUnlock()
	if staleOK {
		t.Error("reap should have deleted the stale topic")
	}
	if !freshOK {
		t.Error("reap must keep the fresh topic")
	}
}

func TestMemRegistryReRegisterRefreshesTTL(t *testing.T) {
	r := New()
	r.Register("t", TopicMeta{})

	r.mu.RLock()
	first := r.topics["t"].expiresAt
	r.mu.RUnlock()

	time.Sleep(time.Millisecond)
	r.Register("t", TopicMeta{})

	r.mu.RLock()
	second := r.topics["t"].expiresAt
	r.mu.RUnlock()

	if !second.After(first) {
		t.Fatalf("re-register should push expiry forward: first=%v second=%v", first, second)
	}
}

func TestMemRegistryConcurrent(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			topic := fmt.Sprintf("t%d", n)
			r.Register(topic, TopicMeta{})
			r.Lookup(topic)
			r.Topics()
			r.Unregister(topic)
		}(i)
	}
	wg.Wait()
}
