package registry

import (
	"fmt"
	"sort"
	"sync"
	"testing"
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
