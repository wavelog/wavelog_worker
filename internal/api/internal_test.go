package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wavelog/wavelog_worker/internal/registry"
	"github.com/wavelog/wavelog_worker/internal/sub"
)

const secret = "test-secret-at-least-32-chars-long!!"

// fakePublisher records Publish calls and reports a fixed cluster node count.
type fakePublisher struct {
	calls        int
	lastTopic    string
	lastPayload  json.RawMessage
	clusterNodes int
}

func (f *fakePublisher) Publish(topic string, payload json.RawMessage) {
	f.calls++
	f.lastTopic = topic
	f.lastPayload = payload
}

func (f *fakePublisher) ClusterNodes() int { return f.clusterNodes }

func newTestServer(t *testing.T) (*Server, *fakePublisher, registry.Registry) {
	t.Helper()
	pub := &fakePublisher{clusterNodes: 3}
	reg := registry.New()
	s := NewServer(sub.NewManager(), pub, reg, secret, "v1.2.3")
	return s, pub, reg
}

// do issues a request against the server's handler and returns the response recorder.
func do(t *testing.T, s *Server, method, path, secretHdr string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if raw, ok := body.(string); ok {
			buf.WriteString(raw)
		} else if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatalf("encode body: %v", err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	if secretHdr != "" {
		req.Header.Set("X-Worker-Secret", secretHdr)
	}
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	return rr
}

func TestHmacEqual(t *testing.T) {
	if hmacEqual("", "") {
		t.Error("empty/empty should be false")
	}
	if hmacEqual("a", "") || hmacEqual("", "a") {
		t.Error("one empty should be false")
	}
	if hmacEqual("abc", "abd") {
		t.Error("different strings should be false")
	}
	if !hmacEqual("abc", "abc") {
		t.Error("equal strings should be true")
	}
}

func TestAuthAndMethodGuards(t *testing.T) {
	endpoints := []struct {
		path        string
		validMethod string
	}{
		{"/internal/register", http.MethodPost},
		{"/internal/unregister", http.MethodPost},
		{"/internal/publish", http.MethodPost},
		{"/internal/push", http.MethodPost},
		{"/internal/status", http.MethodGet},
	}

	for _, ep := range endpoints {
		t.Run(ep.path+" wrong method", func(t *testing.T) {
			s, _, _ := newTestServer(t)
			method := http.MethodPut
			rr := do(t, s, method, ep.path, secret, nil)
			if rr.Code != http.StatusMethodNotAllowed {
				t.Fatalf("got %d, want 405", rr.Code)
			}
		})
		t.Run(ep.path+" wrong secret", func(t *testing.T) {
			s, _, _ := newTestServer(t)
			rr := do(t, s, ep.validMethod, ep.path, "wrong-secret", nil)
			if rr.Code != http.StatusForbidden {
				t.Fatalf("got %d, want 403", rr.Code)
			}
		})
		t.Run(ep.path+" missing secret", func(t *testing.T) {
			s, _, _ := newTestServer(t)
			rr := do(t, s, ep.validMethod, ep.path, "", nil)
			if rr.Code != http.StatusForbidden {
				t.Fatalf("got %d, want 403", rr.Code)
			}
		})
	}
}

func TestRegister(t *testing.T) {
	s, _, reg := newTestServer(t)

	rr := do(t, s, http.MethodPost, "/internal/register", secret,
		registerRequest{Topic: "t", Meta: registry.TopicMeta{RequireToken: true}})
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rr.Code)
	}
	meta, ok := reg.Lookup("t")
	if !ok || !meta.RequireToken {
		t.Fatalf("topic not registered correctly: ok=%v meta=%+v", ok, meta)
	}

	// Empty topic → 400.
	rr = do(t, s, http.MethodPost, "/internal/register", secret, registerRequest{Topic: ""})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("empty topic: got %d, want 400", rr.Code)
	}

	// Malformed JSON → 400.
	rr = do(t, s, http.MethodPost, "/internal/register", secret, "{not json")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("bad json: got %d, want 400", rr.Code)
	}
}

func TestUnregister(t *testing.T) {
	s, _, reg := newTestServer(t)
	reg.Register("t", registry.TopicMeta{})

	rr := do(t, s, http.MethodPost, "/internal/unregister", secret, unregisterRequest{Topic: "t"})
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rr.Code)
	}
	if _, ok := reg.Lookup("t"); ok {
		t.Fatal("topic should be unregistered")
	}

	rr = do(t, s, http.MethodPost, "/internal/unregister", secret, unregisterRequest{Topic: ""})
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("empty topic: got %d, want 400", rr.Code)
	}
}

func TestPush(t *testing.T) {
	s, pub, reg := newTestServer(t)
	reg.Register("t", registry.TopicMeta{})

	// Valid push (both /publish and /push route to the same handler).
	for _, path := range []string{"/internal/publish", "/internal/push"} {
		pub.calls = 0
		rr := do(t, s, http.MethodPost, path, secret,
			pushRequest{Topic: "t", Payload: json.RawMessage(`{"a":1}`)})
		if rr.Code != http.StatusOK {
			t.Fatalf("%s: got %d, want 200", path, rr.Code)
		}
		if pub.calls != 1 || pub.lastTopic != "t" {
			t.Fatalf("%s: publisher not called correctly: calls=%d topic=%q", path, pub.calls, pub.lastTopic)
		}
	}

	// Unregistered topic → 404.
	rr := do(t, s, http.MethodPost, "/internal/push", secret,
		pushRequest{Topic: "missing", Payload: json.RawMessage(`{}`)})
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unregistered: got %d, want 404", rr.Code)
	}

	// Missing topic → 400 (send raw JSON so the field is genuinely absent).
	rr = do(t, s, http.MethodPost, "/internal/push", secret, `{"payload":{}}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing topic: got %d, want 400", rr.Code)
	}

	// Missing payload → 400 (raw JSON: a nil RawMessage would marshal to "null").
	rr = do(t, s, http.MethodPost, "/internal/push", secret, `{"topic":"t"}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing payload: got %d, want 400", rr.Code)
	}

	// Malformed JSON → 400.
	rr = do(t, s, http.MethodPost, "/internal/push", secret, "{bad")
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("bad json: got %d, want 400", rr.Code)
	}
}

func TestStatus(t *testing.T) {
	s, _, reg := newTestServer(t)
	reg.Register("a", registry.TopicMeta{})
	reg.Register("b", registry.TopicMeta{})

	rr := do(t, s, http.MethodGet, "/internal/status", secret, nil)
	if rr.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: got %q", ct)
	}

	var resp statusResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("status: got %q", resp.Status)
	}
	if resp.Version != "v1.2.3" {
		t.Errorf("version: got %q", resp.Version)
	}
	if resp.RegisteredTopics != 2 {
		t.Errorf("registered topics: got %d, want 2", resp.RegisteredTopics)
	}
	if resp.ClusterNodes != 3 {
		t.Errorf("cluster nodes: got %d, want 3 (from fake)", resp.ClusterNodes)
	}
}
