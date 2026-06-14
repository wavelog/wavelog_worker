package ws

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/wavelog/wavelog_worker/internal/auth"
	wlhmac "github.com/wavelog/wavelog_worker/internal/hmac"
	"github.com/wavelog/wavelog_worker/internal/registry"
	"github.com/wavelog/wavelog_worker/internal/sub"
)

const secret = "test-secret-at-least-32-chars-long!!"

type testEnv struct {
	srv *httptest.Server
	reg registry.Registry
	mgr *sub.Manager
}

func newEnv(t *testing.T) *testEnv {
	t.Helper()
	reg := registry.New()
	mgr := sub.NewManager()
	br := auth.NewBridge(reg, secret)
	h := NewHandler(br, mgr, reg)

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &testEnv{srv: srv, reg: reg, mgr: mgr}
}

// wsURL builds the ws:// dial URL for the given topic query (raw, may be empty).
func (e *testEnv) wsURL(topicQuery string) string {
	u := "ws" + strings.TrimPrefix(e.srv.URL, "http")
	if topicQuery != "" {
		u += "?topic=" + topicQuery
	}
	return u
}

func validToken(t *testing.T) string {
	t.Helper()
	tok, err := wlhmac.Sign(wlhmac.Claims{UserID: 1, Expires: time.Now().Add(time.Hour).Unix()}, secret)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return tok
}

func readFrame(t *testing.T, c *websocket.Conn) outboundFrame {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, data, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	var f outboundFrame
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("unmarshal frame %q: %v", data, err)
	}
	return f
}

func TestMissingTopic(t *testing.T) {
	e := newEnv(t)
	_, resp, err := websocket.DefaultDialer.Dial(e.wsURL(""), nil)
	if err == nil {
		t.Fatal("expected dial to fail without topic")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %v", resp)
	}
}

func TestUnregisteredTopic(t *testing.T) {
	e := newEnv(t)
	_, resp, err := websocket.DefaultDialer.Dial(e.wsURL("ghost"), nil)
	if err == nil {
		t.Fatal("expected dial to fail for unregistered topic")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %v", resp)
	}
}

func TestAuthOKAndPush(t *testing.T) {
	e := newEnv(t)
	e.reg.Register("t", registry.TopicMeta{RequireToken: true})

	c, _, err := websocket.DefaultDialer.Dial(e.wsURL("t"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// Send valid auth frame.
	if err := c.WriteJSON(inboundFrame{Type: "auth", Token: validToken(t)}); err != nil {
		t.Fatalf("write auth: %v", err)
	}

	if f := readFrame(t, c); f.Type != "auth_ok" {
		t.Fatalf("expected auth_ok, got %+v", f)
	}

	// Publish via the manager; the client must receive it as a push frame.
	// Poll briefly so the subscription is in place.
	deadline := time.Now().Add(time.Second)
	for !e.mgr.HasSubscribers("t") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	e.mgr.Publish("t", json.RawMessage(`{"hello":"world"}`))

	f := readFrame(t, c)
	if f.Type != "push" {
		t.Fatalf("expected push, got %+v", f)
	}
	if string(f.Payload) != `{"hello":"world"}` {
		t.Fatalf("unexpected payload: %s", f.Payload)
	}
}

func TestInvalidToken(t *testing.T) {
	e := newEnv(t)
	e.reg.Register("t", registry.TopicMeta{RequireToken: true})

	c, _, err := websocket.DefaultDialer.Dial(e.wsURL("t"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.WriteJSON(inboundFrame{Type: "auth", Token: "bogus-token"}); err != nil {
		t.Fatalf("write auth: %v", err)
	}

	f := readFrame(t, c)
	if f.Type != "error" || f.Code != "unauthorized" {
		t.Fatalf("expected error/unauthorized, got %+v", f)
	}
}

func TestFirstFrameNotAuth(t *testing.T) {
	e := newEnv(t)
	e.reg.Register("t", registry.TopicMeta{RequireToken: true})

	c, _, err := websocket.DefaultDialer.Dial(e.wsURL("t"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// Wrong type for the first frame.
	if err := c.WriteJSON(inboundFrame{Type: "subscribe"}); err != nil {
		t.Fatalf("write: %v", err)
	}

	f := readFrame(t, c)
	if f.Type != "error" || f.Code != "auth_required" {
		t.Fatalf("expected error/auth_required, got %+v", f)
	}
}
