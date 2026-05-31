package ws

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/wavelog/wavelog_worker/internal/auth"
	"github.com/wavelog/wavelog_worker/internal/registry"
	"github.com/wavelog/wavelog_worker/internal/sub"
)

const (
	pingInterval  = 30 * time.Second
	writeDeadline = 10 * time.Second
	readDeadline  = 60 * time.Second
	authDeadline  = 10 * time.Second
	sendBuf       = 32
)

// upgrader accepts all origins — CSRF protection is handled by the HMAC token in
// the first frame, not by Origin header checks.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type inboundFrame struct {
	Type  string `json:"type"`
	Token string `json:"token,omitempty"`
}

type outboundFrame struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Code    string          `json:"code,omitempty"`
	Message string          `json:"message,omitempty"`
}

type Client struct {
	conn  *websocket.Conn
	send  chan []byte
	topic string
	mu    sync.Mutex
}

func (c *Client) Send(payload json.RawMessage) {
	frame := outboundFrame{Type: "push", Payload: payload}
	data, err := json.Marshal(frame)
	if err != nil {
		return
	}
	select {
	case c.send <- data:
	default:
	}
}

type Handler struct {
	auth *auth.Bridge
	sub  *sub.Manager
	reg  registry.Registry
}

func NewHandler(a *auth.Bridge, s *sub.Manager, reg registry.Registry) *Handler {
	return &Handler{auth: a, sub: s, reg: reg}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}
	if _, ok := h.reg.Lookup(topic); !ok {
		http.Error(w, "topic not registered", http.StatusNotFound)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws: upgrade error: %v", err)
		return
	}

	// Auth handshake: first frame must be { type: "auth", token: "<hmac>" }.
	conn.SetReadDeadline(time.Now().Add(authDeadline))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return
	}
	conn.SetReadDeadline(time.Time{})

	var authFrame inboundFrame
	if err := json.Unmarshal(msg, &authFrame); err != nil || authFrame.Type != "auth" || authFrame.Token == "" {
		conn.WriteMessage(websocket.TextMessage, mustMarshal(outboundFrame{Type: "error", Code: "auth_required", Message: "first frame must be {type:auth,token:...}"}))
		conn.Close()
		return
	}

	if !h.auth.Validate(topic, authFrame.Token) {
		conn.WriteMessage(websocket.TextMessage, mustMarshal(outboundFrame{Type: "error", Code: "unauthorized", Message: "invalid or expired token"}))
		conn.Close()
		return
	}

	conn.SetWriteDeadline(time.Now().Add(writeDeadline))
	conn.WriteMessage(websocket.TextMessage, mustMarshal(outboundFrame{Type: "auth_ok"}))

	c := &Client{
		conn:  conn,
		send:  make(chan []byte, sendBuf),
		topic: topic,
	}

	h.sub.Subscribe(topic, c)
	log.Printf("ws: client connected topic=%s", topic)

	ctx, cancel := context.WithCancel(r.Context())
	defer func() {
		cancel()
		conn.Close()
		h.sub.UnsubscribeAll(c)
		log.Printf("ws: client disconnected topic=%s", topic)
	}()

	go c.writePump(ctx)
	c.readPump()
}

// readPump drains incoming frames and keeps the pong handler alive.
// The worker is push-only — browser requests go directly to PHP via AJAX.
func (c *Client) readPump() {
	defer close(c.send)
	c.conn.SetReadDeadline(time.Now().Add(readDeadline))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(readDeadline))
		return nil
	})
	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		// All inbound frames (except the auth handshake above) are ignored.
		// Browsers must not send requests over WS; use AJAX instead.
	}
}

func (c *Client) writePump(ctx context.Context) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-c.send:
			if !ok {
				return
			}
			c.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
