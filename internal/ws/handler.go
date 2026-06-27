package ws

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
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
	// maxMessageSize caps inbound WS frames. The worker is push-only, so the
	// only legitimate inbound frame is the tiny auth handshake; anything larger
	// is rejected to prevent a single client exhausting memory.
	maxMessageSize = 4 << 10 // 4 KiB
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
	conn   *websocket.Conn
	send   chan []byte
	topic  string
	mu     sync.Mutex
	closed bool
}

func (c *Client) Send(payload json.RawMessage) {
	frame := outboundFrame{Type: "push", Payload: payload}
	data, err := json.Marshal(frame)
	if err != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	select {
	case c.send <- data:
	default:
	}
}

// closeSend closes the send channel exactly once, under the lock, so a
// concurrent Send never writes to a closed channel (which would panic).
func (c *Client) closeSend() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		close(c.send)
	}
	c.mu.Unlock()
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

	ip := clientIP(r)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("ws: upgrade error: %v ip=%s", err, ip)
		return
	}
	conn.SetReadLimit(maxMessageSize)

	// Auth handshake: first frame must be { type: "auth", token: "<hmac>" }.
	conn.SetReadDeadline(time.Now().Add(authDeadline))
	_, msg, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		log.Printf("ip=%s -- ws: auth handshake failed topic=%s: %v", ip, topic, err)
		return
	}
	conn.SetReadDeadline(time.Time{})

	var authFrame inboundFrame
	if err := json.Unmarshal(msg, &authFrame); err != nil || authFrame.Type != "auth" || authFrame.Token == "" {
		conn.WriteMessage(websocket.TextMessage, mustMarshal(outboundFrame{Type: "error", Code: "auth_required", Message: "first frame must be {type:auth,token:...}"}))
		conn.Close()
		log.Printf("ip=%s -- ws: auth_required topic=%s", ip, topic)
		return
	}

	if !h.auth.Validate(topic, authFrame.Token) {
		conn.WriteMessage(websocket.TextMessage, mustMarshal(outboundFrame{Type: "error", Code: "unauthorized", Message: "invalid or expired token"}))
		conn.Close()
		log.Printf("ip=%s -- ws: unauthorized topic=%s", ip, topic)
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
	log.Printf("ip=%s -- ws: client connected topic=%s", ip, topic)

	ctx, cancel := context.WithCancel(r.Context())
	defer func() {
		cancel()
		conn.Close()
		// Unsubscribe before closing the send channel so no Publish can pick
		// up this client and write to a channel we are about to close.
		h.sub.UnsubscribeAll(c)
		c.closeSend()
		log.Printf("ip=%s -- ws: client disconnected topic=%s", ip, topic)
	}()

	go c.writePump(ctx)
	c.readPump()
}

// readPump drains incoming frames and keeps the pong handler alive.
// The worker is push-only — browser requests go directly to PHP via AJAX.
func (c *Client) readPump() {
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

// clientIP returns the originating client address, preferring the
// X-Forwarded-For / X-Real-IP headers set by a reverse proxy over
// r.RemoteAddr (which would otherwise show the proxy's address).
//
// The header values are attacker-controlled (any client can send them), so the
// result is sanitized before it reaches the logs to prevent log injection.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if ip := strings.TrimSpace(strings.Split(xff, ",")[0]); ip != "" {
			return sanitizeIP(ip)
		}
	}
	if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
		return sanitizeIP(strings.TrimSpace(xrip))
	}
	return r.RemoteAddr
}

// sanitizeIP strips characters that don't belong in an IP/host so a forged
// proxy header cannot inject newlines or control characters into log lines.
// Anything unexpected is replaced and the result is length-capped.
func sanitizeIP(s string) string {
	if len(s) > 64 {
		s = s[:64]
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= '0' && r <= '9',
			r >= 'a' && r <= 'f',
			r >= 'A' && r <= 'F',
			r == '.' || r == ':' || r == '%': // IPv4, IPv6, zone id
			return r
		default:
			return '?'
		}
	}, s)
}
