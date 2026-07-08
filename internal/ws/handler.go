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
	"github.com/wavelog/wavelog_worker/internal/cluster"
	"github.com/wavelog/wavelog_worker/internal/registry"
	"github.com/wavelog/wavelog_worker/internal/sub"
)

const StatusTopic = "worker.status"

// statusMinInterval rate-limit
const statusMinInterval = 700 * time.Millisecond

type statusSnapshot struct {
	Status           string `json:"status"`
	Version          string `json:"version"`
	Uptime           string `json:"uptime"`
	UptimeSeconds    int    `json:"uptime_seconds"`
	RegisteredTopics int    `json:"registered_topics"`
	ActiveTopics     int    `json:"active_topics"`
	Clients          int    `json:"connected_clients"`
	ClusterNodes     int    `json:"cluster_nodes"` // -1 = single-instance mode
}

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

	statusFn   func() statusSnapshot
	lastStatus time.Time
}

func (c *Client) Send(payload json.RawMessage) {
	c.sendFrame(outboundFrame{Type: "push", Payload: payload})
}

func (c *Client) sendFrame(frame outboundFrame) {
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
	auth    *auth.Bridge
	sub     *sub.Manager
	reg     registry.Registry
	pub     cluster.Publisher
	version string
	started time.Time
}

func NewHandler(a *auth.Bridge, s *sub.Manager, reg registry.Registry, pub cluster.Publisher, version string, started time.Time) *Handler {
	return &Handler{auth: a, sub: s, reg: reg, pub: pub, version: version, started: started}
}

// buildStatus gathers the current live stats for the status feed.
func (h *Handler) buildStatus() statusSnapshot {
	active, clients := h.sub.Stats()
	uptime := time.Since(h.started).Round(time.Second)
	return statusSnapshot{
		Status:           "ok",
		Version:          h.version,
		Uptime:           uptime.String(),
		UptimeSeconds:    int(uptime.Seconds()),
		RegisteredTopics: len(h.reg.Topics()),
		ActiveTopics:     active,
		Clients:          clients,
		ClusterNodes:     h.pub.ClusterNodes(),
	}
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
	// Clients on the reserved status topic may pull live stats over the socket.
	if topic == StatusTopic {
		c.statusFn = h.buildStatus
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
// The worker is push-only with one exception: clients on the reserved status
// topic may send {type:"status"} to pull a live stats snapshot over the same
// connection. All other inbound frames are ignored (browser sync requests go to
// PHP via AJAX).
func (c *Client) readPump() {
	c.conn.SetReadDeadline(time.Now().Add(readDeadline))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(readDeadline))
		return nil
	})
	for {
		_, msg, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		if c.statusFn == nil {
			continue
		}
		var frame inboundFrame
		if err := json.Unmarshal(msg, &frame); err != nil || frame.Type != "status" {
			continue
		}
		// Throttle: ignore status requests that arrive faster than the minimum
		// interval so a client cannot flood the worker with snapshot work.
		if now := time.Now(); now.Sub(c.lastStatus) >= statusMinInterval {
			c.lastStatus = now
			snap, err := json.Marshal(c.statusFn())
			if err != nil {
				continue
			}
			c.sendFrame(outboundFrame{Type: "status", Payload: snap})
		}
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
