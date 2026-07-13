package api

import (
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/wavelog/wavelog_worker/internal/cluster"
	"github.com/wavelog/wavelog_worker/internal/registry"
	"github.com/wavelog/wavelog_worker/internal/sub"
)

// hmacEqual is a constant-time string comparison to prevent timing attacks.
// Returns false if either argument is empty to avoid vacuous matches.
func hmacEqual(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

type pushRequest struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

type registerRequest struct {
	Topic string             `json:"topic"`
	Meta  registry.TopicMeta `json:"meta"`
}

type unregisterRequest struct {
	Topic string `json:"topic"`
}

type Server struct {
	sub     *sub.Manager
	pub     cluster.Publisher
	reg     registry.Registry
	secret  string
	version string
	started time.Time
}

func NewServer(s *sub.Manager, pub cluster.Publisher, reg registry.Registry, secret, version string) *Server {
	return &Server{sub: s, pub: pub, reg: reg, secret: secret, version: version, started: time.Now()}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/register", s.handleRegister)
	mux.HandleFunc("/internal/unregister", s.handleUnregister)
	mux.HandleFunc("/internal/publish", s.handlePush)
	mux.HandleFunc("/internal/push", s.handlePush)
	mux.HandleFunc("/internal/status", s.handleStatus)
	return mux
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !hmacEqual(r.Header.Get("X-Worker-Secret"), s.secret) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Topic == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	s.reg.Register(req.Topic, req.Meta)
	log.Printf("api: registered topic %q (require_token=%v)", req.Topic, req.Meta.RequireToken)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleUnregister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !hmacEqual(r.Header.Get("X-Worker-Secret"), s.secret) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req unregisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Topic == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	s.reg.Unregister(req.Topic)
	log.Printf("api: unregistered topic %q", req.Topic)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handlePush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !hmacEqual(r.Header.Get("X-Worker-Secret"), s.secret) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req pushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.Topic == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}
	if len(req.Payload) == 0 {
		http.Error(w, "missing payload", http.StatusBadRequest)
		return
	}
	if _, ok := s.reg.Lookup(req.Topic); !ok {
		// Worker may have restarted and lost the registry.
		// PHP catches 404 and re-registers before retrying.
		http.Error(w, "topic not registered", http.StatusNotFound)
		return
	}
	s.pub.Publish(req.Topic, req.Payload)
	w.WriteHeader(http.StatusOK)
}

type statusResponse struct {
	Status           string   `json:"status"`
	Version          string   `json:"version"`
	Uptime           string   `json:"uptime"`
	RegisteredTopics int      `json:"registered_topics"`
	ActiveTopics     int      `json:"active_topics"`
	Clients          int      `json:"connected_clients"`
	TopicList        []string `json:"topic_list"`
	ActiveTopicList  []string `json:"active_topic_list"`
	ClusterNodes     int      `json:"cluster_nodes"` // -1 = single-instance mode
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !hmacEqual(r.Header.Get("X-Worker-Secret"), s.secret) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	topics, clients := s.sub.Stats()
	regTopics := s.reg.Topics()
	activeTopics := s.sub.Topics()
	resp := statusResponse{
		Status:           "ok",
		Version:          s.version,
		Uptime:           time.Since(s.started).Round(time.Second).String(),
		RegisteredTopics: len(regTopics),
		ActiveTopics:     topics,
		Clients:          clients,
		TopicList:        regTopics,
		ActiveTopicList:  activeTopics,
		ClusterNodes:     s.pub.ClusterNodes(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
