package cluster

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"sort"
	"time"
)

const nodesKey = "wavelog:nodes"

var (
	heartbeatInterval = 5 * time.Second
	nodeAliveAfter    = 15 * time.Second // 3 missed heartbeats
	nodeForgetAfter   = 15 * time.Minute // ponytail: fixed grace window, make it a config key if anyone asks
)

type NodeInfo struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Version       string    `json:"version"`
	StartedAt     time.Time `json:"started_at"`
	SeenAt        time.Time `json:"seen_at"`
	ActiveTopics  int       `json:"active_topics"`
	Clients       int       `json:"connected_clients"`
	Sockets       int       `json:"connected_sockets"`
	Alive         bool      `json:"alive"`
	Uptime        string    `json:"uptime"`
	UptimeSeconds int       `json:"uptime_seconds"`
}

func (n *NodeInfo) fill(now time.Time) {
	n.Alive = now.Sub(n.SeenAt) < nodeAliveAfter
	end := now
	if !n.Alive {
		end = n.SeenAt
	}
	up := end.Sub(n.StartedAt).Round(time.Second)
	n.Uptime = up.String()
	n.UptimeSeconds = int(up.Seconds())
}

type Self struct {
	ID      string
	Name    string
	Version string
	Started time.Time
	Stats   func() (topics, sockets, clients int)
}

func NewSelf(version string, started time.Time, stats func() (topics, sockets, clients int)) Self {
	id := make([]byte, 8)
	rand.Read(id)
	s := Self{ID: hex.EncodeToString(id), Version: version, Started: started, Stats: stats}
	if h, err := os.Hostname(); err == nil && h != "" {
		s.Name = h
	} else {
		s.Name = s.ID
	}
	return s
}

func (s Self) info(now time.Time) NodeInfo {
	topics, sockets, clients := s.Stats()
	n := NodeInfo{
		ID: s.ID, Name: s.Name, Version: s.Version,
		StartedAt: s.Started, SeenAt: now,
		ActiveTopics: topics, Clients: clients, Sockets: sockets,
	}
	n.fill(now)
	return n
}

func sortNodes(nodes []NodeInfo) {
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].Name != nodes[j].Name {
			return nodes[i].Name < nodes[j].Name
		}
		return nodes[i].ID < nodes[j].ID
	})
}
