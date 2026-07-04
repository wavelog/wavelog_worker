package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	WSBind       string `yaml:"ws_bind"`       // optional, empty = all interfaces
	WSPort       int    `yaml:"ws_port"`
	InternalBind string `yaml:"internal_bind"` // optional, empty = all interfaces
	InternalPort int    `yaml:"internal_port"`
	WorkerSecret string `yaml:"worker_secret"`
	RedisURL     string `yaml:"redis_url"` // optional, empty = single-instance mode

	// TopicTTL is the parsed idle window after which a registered topic expires.
	// Zero means "unset" — the caller keeps registry.DefaultTTL (24h).
	TopicTTL    time.Duration `yaml:"-"`
	RawTopicTTL string        `yaml:"topic_ttl"` // human duration, e.g. "24h"; empty = default
}

func Load(p string) (*Config, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	cfg := &Config{
		WSPort:       9000,
		InternalBind: "127.0.0.1", // internal API carries the worker_secret — default to localhost
		InternalPort: 9001,
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	if cfg.RawTopicTTL != "" {
		d, err := time.ParseDuration(cfg.RawTopicTTL)
		if err != nil {
			return nil, fmt.Errorf("config: invalid topic_ttl %q: %w", cfg.RawTopicTTL, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("config: topic_ttl must be positive, got %q", cfg.RawTopicTTL)
		}
		cfg.TopicTTL = d
	}
	return cfg, nil
}
