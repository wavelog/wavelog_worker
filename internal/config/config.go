package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	WSBind       string `yaml:"ws_bind"` // optional, empty = all interfaces
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
	cfg := &Config{
		WSPort:       9000,
		InternalBind: "127.0.0.1", // internal API carries the worker_secret — default to localhost
		InternalPort: 9001,
	}
	data, err := os.ReadFile(p)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, err
		}
	}
	if err := cfg.applyEnv(); err != nil {
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

// applyEnv overrides fields from WORKER_* variables. Empty values are ignored.
func (c *Config) applyEnv() error {
	str := func(key string, dst *string) {
		if v := os.Getenv(key); v != "" {
			*dst = v
		}
	}
	num := func(key string, dst *int) error {
		v := os.Getenv(key)
		if v == "" {
			return nil
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("config: %s must be a number, got %q", key, v)
		}
		*dst = n
		return nil
	}
	str("WORKER_WS_BIND", &c.WSBind)
	str("WORKER_INTERNAL_BIND", &c.InternalBind)
	str("WORKER_SECRET", &c.WorkerSecret)
	str("WORKER_REDIS_URL", &c.RedisURL)
	str("WORKER_TOPIC_TTL", &c.RawTopicTTL)
	if err := num("WORKER_WS_PORT", &c.WSPort); err != nil {
		return err
	}
	return num("WORKER_INTERNAL_PORT", &c.InternalPort)
}
