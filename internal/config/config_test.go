package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return p
}

func TestLoadValid(t *testing.T) {
	p := writeTemp(t, `
ws_bind: "0.0.0.0"
ws_port: 8000
internal_bind: "127.0.0.1"
internal_port: 8001
worker_secret: "supersecret"
redis_url: "redis://localhost:6379/0"
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.WSPort != 8000 || cfg.InternalPort != 8001 {
		t.Errorf("ports: got %d/%d", cfg.WSPort, cfg.InternalPort)
	}
	if cfg.WSBind != "0.0.0.0" || cfg.InternalBind != "127.0.0.1" {
		t.Errorf("binds: got %q/%q", cfg.WSBind, cfg.InternalBind)
	}
	if cfg.WorkerSecret != "supersecret" {
		t.Errorf("worker_secret: got %q", cfg.WorkerSecret)
	}
	if cfg.RedisURL != "redis://localhost:6379/0" {
		t.Errorf("redis_url: got %q", cfg.RedisURL)
	}
}

func TestLoadDefaults(t *testing.T) {
	// Only worker_secret set — ports must fall back to defaults.
	p := writeTemp(t, `worker_secret: "x"`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.WSPort != 9000 {
		t.Errorf("default WSPort: got %d, want 9000", cfg.WSPort)
	}
	if cfg.InternalPort != 9001 {
		t.Errorf("default InternalPort: got %d, want 9001", cfg.InternalPort)
	}
	if cfg.RedisURL != "" {
		t.Errorf("RedisURL should be empty, got %q", cfg.RedisURL)
	}
	if cfg.WSBind != "" {
		t.Errorf("WSBind should default to empty (all interfaces), got %q", cfg.WSBind)
	}
	if cfg.InternalBind != "127.0.0.1" {
		t.Errorf("InternalBind should default to 127.0.0.1, got %q", cfg.InternalBind)
	}
}

func TestLoadTopicTTL(t *testing.T) {
	p := writeTemp(t, `
worker_secret: "x"
topic_ttl: "12h"
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.TopicTTL != 12*time.Hour {
		t.Errorf("TopicTTL: got %s, want 12h", cfg.TopicTTL)
	}
}

func TestLoadTopicTTLDefaultUnset(t *testing.T) {
	// Unset topic_ttl leaves TopicTTL at zero so the caller keeps its default.
	p := writeTemp(t, `worker_secret: "x"`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.TopicTTL != 0 {
		t.Errorf("TopicTTL should be zero when unset, got %s", cfg.TopicTTL)
	}
}

func TestLoadTopicTTLInvalid(t *testing.T) {
	if _, err := Load(writeTemp(t, "topic_ttl: \"nonsense\"")); err == nil {
		t.Fatal("expected error for malformed topic_ttl")
	}
	if _, err := Load(writeTemp(t, "topic_ttl: \"-5h\"")); err == nil {
		t.Fatal("expected error for non-positive topic_ttl")
	}
}

// A missing file is fine: defaults apply and the environment can fill the rest.
func TestLoadMissingFile(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err != nil {
		t.Fatalf("missing file must not be an error: %v", err)
	}
	if cfg.WSPort != 9000 || cfg.InternalPort != 9001 || cfg.InternalBind != "127.0.0.1" {
		t.Errorf("defaults: got %+v", cfg)
	}
}

func TestLoadEnvOnly(t *testing.T) {
	t.Setenv("WORKER_WS_BIND", "0.0.0.0")
	t.Setenv("WORKER_WS_PORT", "8000")
	t.Setenv("WORKER_INTERNAL_BIND", "0.0.0.0")
	t.Setenv("WORKER_INTERNAL_PORT", "8001")
	t.Setenv("WORKER_SECRET", "env-secret")
	t.Setenv("WORKER_REDIS_URL", "redis://env:6379/1")
	t.Setenv("WORKER_TOPIC_TTL", "2h")
	cfg, err := Load(filepath.Join(t.TempDir(), "none.yaml"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.WSBind != "0.0.0.0" || cfg.WSPort != 8000 || cfg.InternalBind != "0.0.0.0" || cfg.InternalPort != 8001 {
		t.Errorf("listeners: got %+v", cfg)
	}
	if cfg.WorkerSecret != "env-secret" || cfg.RedisURL != "redis://env:6379/1" || cfg.TopicTTL != 2*time.Hour {
		t.Errorf("values: got %+v", cfg)
	}
}

// Env overrides the file; unset or empty env vars leave file values alone.
func TestLoadEnvOverridesFile(t *testing.T) {
	p := writeTemp(t, "ws_port: 8000\nworker_secret: \"from-file\"\nredis_url: \"redis://file:6379/0\"\n")
	t.Setenv("WORKER_SECRET", "from-env")
	t.Setenv("WORKER_REDIS_URL", "")
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.WorkerSecret != "from-env" {
		t.Errorf("worker_secret: env must win, got %q", cfg.WorkerSecret)
	}
	if cfg.WSPort != 8000 || cfg.RedisURL != "redis://file:6379/0" {
		t.Errorf("file values must survive: got %+v", cfg)
	}
}

func TestLoadEnvBadPort(t *testing.T) {
	t.Setenv("WORKER_WS_PORT", "nine")
	if _, err := Load(filepath.Join(t.TempDir(), "none.yaml")); err == nil {
		t.Fatal("expected error for non-numeric WORKER_WS_PORT")
	}
}

func TestLoadBadYAML(t *testing.T) {
	p := writeTemp(t, "ws_port: [not, an, int")
	if _, err := Load(p); err == nil {
		t.Fatal("expected error for malformed YAML")
	}
}
