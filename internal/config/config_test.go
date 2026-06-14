package config

import (
	"os"
	"path/filepath"
	"testing"
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
	if cfg.WSBind != "" || cfg.InternalBind != "" {
		t.Errorf("binds should default to empty (all interfaces), got %q/%q", cfg.WSBind, cfg.InternalBind)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadBadYAML(t *testing.T) {
	p := writeTemp(t, "ws_port: [not, an, int")
	if _, err := Load(p); err == nil {
		t.Fatal("expected error for malformed YAML")
	}
}
