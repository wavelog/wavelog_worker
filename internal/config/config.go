package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	WSBind       string `yaml:"ws_bind"`       // optional, empty = all interfaces
	WSPort       int    `yaml:"ws_port"`
	InternalBind string `yaml:"internal_bind"` // optional, empty = all interfaces
	InternalPort int    `yaml:"internal_port"`
	WorkerSecret string `yaml:"worker_secret"`
	RedisURL     string `yaml:"redis_url"` // optional, empty = single-instance mode
}

func Load(p string) (*Config, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	cfg := &Config{
		WSPort:       9000,
		InternalPort: 9001,
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}
