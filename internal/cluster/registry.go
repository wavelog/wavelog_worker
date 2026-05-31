package cluster

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wavelog/wavelog_worker/internal/registry"
)

const (
	topicKeyPrefix = "wavelog:topic:"
	topicTTL       = 24 * time.Hour
)

// RedisRegistry stores topic registrations in Redis so all cluster nodes share
// the same registry state. Topics expire automatically after 24h (refreshed on
// re-register). This replaces MemRegistry when redis_url is configured.
type RedisRegistry struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisRegistry(client *redis.Client, ctx context.Context) *RedisRegistry {
	return &RedisRegistry{client: client, ctx: ctx}
}

func (r *RedisRegistry) Register(topic string, meta registry.TopicMeta) {
	val, err := json.Marshal(meta)
	if err != nil {
		log.Printf("registry: marshal meta for %q: %v", topic, err)
		return
	}
	if err := r.client.Set(r.ctx, topicKeyPrefix+topic, val, topicTTL).Err(); err != nil {
		log.Printf("registry: redis SET %q: %v", topic, err)
	}
}

func (r *RedisRegistry) Unregister(topic string) {
	if err := r.client.Del(r.ctx, topicKeyPrefix+topic).Err(); err != nil {
		log.Printf("registry: redis DEL %q: %v", topic, err)
	}
}

func (r *RedisRegistry) Lookup(topic string) (registry.TopicMeta, bool) {
	val, err := r.client.Get(r.ctx, topicKeyPrefix+topic).Bytes()
	if err == redis.Nil {
		return registry.TopicMeta{}, false
	}
	if err != nil {
		log.Printf("registry: redis GET %q: %v", topic, err)
		return registry.TopicMeta{}, false
	}
	var meta registry.TopicMeta
	if err := json.Unmarshal(val, &meta); err != nil {
		log.Printf("registry: unmarshal meta for %q: %v", topic, err)
		return registry.TopicMeta{}, false
	}
	return meta, true
}

func (r *RedisRegistry) Topics() []string {
	keys, err := r.client.Keys(r.ctx, topicKeyPrefix+"*").Result()
	if err != nil {
		log.Printf("registry: redis KEYS: %v", err)
		return nil
	}
	topics := make([]string, len(keys))
	for i, k := range keys {
		topics[i] = strings.TrimPrefix(k, topicKeyPrefix)
	}
	return topics
}
