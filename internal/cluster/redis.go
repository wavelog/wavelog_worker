package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wavelog/wavelog_worker/internal/sub"
)

const redisChannel = "wavelog:events"

var (
	reconnectMin   = time.Second
	reconnectMax   = 30 * time.Second
	healthInterval = 5 * time.Second
	pingTimeout    = 5 * time.Second
)

// Publisher is the narrow interface api.Server uses to broadcast events.
// ClusterNodes returns the number of active worker instances sharing the Redis
// channel, or -1 if not in cluster mode.
type Publisher interface {
	Publish(topic string, payload json.RawMessage)
	ClusterNodes() int
}

// NoopPublisher delegates directly to the local sub.Manager (single-instance mode).
type NoopPublisher struct{ mgr *sub.Manager }

func NewNoopPublisher(m *sub.Manager) *NoopPublisher { return &NoopPublisher{mgr: m} }

func (n *NoopPublisher) Publish(topic string, payload json.RawMessage) {
	n.mgr.Publish(topic, payload)
}

func (n *NoopPublisher) ClusterNodes() int { return -1 }

// envelope is the wire format for Redis messages.
type envelope struct {
	OriginID string          `json:"origin_id"`
	Topic    string          `json:"topic"`
	Payload  json.RawMessage `json:"payload"`
}

// RedisPublisher fans out events via Redis Pub/Sub so all worker instances
// receive every publish, regardless of which instance PHP posted to.
type RedisPublisher struct {
	client      *redis.Client
	mgr         *sub.Manager
	instanceID  string
	ctx         context.Context
	cancel      context.CancelFunc
	connected   atomic.Bool
	monitorDone chan struct{}
}

func NewRedisPublisher(redisURL string, mgr *sub.Manager) (*RedisPublisher, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opts)
	ctx, cancel := context.WithCancel(context.Background())

	id := make([]byte, 8)
	rand.Read(id)

	rp := &RedisPublisher{
		client:      client,
		mgr:         mgr,
		instanceID:  hex.EncodeToString(id),
		ctx:         ctx,
		cancel:      cancel,
		monitorDone: make(chan struct{}),
	}
	go rp.subscribe()
	go rp.monitor()
	return rp, nil
}

func (r *RedisPublisher) Ready() bool { return r.connected.Load() }

func (r *RedisPublisher) monitor() {
	defer close(r.monitorDone)
	delay := reconnectMin
	for {
		pctx, cancel := context.WithTimeout(r.ctx, pingTimeout)
		err := r.client.Ping(pctx).Err()
		cancel()

		up := err == nil
		if r.connected.Swap(up) != up {
			if up {
				log.Printf("cluster: redis connected")
			} else {
				log.Printf("cluster: redis lost, retrying: %v", err)
			}
		}

		wait := healthInterval
		if !up {
			wait = delay
			delay = min(delay*2, reconnectMax)
		} else {
			delay = reconnectMin
		}
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// Publish broadcasts the event to all cluster members via Redis, and also
// delivers it locally immediately to avoid the Redis round-trip for own subscribers.
func (r *RedisPublisher) Publish(topic string, payload json.RawMessage) {
	// Deliver locally first — subscribers on this instance don't wait for Redis.
	r.mgr.Publish(topic, payload)

	env := envelope{
		OriginID: r.instanceID,
		Topic:    topic,
		Payload:  payload,
	}
	msg, err := json.Marshal(env)
	if err != nil {
		log.Printf("cluster: marshal envelope: %v", err)
		return
	}
	if err := r.client.Publish(r.ctx, redisChannel, msg).Err(); err != nil {
		log.Printf("cluster: redis publish: %v", err)
	}
}

// subscribe runs for the lifetime of the process and forwards incoming Redis
// messages to local subscribers, skipping messages that originated here.
func (r *RedisPublisher) subscribe() {
	pubsub := r.client.Subscribe(r.ctx, redisChannel)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case <-r.ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var env envelope
			if err := json.Unmarshal([]byte(msg.Payload), &env); err != nil {
				log.Printf("cluster: malformed envelope: %v", err)
				continue
			}
			// Skip own messages — already delivered locally in Publish().
			if env.OriginID == r.instanceID {
				continue
			}
			r.mgr.Publish(env.Topic, env.Payload)
		}
	}
}

// ClusterNodes returns the number of worker instances currently subscribed to
// the Redis channel, which equals the number of active cluster nodes.
func (r *RedisPublisher) ClusterNodes() int {
	res, err := r.client.PubSubNumSub(r.ctx, redisChannel).Result()
	if err != nil {
		return -1
	}
	return int(res[redisChannel])
}

func (r *RedisPublisher) Client() *redis.Client    { return r.client }
func (r *RedisPublisher) Context() context.Context { return r.ctx }

// Close shuts down the subscriber goroutine and the Redis connection.
func (r *RedisPublisher) Close() {
	r.cancel()
	<-r.monitorDone
	r.client.Close()
}
