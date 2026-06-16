package auth

import (
	wlhmac "github.com/wavelog/wavelog_worker/internal/hmac"
	"github.com/wavelog/wavelog_worker/internal/registry"
)

type Bridge struct {
	reg    registry.Registry
	secret string
}

func NewBridge(reg registry.Registry, secret string) *Bridge {
	return &Bridge{reg: reg, secret: secret}
}

// Validate checks that the topic is registered and the HMAC token authorizes
// access to exactly this topic.
// Returns false for unknown topics — PHP must register before browsers can connect.
// A valid signature is not sufficient on its own: the token's Topic claim must
// match the requested topic, otherwise any valid token would grant access to
// every registered topic (broken access control).
func (b *Bridge) Validate(topic, token string) bool {
	meta, ok := b.reg.Lookup(topic)
	if !ok {
		return false
	}
	if !meta.RequireToken {
		return true
	}
	claims, err := wlhmac.Verify(token, b.secret)
	if err != nil {
		return false
	}
	return claims.Topic == topic
}
