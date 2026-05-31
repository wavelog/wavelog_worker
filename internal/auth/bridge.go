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

// Validate checks that the topic is registered and the HMAC token is valid.
// Returns false for unknown topics — PHP must register before browsers can connect.
// Token validity (signature + expiry) is sufficient — topic names are opaque identifiers.
func (b *Bridge) Validate(topic, token string) bool {
	meta, ok := b.reg.Lookup(topic)
	if !ok {
		return false
	}
	if !meta.RequireToken {
		return true
	}
	_, err := wlhmac.Verify(token, b.secret)
	return err == nil
}
