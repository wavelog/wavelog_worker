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
// access to exactly this topic. On success it also returns the authenticated
// user's ID (from the token claims) so callers can attribute the connection.
// Returns ok=false for unknown topics — PHP must register before browsers can connect.
// A valid signature is not sufficient on its own: the token's Topic claim must
// match the requested topic, otherwise any valid token would grant access to
// every registered topic (broken access control).
// For topics that don't require a token the user is anonymous, so userID is 0.
func (b *Bridge) Validate(topic, token string) (userID int, ok bool) {
	meta, found := b.reg.Lookup(topic)
	if !found {
		return 0, false
	}
	if !meta.RequireToken {
		return 0, true
	}
	claims, err := wlhmac.Verify(token, b.secret)
	if err != nil {
		return 0, false
	}
	if claims.Topic != topic {
		return 0, false
	}
	return claims.UserID, true
}
