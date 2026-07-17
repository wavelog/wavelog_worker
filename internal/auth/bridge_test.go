package auth

import (
	"testing"
	"time"

	wlhmac "github.com/wavelog/wavelog_worker/internal/hmac"
	"github.com/wavelog/wavelog_worker/internal/registry"
)

const secret = "test-secret-at-least-32-chars-long!!"

// tokenFor mints a valid token bound to the given topic.
func tokenFor(t *testing.T, topic string) string {
	t.Helper()
	tok, err := wlhmac.Sign(wlhmac.Claims{UserID: 1, Topic: topic, Expires: time.Now().Add(time.Hour).Unix()}, secret)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	return tok
}

func TestValidateUnknownTopic(t *testing.T) {
	b := NewBridge(registry.New(), secret)
	if _, ok := b.Validate("nope", tokenFor(t, "nope")); ok {
		t.Fatal("unknown topic must not validate")
	}
}

func TestValidateNoTokenRequired(t *testing.T) {
	reg := registry.New()
	reg.Register("open", registry.TopicMeta{RequireToken: false})
	b := NewBridge(reg, secret)

	// Token is irrelevant when the topic does not require one. The user is
	// anonymous, so userID is 0.
	if uid, ok := b.Validate("open", ""); !ok || uid != 0 {
		t.Fatalf("topic without token requirement should validate with empty token (uid=%d ok=%v)", uid, ok)
	}
	if uid, ok := b.Validate("open", "garbage"); !ok || uid != 0 {
		t.Fatalf("topic without token requirement should validate with garbage token (uid=%d ok=%v)", uid, ok)
	}
}

func TestValidateTokenRequired(t *testing.T) {
	reg := registry.New()
	reg.Register("secure", registry.TopicMeta{RequireToken: true})
	b := NewBridge(reg, secret)

	// A valid token validates and yields the user_id from its claims (1).
	if uid, ok := b.Validate("secure", tokenFor(t, "secure")); !ok || uid != 1 {
		t.Fatalf("valid token should validate with its user_id (uid=%d ok=%v)", uid, ok)
	}
	if _, ok := b.Validate("secure", "invalid-token"); ok {
		t.Fatal("invalid token must not validate")
	}
	if _, ok := b.Validate("secure", ""); ok {
		t.Fatal("empty token must not validate")
	}

	// Expired token must be rejected.
	expired, err := wlhmac.Sign(wlhmac.Claims{UserID: 1, Topic: "secure", Expires: time.Now().Add(-time.Hour).Unix()}, secret)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if _, ok := b.Validate("secure", expired); ok {
		t.Fatal("expired token must not validate")
	}
}

// A validly-signed token for one topic must not grant access to another topic.
func TestValidateTokenTopicMismatch(t *testing.T) {
	reg := registry.New()
	reg.Register("contest_session.100", registry.TopicMeta{RequireToken: true})
	reg.Register("contest_session.999", registry.TopicMeta{RequireToken: true})
	reg.Register("radio.5", registry.TopicMeta{RequireToken: true})
	b := NewBridge(reg, secret)

	// Token minted for the attacker's own session.
	own := tokenFor(t, "contest_session.100")

	if _, ok := b.Validate("contest_session.100", own); !ok {
		t.Fatal("token must validate for its own topic")
	}
	if _, ok := b.Validate("contest_session.999", own); ok {
		t.Fatal("token for one contest session must not validate for another")
	}
	if _, ok := b.Validate("radio.5", own); ok {
		t.Fatal("contest-session token must not validate for a radio topic")
	}
}
