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
	if b.Validate("nope", tokenFor(t, "nope")) {
		t.Fatal("unknown topic must not validate")
	}
}

func TestValidateNoTokenRequired(t *testing.T) {
	reg := registry.New()
	reg.Register("open", registry.TopicMeta{RequireToken: false})
	b := NewBridge(reg, secret)

	// Token is irrelevant when the topic does not require one.
	if !b.Validate("open", "") {
		t.Fatal("topic without token requirement should validate with empty token")
	}
	if !b.Validate("open", "garbage") {
		t.Fatal("topic without token requirement should validate with garbage token")
	}
}

func TestValidateTokenRequired(t *testing.T) {
	reg := registry.New()
	reg.Register("secure", registry.TopicMeta{RequireToken: true})
	b := NewBridge(reg, secret)

	if !b.Validate("secure", tokenFor(t, "secure")) {
		t.Fatal("valid token should validate")
	}
	if b.Validate("secure", "invalid-token") {
		t.Fatal("invalid token must not validate")
	}
	if b.Validate("secure", "") {
		t.Fatal("empty token must not validate")
	}

	// Expired token must be rejected.
	expired, err := wlhmac.Sign(wlhmac.Claims{UserID: 1, Topic: "secure", Expires: time.Now().Add(-time.Hour).Unix()}, secret)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if b.Validate("secure", expired) {
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

	if !b.Validate("contest_session.100", own) {
		t.Fatal("token must validate for its own topic")
	}
	if b.Validate("contest_session.999", own) {
		t.Fatal("token for one contest session must not validate for another")
	}
	if b.Validate("radio.5", own) {
		t.Fatal("contest-session token must not validate for a radio topic")
	}
}
