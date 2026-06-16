package hmac

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Claims is the payload embedded in a signed token.
//
// Topic binds the token to exactly one topic. The worker must compare it against
// the topic the client tries to subscribe to — a valid signature alone does not
// authorize access to an arbitrary topic.
type Claims struct {
	UserID  int    `json:"user_id"`
	Topic   string `json:"topic"`
	Expires int64  `json:"expires"` // Unix timestamp
}

// Sign produces a URL-safe token: base64url(json(claims)).hex(hmac-sha256).
func Sign(claims Claims, secret string) (string, error) {
	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	encoded := hex.EncodeToString(payload)
	sig := sign(encoded, secret)
	return encoded + "." + sig, nil
}

// Verify parses and validates a token. Returns Claims on success.
// Rejects expired tokens and tokens with a bad signature.
func Verify(token, secret string) (Claims, error) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return Claims{}, fmt.Errorf("malformed token")
	}
	encoded, sig := parts[0], parts[1]

	if expected := sign(encoded, secret); !hmac.Equal([]byte(expected), []byte(sig)) {
		return Claims{}, fmt.Errorf("invalid signature")
	}

	raw, err := hex.DecodeString(encoded)
	if err != nil {
		return Claims{}, fmt.Errorf("invalid encoding")
	}
	var c Claims
	if err := json.Unmarshal(raw, &c); err != nil {
		return Claims{}, fmt.Errorf("invalid claims")
	}
	if time.Now().Unix() > c.Expires {
		return Claims{}, fmt.Errorf("token expired")
	}
	return c, nil
}

func sign(payload, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}
