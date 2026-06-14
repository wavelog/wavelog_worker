package hmac

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

const testSecret = "test-secret-at-least-32-chars-long!!"

func TestSignVerifyRoundtrip(t *testing.T) {
	claims := Claims{UserID: 42, SessionID: 7, Expires: time.Now().Add(time.Hour).Unix()}

	token, err := Sign(claims, testSecret)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	got, err := Verify(token, testSecret)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got != claims {
		t.Fatalf("roundtrip mismatch: got %+v, want %+v", got, claims)
	}
}

func TestVerifyRejects(t *testing.T) {
	valid, err := Sign(Claims{UserID: 1, Expires: time.Now().Add(time.Hour).Unix()}, testSecret)
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	expired, err := Sign(Claims{UserID: 1, Expires: time.Now().Add(-time.Hour).Unix()}, testSecret)
	if err != nil {
		t.Fatalf("Sign expired: %v", err)
	}

	// Tamper with the signature part of an otherwise valid token.
	parts := strings.SplitN(valid, ".", 2)
	tampered := parts[0] + "." + flipLastHex(parts[1])

	// Payload that is not valid hex.
	badEncoding := "zz." + signHelper("zz", testSecret)

	// Payload that is valid hex but not valid JSON claims.
	notJSON := hex.EncodeToString([]byte("not json"))
	badClaims := notJSON + "." + signHelper(notJSON, testSecret)

	tests := []struct {
		name   string
		token  string
		secret string
	}{
		{"malformed no dot", "no-dot-here", testSecret},
		{"empty", "", testSecret},
		{"wrong secret", valid, "another-secret-also-32-chars-long!!!"},
		{"tampered signature", tampered, testSecret},
		{"expired", expired, testSecret},
		{"invalid hex encoding", badEncoding, testSecret},
		{"invalid claims json", badClaims, testSecret},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Verify(tt.token, tt.secret); err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

// signHelper exposes the internal sign() for building deliberately-broken
// tokens whose signature is correct but whose payload is invalid.
func signHelper(payload, secret string) string {
	return sign(payload, secret)
}

// flipLastHex changes the last hex character so the signature no longer matches.
func flipLastHex(s string) string {
	if s == "" {
		return "0"
	}
	last := s[len(s)-1]
	repl := byte('0')
	if last == '0' {
		repl = '1'
	}
	return s[:len(s)-1] + string(repl)
}
