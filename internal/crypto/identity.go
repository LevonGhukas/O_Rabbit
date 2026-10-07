package crypto

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

// IdentityTag returns a stable, non-reversible tag for a secret-bearing value
// such as a source DSN. Equal (domain, value) pairs always map to the same
// tag, while the tag reveals nothing about the value without the master key,
// so it is safe to use in resource names, logs and API responses.
//
// domain separates unrelated uses so tags are never comparable across them.
func IdentityTag(k Key, domain string, value []byte) (string, error) {
	if k.IsZero() {
		return "", errors.New("identity tag requires a master key")
	}
	derived := hmac.New(sha256.New, k.key)
	derived.Write([]byte("orabbit-identity-v1:" + domain))
	mac := hmac.New(sha256.New, derived.Sum(nil))
	mac.Write(value)
	return hex.EncodeToString(mac.Sum(nil)[:8]), nil
}
