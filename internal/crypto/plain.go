package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// Layout for plaintext blobs: [version=0][plaintext...]
const blobVersion0Plain byte = 0

// Layout for encrypted blobs: [version=1][12-byte nonce][ciphertext+tag...]
const blobVersion1AESGCM byte = 1

// Key is the master key used for secret encryption-at-rest.
// A zero-value key means plaintext compatibility mode.
type Key struct {
	key []byte
}

// LoadMasterKeyFromEnv loads ORABBIT_MASTER_KEY.
// Supported formats:
// - base64-encoded 32-byte key
// - hex-encoded 32-byte key (64 chars)
func LoadMasterKeyFromEnv() (Key, error) {
	raw := strings.TrimSpace(os.Getenv("ORABBIT_MASTER_KEY"))
	if raw == "" {
		return Key{}, nil
	}
	k, err := decodeKey(raw)
	if err != nil {
		return Key{}, err
	}
	return Key{key: k}, nil
}

// IsZero returns true if no encryption key is configured.
func (k Key) IsZero() bool { return len(k.key) == 0 }

// Encrypt encrypts a secret blob.
// When key is empty, it stores plaintext with a version marker for backward compatibility.
func Encrypt(k Key, plaintext, aad []byte) ([]byte, error) {
	if k.IsZero() {
		out := make([]byte, 0, 1+len(plaintext))
		out = append(out, blobVersion0Plain)
		out = append(out, plaintext...)
		return out, nil
	}

	aead, err := newAEAD(k)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, aad)
	out := make([]byte, 0, 1+len(nonce)+len(ciphertext))
	out = append(out, blobVersion1AESGCM)
	out = append(out, nonce...)
	out = append(out, ciphertext...)
	return out, nil
}

// Decrypt decrypts the stored secret blob.
// - version 0: plaintext passthrough (legacy/default when no key configured)
// - version 1: AES-256-GCM using ORABBIT_MASTER_KEY
func Decrypt(k Key, blob, aad []byte) ([]byte, error) {
	if len(blob) == 0 {
		return nil, errors.New("secret blob empty")
	}
	switch blob[0] {
	case blobVersion0Plain:
		return blob[1:], nil
	case blobVersion1AESGCM:
		if k.IsZero() {
			return nil, errors.New("encrypted secret requires ORABBIT_MASTER_KEY")
		}
		if len(blob) < 1+12 {
			return nil, errors.New("encrypted secret blob too short")
		}
		aead, err := newAEAD(k)
		if err != nil {
			return nil, err
		}
		nonce := blob[1 : 1+12]
		ciphertext := blob[1+12:]
		plaintext, err := aead.Open(nil, nonce, ciphertext, aad)
		if err != nil {
			return nil, fmt.Errorf("decrypt secret: %w", err)
		}
		return plaintext, nil
	default:
		// Back-compat: accept historical blobs that were stored without a version prefix.
		return blob, nil
	}
}

func decodeKey(raw string) ([]byte, error) {
	if isLikelyHex(raw) {
		b, err := hex.DecodeString(raw)
		if err != nil {
			return nil, errors.New("invalid ORABBIT_MASTER_KEY format: expected base64 or hex encoded 32-byte key")
		}
		if len(b) != 32 {
			return nil, fmt.Errorf("invalid ORABBIT_MASTER_KEY length: got %d bytes after hex decode, want 32", len(b))
		}
		return b, nil
	}
	if b, err := base64.StdEncoding.DecodeString(raw); err == nil {
		if len(b) != 32 {
			return nil, fmt.Errorf("invalid ORABBIT_MASTER_KEY length: got %d bytes after base64 decode, want 32", len(b))
		}
		return b, nil
	}
	return nil, errors.New("invalid ORABBIT_MASTER_KEY format: expected base64 or hex encoded 32-byte key")
}

func isLikelyHex(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F') {
			continue
		}
		return false
	}
	return true
}

func newAEAD(k Key) (cipher.AEAD, error) {
	if len(k.key) != 32 {
		return nil, fmt.Errorf("master key must be 32 bytes, got %d", len(k.key))
	}
	block, err := aes.NewCipher(k.key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
