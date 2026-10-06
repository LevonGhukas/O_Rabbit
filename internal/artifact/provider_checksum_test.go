package artifact

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
)

func TestValidateProviderChecksum(t *testing.T) {
	sum := sha256.Sum256([]byte("object"))
	digest := hex.EncodeToString(sum[:])
	full := base64.StdEncoding.EncodeToString(sum[:])
	other := sha256.Sum256([]byte("other"))

	if err := ValidateProviderChecksum(full, digest); err != nil {
		t.Fatalf("matching full-object checksum: %v", err)
	}
	if err := ValidateProviderChecksum(base64.StdEncoding.EncodeToString(other[:]), digest); err == nil {
		t.Fatal("a full-object checksum of different bytes must be rejected")
	}
	if err := ValidateProviderChecksum(base64.StdEncoding.EncodeToString(other[:])+"-3", digest); err != nil {
		t.Fatalf("multipart composite checksum must be accepted by form: %v", err)
	}
	for _, bad := range []string{"not-base64!", "AAAA", full + "-0", full + "-x"} {
		if err := ValidateProviderChecksum(bad, digest); err == nil {
			t.Fatalf("malformed checksum %q accepted", bad)
		}
	}
}
