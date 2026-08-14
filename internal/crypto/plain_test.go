package crypto

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"os"
	"testing"
)

func TestEncryptDecryptPlainCompatNoKey(t *testing.T) {
	k := Key{}
	aad := []byte("conn-1")
	plain := []byte(`{"dsn":"postgres://u:p@h/db"}`)
	blob, err := Encrypt(k, plain, aad)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if len(blob) < 2 || blob[0] != blobVersion0Plain {
		t.Fatalf("expected plaintext blob version")
	}
	got, err := Decrypt(k, blob, aad)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("decrypted plaintext mismatch: got=%q want=%q", got, plain)
	}
}

func TestEncryptDecryptAESGCM(t *testing.T) {
	keyBytes := bytes.Repeat([]byte{0x7a}, 32)
	k := Key{key: keyBytes}
	aad := []byte("conn-2")
	plain := []byte(`{"access_key_id":"a","secret_access_key":"b"}`)

	blob, err := Encrypt(k, plain, aad)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if len(blob) < 1+12+16 || blob[0] != blobVersion1AESGCM {
		t.Fatalf("expected encrypted blob version")
	}

	got, err := Decrypt(k, blob, aad)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Fatalf("decrypted plaintext mismatch: got=%q want=%q", got, plain)
	}
}

func TestDecryptEncryptedRequiresKey(t *testing.T) {
	keyBytes := bytes.Repeat([]byte{0x11}, 32)
	encKey := Key{key: keyBytes}
	blob, err := Encrypt(encKey, []byte("secret"), []byte("aad"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	if _, err := Decrypt(Key{}, blob, []byte("aad")); err == nil {
		t.Fatalf("expected decrypt error without key")
	}
}

func TestLoadMasterKeyFromEnvParsesBase64AndHex(t *testing.T) {
	base := bytes.Repeat([]byte{0x42}, 32)
	b64 := base64.StdEncoding.EncodeToString(base)
	if err := os.Setenv("ORABBIT_MASTER_KEY", b64); err != nil {
		t.Fatalf("Setenv base64: %v", err)
	}
	t.Cleanup(func() { _ = os.Unsetenv("ORABBIT_MASTER_KEY") })

	k, err := LoadMasterKeyFromEnv()
	if err != nil {
		t.Fatalf("LoadMasterKeyFromEnv(base64): %v", err)
	}
	if !bytes.Equal(k.key, base) {
		t.Fatalf("decoded base64 key mismatch")
	}

	hexKey := hex.EncodeToString(base)
	if err := os.Setenv("ORABBIT_MASTER_KEY", hexKey); err != nil {
		t.Fatalf("Setenv hex: %v", err)
	}
	k, err = LoadMasterKeyFromEnv()
	if err != nil {
		t.Fatalf("LoadMasterKeyFromEnv(hex): %v", err)
	}
	if !bytes.Equal(k.key, base) {
		t.Fatalf("decoded hex key mismatch")
	}
}
