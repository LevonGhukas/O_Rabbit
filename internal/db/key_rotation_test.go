package db

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"testing"

	secretcrypto "github.com/LevonGhukas/O_Rabbit/internal/crypto"
)

func rotationKey(t *testing.T, seed byte) secretcrypto.Key {
	t.Helper()
	k, err := secretcrypto.ParseKey(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{seed}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func seedSealedValues(t *testing.T, st *Store, k secretcrypto.Key) {
	t.Helper()
	ctx := context.Background()
	blob, err := secretcrypto.Encrypt(k, []byte("db-password"), []byte("conn-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO connections(id,name,kind,engine,metadata_json,secret_enc_blob,created_at,updated_at) VALUES('conn-1','c','source','postgres','{}',?,'t','t')`, blob); err != nil {
		t.Fatal(err)
	}
	caKey, err := secretcrypto.Encrypt(k, []byte("ca-private-key"), WorkerCAKeyAAD)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO worker_ca(id,cert_pem,key_enc,created_at) VALUES(1,'pem',?,'t')`, caKey); err != nil {
		t.Fatal(err)
	}
	reg, err := encryptStoredJSON(k, []byte(`{"token":"x"}`), runRegistrationConfigAAD("run-1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO runs(id,job_id,status,correlation_id,started_at,registration_config_json) VALUES('run-1','job','SUCCEEDED','c','t',?)`, reg); err != nil {
		t.Fatal(err)
	}
}

func TestRotateMasterKeyReencryptsEverySealedValue(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	oldKey, newKey := rotationKey(t, 1), rotationKey(t, 2)
	seedSealedValues(t, st, oldKey)

	res, err := st.RotateMasterKey(ctx, oldKey, newKey)
	if err != nil {
		t.Fatal(err)
	}
	if res.ConnectionSecrets != 1 || res.WorkerCAKeys != 1 || res.RunRegistrationConfs != 1 {
		t.Fatalf("result=%+v", res)
	}

	var blob, caKey []byte
	var reg string
	_ = st.db.QueryRowContext(ctx, `SELECT secret_enc_blob FROM connections WHERE id='conn-1'`).Scan(&blob)
	_ = st.db.QueryRowContext(ctx, `SELECT key_enc FROM worker_ca WHERE id=1`).Scan(&caKey)
	_ = st.db.QueryRowContext(ctx, `SELECT registration_config_json FROM runs WHERE id='run-1'`).Scan(&reg)
	if got, err := secretcrypto.Decrypt(newKey, blob, []byte("conn-1")); err != nil || string(got) != "db-password" {
		t.Fatalf("connection secret: %q %v", got, err)
	}
	if _, err := secretcrypto.Decrypt(oldKey, blob, []byte("conn-1")); err == nil {
		t.Fatal("old key still opens the connection secret")
	}
	if got, err := secretcrypto.Decrypt(newKey, caKey, WorkerCAKeyAAD); err != nil || string(got) != "ca-private-key" {
		t.Fatalf("worker CA key: %q %v", got, err)
	}
	if got, err := decryptStoredJSON(newKey, reg, runRegistrationConfigAAD("run-1")); err != nil || string(got) != `{"token":"x"}` {
		t.Fatalf("registration config: %q %v", got, err)
	}
}

func TestRotateMasterKeyIsAllOrNothing(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	oldKey, newKey, otherKey := rotationKey(t, 1), rotationKey(t, 2), rotationKey(t, 3)
	seedSealedValues(t, st, oldKey)
	// One value sealed with a different key makes the rotation fail.
	foreign, _ := secretcrypto.Encrypt(otherKey, []byte("x"), []byte("conn-2"))
	if _, err := st.db.ExecContext(ctx, `INSERT INTO connections(id,name,kind,engine,metadata_json,secret_enc_blob,created_at,updated_at) VALUES('conn-2','c2','source','postgres','{}',?,'t','t')`, foreign); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RotateMasterKey(ctx, oldKey, newKey); !errors.Is(err, ErrKeyRotationDecrypt) {
		t.Fatalf("err=%v", err)
	}
	var blob []byte
	_ = st.db.QueryRowContext(ctx, `SELECT secret_enc_blob FROM connections WHERE id='conn-1'`).Scan(&blob)
	if _, err := secretcrypto.Decrypt(oldKey, blob, []byte("conn-1")); err != nil {
		t.Fatalf("value changed despite failed rotation: %v", err)
	}
}
