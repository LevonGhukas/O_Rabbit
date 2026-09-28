package httpapi

import "github.com/LevonGhukas/O_Rabbit/internal/crypto"

// testCryptoKey is a fixed all-zero AES-256 key; the master refuses to run
// without an encryption key, so tests must supply one too.
var testCryptoKey = func() crypto.Key {
	k, err := crypto.ParseKey("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		panic(err)
	}
	return k
}()
