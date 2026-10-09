package crypto

import (
	"strings"
	"testing"
)

func TestIdentityTag(t *testing.T) {
	k, err := ParseKey(strings.Repeat("ab", 32))
	if err != nil {
		t.Fatal(err)
	}
	other, err := ParseKey(strings.Repeat("cd", 32))
	if err != nil {
		t.Fatal(err)
	}
	dsn := []byte("postgres://u:secret@db:5432/app")

	a, err := IdentityTag(k, "source", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := IdentityTag(k, "source", dsn); a != b {
		t.Fatalf("tag not stable: %s != %s", a, b)
	}
	if len(a) != 16 {
		t.Fatalf("tag length = %d, want 16", len(a))
	}
	if strings.Contains(a, "secret") {
		t.Fatalf("tag leaks input: %s", a)
	}
	if b, _ := IdentityTag(k, "source", []byte("postgres://u:secret@db:5432/other")); a == b {
		t.Fatal("different values produced the same tag")
	}
	if b, _ := IdentityTag(k, "target", dsn); a == b {
		t.Fatal("different domains produced the same tag")
	}
	if b, _ := IdentityTag(other, "source", dsn); a == b {
		t.Fatal("different keys produced the same tag")
	}
	if _, err := IdentityTag(Key{}, "source", dsn); err == nil {
		t.Fatal("zero key must be rejected")
	}
}
