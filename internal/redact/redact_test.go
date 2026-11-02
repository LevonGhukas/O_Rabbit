package redact

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestText(t *testing.T) {
	cases := []struct{ in, want string }{
		{"dial postgres://app:s3cr3t@db:5432/x failed", "dial postgres://app:***@db:5432/x failed"},
		{"Authorization: Bearer abcdefghijkl123", "Authorization: Bearer ***"},
		{"login password=hunter2 user=bob", "login password=*** user=bob"},
		{`{"secret_access_key":"AKIAxyz","region":"us-east-1"}`, `{"secret_access_key":"***","region":"us-east-1"}`},
		{"ORABBIT_WORKER_AUTH_TOKEN='abc def'", "ORABBIT_WORKER_AUTH_TOKEN='***'"},
		{"partition_key=day rows=10", "partition_key=day rows=10"}, // not a secret
		{"plain message", "plain message"},
	}
	for _, c := range cases {
		if got := Text(c.in); got != c.want {
			t.Errorf("Text(%q)\n got  %q\n want %q", c.in, got, c.want)
		}
	}
}

func TestTextPrivateKey(t *testing.T) {
	in := "key:\n-----BEGIN OPENSSH PRIVATE KEY-----\nAAAAB3Nza\n-----END OPENSSH PRIVATE KEY-----\ndone"
	got := Text(in)
	if strings.Contains(got, "AAAAB3Nza") {
		t.Fatalf("private key leaked: %q", got)
	}
}

func TestJSON(t *testing.T) {
	raw := json.RawMessage(`{"dsn":"postgres://u:p@h/db","worker_id":"w1","nested":{"password":"x","note":"token=abc"},"list":["https://u:p@h"],"n":3}`)
	var got map[string]any
	if err := json.Unmarshal(JSON(raw), &got); err != nil {
		t.Fatal(err)
	}
	if got["dsn"] != Mask || got["worker_id"] != "w1" || got["n"] != float64(3) {
		t.Fatalf("top level: %v", got)
	}
	nested := got["nested"].(map[string]any)
	if nested["password"] != Mask || nested["note"] != "token=***" {
		t.Fatalf("nested: %v", nested)
	}
	if got["list"].([]any)[0] != "https://u:***@h" {
		t.Fatalf("list: %v", got["list"])
	}
}

func TestJSONInvalidIsTreatedAsText(t *testing.T) {
	out := string(JSON(json.RawMessage(`not json password=x`)))
	if strings.Contains(out, "password=x") {
		t.Fatalf("leaked: %s", out)
	}
}
