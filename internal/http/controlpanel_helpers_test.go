package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	opsconfigs "github.com/LevonGhukas/O_Rabbit/internal/ops/configs"
	opsdeploy "github.com/LevonGhukas/O_Rabbit/internal/ops/deploy"
)

func TestFirstNonEmptyString(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want string
	}{
		{"none", nil, ""},
		{"skips empty and whitespace", []string{"", "  \t", "  value  ", "later"}, "value"},
		{"trims first value", []string{"  first\n", "second"}, "first"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := firstNonEmptyString(tt.in...); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRequestActor(t *testing.T) {
	tests := []struct {
		name string
		req  *http.Request
		want string
	}{
		{"nil", nil, "api"},
		{"authorization", func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.Header.Set("Authorization", " Bearer token ")
			return r
		}(), "api"},
		{"remote address", func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			r.RemoteAddr = "192.0.2.1:1234"
			return r
		}(), "192.0.2.1:1234"},
		{"fallback", func() *http.Request { r := httptest.NewRequest(http.MethodGet, "/", nil); r.RemoteAddr = ""; return r }(), "api"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requestActor(tt.req); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStringPointers(t *testing.T) {
	if got := ptrString(""); got != nil {
		t.Fatal("empty string should produce nil")
	}
	if got := ptrString("  \t"); got != nil {
		t.Fatal("whitespace should produce nil")
	}
	value := ptrString(" value ")
	if value == nil || *value != " value " {
		t.Fatalf("got %#v", value)
	}
	if derefString(nil) != "" || derefString(value) != " value " {
		t.Fatal("unexpected dereference")
	}
}

func TestLineStreamBuffer(t *testing.T) {
	var got []string
	b := newLineStreamBuffer(func(line string) { got = append(got, line) })
	b.Write(" first\r\n\nsecond")
	b.Write("\r\n third\n")
	b.Flush()
	if want := []string{" first", "second", " third"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	if b.pending != "" {
		t.Fatalf("pending = %q", b.pending)
	}
	var nilBuffer *lineStreamBuffer
	nilBuffer.Write("ignored")
	nilBuffer.Flush()
}

func TestSplitPreservingNonEmpty(t *testing.T) {
	got := splitPreservingNonEmpty("one\r\n\n  \r\ntwo\r\nthree\r")
	want := []string{"one", "two", "three"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestStatusHelpers(t *testing.T) {
	if validationStatusString(opsconfigs.ValidationResult{OK: true}) != "valid" {
		t.Fatal("expected valid")
	}
	if validationStatusString(opsconfigs.ValidationResult{OK: false}) != "invalid" {
		t.Fatal("expected invalid")
	}
	for _, status := range []string{"SUCCEEDED", "FAILED", "CANCELED"} {
		if deploymentStatusForExecution(status) != status {
			t.Fatalf("status %q was not preserved", status)
		}
	}
	for _, status := range []string{"", "RUNNING", "PENDING", "unknown"} {
		if deploymentStatusForExecution(status) != "RUNNING" {
			t.Fatalf("status %q was not mapped to RUNNING", status)
		}
	}
}

func TestJSONHelpers(t *testing.T) {
	if got := parseStringSliceJSON(nil); !reflect.DeepEqual(got, []string{}) {
		t.Fatalf("slice nil: %#v", got)
	}
	if got := parseStringSliceJSON(json.RawMessage(`malformed`)); !reflect.DeepEqual(got, []string{}) {
		t.Fatalf("slice malformed: %#v", got)
	}
	if got := parseStringSliceJSON(json.RawMessage(`["a","b"]`)); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("slice: %#v", got)
	}
	if got := parseStringMapJSON(json.RawMessage(`null`)); !reflect.DeepEqual(got, map[string]string{}) {
		t.Fatalf("map null: %#v", got)
	}
	if got := parseStringMapJSON(json.RawMessage(`{"a":"b"}`)); !reflect.DeepEqual(got, map[string]string{"a": "b"}) {
		t.Fatalf("map: %#v", got)
	}
	if got := parseStringMapJSON(json.RawMessage(`bad`)); !reflect.DeepEqual(got, map[string]string{}) {
		t.Fatalf("map malformed: %#v", got)
	}
	if got := string(toRawJSON(nil, `{"fallback":true}`)); got != `{"fallback":true}` {
		t.Fatalf("nil fallback: %s", got)
	}
	if got := string(toRawJSON(func() {}, `fallback`)); got != "fallback" {
		t.Fatalf("marshal fallback: %s", got)
	}
	if got := string(toRawJSON(map[string]int{"x": 1}, `fallback`)); got != `{"x":1}` {
		t.Fatalf("json: %s", got)
	}
	params := deploymentParamsJSON(opsdeploy.DeploymentParams{})
	if !json.Valid(params) {
		t.Fatalf("deployment params are not valid JSON: %s", params)
	}
}

func TestNowTimeUTC(t *testing.T) {
	before := time.Now().UTC()
	got := nowTimeUTC()
	after := time.Now().UTC()
	if got.Location() != time.UTC || got.Before(before) || got.After(after) {
		t.Fatalf("got %v outside UTC time window", got)
	}
}
