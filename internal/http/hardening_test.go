package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

func TestRequestBodiesAreBounded(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "")
	body := `{"name":"` + strings.Repeat("x", maxRequestBodyBytes) + `"}`
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/connections", strings.NewReader(body)))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "too large") {
		t.Fatalf("oversized body status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestConnectionAndRunListsPaginate(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		createTestConnection(t, st, db.Connection{ID: fmt.Sprintf("c%d", i), Name: fmt.Sprintf("c%d", i), Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: []byte{1}})
		if err := st.CreateRun(ctx, db.Run{ID: fmt.Sprintf("r%d", i), JobID: "j", Status: "SUCCEEDED", CorrelationID: "c", StartedAt: time.Date(2026, 9, 29, 12, 0, i, 0, time.UTC).Format(time.RFC3339Nano)}); err != nil {
			t.Fatal(err)
		}
	}
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "")
	get := func(path string) (*httptest.ResponseRecorder, []map[string]any) {
		rec := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		var items []map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &items)
		return rec, items
	}

	for _, list := range []string{"/connections", "/runs"} {
		if _, all := get(list); len(all) != 3 {
			t.Fatalf("%s without limit returned %d items, want all 3", list, len(all))
		}
		rec, first := get(list + "?limit=2")
		next := rec.Header().Get("X-Next-Cursor")
		if len(first) != 2 || next == "" {
			t.Fatalf("%s first page items=%d next=%q", list, len(first), next)
		}
		rec, second := get(list + "?limit=2&cursor=" + next)
		if len(second) != 1 || rec.Header().Get("X-Next-Cursor") != "" || second[0]["id"] == first[0]["id"] || second[0]["id"] == first[1]["id"] {
			t.Fatalf("%s second page items=%v next=%q", list, second, rec.Header().Get("X-Next-Cursor"))
		}
		for _, bad := range []string{"?limit=0", "?limit=5000", "?limit=x", "?cursor=abc", "?limit=2&cursor=nope"} {
			if rec, _ := get(list + bad); rec.Code != http.StatusBadRequest {
				t.Fatalf("%s%s status=%d, want 400", list, bad, rec.Code)
			}
		}
	}
}
