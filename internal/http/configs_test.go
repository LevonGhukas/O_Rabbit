package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/httperr"
)

func TestHandleServerConfigsRouting(t *testing.T) {
	tests := []struct {
		name   string
		method string
		parts  []string
		want   int
		allow  string
	}{
		{"collection method", http.MethodPost, nil, http.StatusMethodNotAllowed, http.MethodGet},
		{"item method", http.MethodDelete, []string{"app.env"}, http.StatusMethodNotAllowed, "GET, PUT"},
		{"validate method", http.MethodGet, []string{"app.env", "validate"}, http.StatusMethodNotAllowed, http.MethodPost},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{}, "")
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, "/servers/s1/configs", nil)
			srv.handleServerConfigs(rec, req, "s1", tt.parts)
			resp := decodeErrorResponse(t, rec)
			if rec.Code != tt.want {
				t.Fatalf("status=%d want=%d", rec.Code, tt.want)
			}
			if resp.Error.Code != httperr.CodeMethodNotAllowed {
				t.Fatalf("code=%q", resp.Error.Code)
			}
			if got := rec.Header().Get("Allow"); got != tt.allow {
				t.Fatalf("Allow=%q want=%q", got, tt.allow)
			}
		})
	}
}

func TestHandleServerConfigsUnknownRoutes(t *testing.T) {
	srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{}, "")
	for _, parts := range [][]string{{"app.env", "other"}, {"app.env", "validate", "extra"}} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/servers/s1/configs", nil)
		srv.handleServerConfigs(rec, req, "s1", parts)
		if rec.Code != http.StatusNotFound {
			t.Errorf("parts=%v status=%d want=%d", parts, rec.Code, http.StatusNotFound)
		}
	}
}

func TestHandleValidateConfigInvalidJSON(t *testing.T) {
	st := openTestStore(t)
	server := db.Server{ID: "s1", Name: "server", Host: "example.test", SSHUser: "tester", ProjectDir: "/tmp/project"}
	if _, err := st.CreateServer(context.Background(), server); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/servers/s1/configs/app.env/validate", strings.NewReader("{"))
	srv.handleServerConfigs(rec, req, "s1", []string{"app.env", "validate"})
	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusBadRequest)
	}
	if resp.Error.Code != httperr.CodeInvalidInput {
		t.Fatalf("code=%q", resp.Error.Code)
	}
}

func TestHandleValidateConfig(t *testing.T) {
	st := openTestStore(t)
	server := db.Server{ID: "s1", Name: "server", Host: "example.test", SSHUser: "tester", ProjectDir: "/tmp/project"}
	if _, err := st.CreateServer(context.Background(), server); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/servers/s1/configs/app.env/validate", strings.NewReader(`{"content":"PORT=8080\n"}`))
	srv.handleServerConfigs(rec, req, "s1", []string{"app.env", "validate"})
	var body map[string]any
	decodeJSONBody(t, rec, &body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusOK)
	}
	if body["config_id"] != "app.env" {
		t.Fatalf("config_id=%v", body["config_id"])
	}
	if _, ok := body["ok"].(bool); !ok {
		t.Fatalf("ok=%T", body["ok"])
	}
	if _, ok := body["validation"].(map[string]any); !ok {
		t.Fatalf("validation=%T", body["validation"])
	}
	if !json.Valid(rec.Body.Bytes()) {
		t.Fatal("response is not valid JSON")
	}
}
