package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
)

func TestHandleMaintenanceSubmitMethodNotAllowed(t *testing.T) {
	srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{}, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/maintenance", nil)

	srv.handleMaintenanceSubmit(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleMaintenanceSubmitInvalidJSON(t *testing.T) {
	srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{}, "")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/maintenance", strings.NewReader("{"))

	srv.handleMaintenanceSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleMaintenanceSubmitInvalidOperation(t *testing.T) {
	srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{}, "")

	body := `{"operation":"drop"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/maintenance", strings.NewReader(body))

	srv.handleMaintenanceSubmit(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleMaintenanceSubmitCompactNotImplemented(t *testing.T) {
	srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{}, "")

	body := `{
		"operation":"compact",
		"iceberg":{"table":"orders.events"}
	}`

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/maintenance", strings.NewReader(body))

	srv.handleMaintenanceSubmit(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusNotImplemented)
	}
}

func TestHandleMaintenanceSubmitVacuumNotImplemented(t *testing.T) {
	srv := NewServer(nil, nil, nil, crypto.Key{}, StatusInfo{}, "")

	body := `{
		"operation":"vacuum",
		"iceberg":{"table":"orders.events"}
	}`

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/maintenance", strings.NewReader(body))

	srv.handleMaintenanceSubmit(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusNotImplemented)
	}
}
