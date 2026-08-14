package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/httperr"
)

func TestUnsubscribeDoesNotCloseChannel(t *testing.T) {
	b := NewBroadcaster(nil)
	ch, unsub := b.Subscribe("run-1")

	unsub()

	select {
	case _, ok := <-ch:
		if !ok {
			t.Fatalf("subscriber channel was closed on unsubscribe")
		}
		t.Fatalf("unexpected event in channel after unsubscribe")
	default:
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("publish panicked after unsubscribe: %v", r)
		}
	}()
	b.Publish(db.Event{RunID: "run-1"})
}

func TestSSEHandlerMissingRunIDReturnsJSONError(t *testing.T) {
	h := SSEHandler(nil, nil, NewBroadcaster(nil))
	req := httptest.NewRequest(http.MethodGet, "/sse", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusBadRequest)
	}
	if resp.Error.Code != httperr.CodeInvalidInput {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeInvalidInput)
	}
	if resp.Error.Message != "missing run_id" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "missing run_id")
	}
	details := detailsMap(t, resp.Error.Details)
	if details["field"] != "run_id" {
		t.Fatalf("field detail=%v want=%q", details["field"], "run_id")
	}
}

func TestSSEHandlerNilBroadcasterReturnsStructuredError(t *testing.T) {
	h := SSEHandler(nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/sse?run_id=run-1", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusServiceUnavailable)
	}
	if resp.Error.Code != httperr.CodeDependencyUnavailable {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeDependencyUnavailable)
	}
	if resp.Error.Message != "event stream unavailable" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "event stream unavailable")
	}
}

func TestSSEHandlerMissingRunReturnsStructuredError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.sqlite")
	st, err := db.Open(context.Background(), db.Config{Path: path}, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	h := SSEHandler(nil, st, NewBroadcaster(nil))
	req := httptest.NewRequest(http.MethodGet, "/sse?run_id=missing-run", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	resp := decodeErrorResponse(t, rec)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d want=%d", rec.Code, http.StatusNotFound)
	}
	if resp.Error.Code != httperr.CodeNotFound {
		t.Fatalf("error code=%q want=%q", resp.Error.Code, httperr.CodeNotFound)
	}
	if resp.Error.Message != "run not found" {
		t.Fatalf("error message=%q want=%q", resp.Error.Message, "run not found")
	}
}
