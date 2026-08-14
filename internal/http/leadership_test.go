package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

type fakeLeadership struct {
	status db.Leadership
	err    error
}

func (f fakeLeadership) Assert(context.Context) error { return f.err }
func (f fakeLeadership) Status() db.Leadership        { return f.status }

func TestLeadershipControlsReadinessAndMutations(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "")
	srv.SetLeadershipGuard(fakeLeadership{status: db.Leadership{State: "LOST", Ready: false}, err: errors.New("lost")})
	h := srv.Handler()

	ready := httptest.NewRecorder()
	h.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready status=%d", ready.Code)
	}
	mutation := httptest.NewRecorder()
	h.ServeHTTP(mutation, httptest.NewRequest(http.MethodPost, "/runs", nil))
	if mutation.Code != http.StatusServiceUnavailable {
		t.Fatalf("mutation status=%d", mutation.Code)
	}
	health := httptest.NewRecorder()
	h.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d", health.Code)
	}
}
