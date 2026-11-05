package icebergreg

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestIceBackendUsesRESTCatalogToProveStableTableAbsence(t *testing.T) {
	var tableReads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/config":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"defaults":  map[string]string{},
				"overrides": map[string]string{},
				"endpoints": []string{},
			})
		case strings.Contains(r.URL.Path, "/namespaces/ns/tables/orders"):
			tableReads.Add(1)
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": map[string]any{
					"message": "table does not exist",
					"type":    "NoSuchTableException",
					"code":    http.StatusNotFound,
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	observation, err := NewManager(nil).InspectCatalog(
		context.Background(),
		InspectionRequest{
			Registration: RunConfig{
				Enabled: true,
				Engine:  "ice",
				URI:     server.URL,
			},
			Table:         "ns.orders",
			DatasetBucket: "bucket",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if observation.Backend != "rest-go" ||
		observation.TableExists ||
		!observation.CompleteHistory ||
		observation.MetadataStart != "TABLE_NOT_FOUND" ||
		observation.MetadataEnd != "TABLE_NOT_FOUND" {
		t.Fatalf("observation=%+v", observation)
	}
	if tableReads.Load() != 2 {
		t.Fatalf("table reads=%d want 2", tableReads.Load())
	}
}

func TestInspectCatalogDeadlineInterruptsHungRESTCatalog(t *testing.T) {
	const (
		deadline  = 200 * time.Millisecond
		maxReturn = 5 * time.Second
	)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	start := time.Now()
	_, err := NewManager(nil).InspectCatalog(ctx, InspectionRequest{
		Registration:  RunConfig{Enabled: true, Engine: "rest-go", URI: server.URL},
		Table:         "ns.orders",
		DatasetBucket: "bucket",
	})
	if err == nil {
		t.Fatal("inspection of a hung catalog succeeded")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > maxReturn {
		t.Fatalf("inspection returned after %s, deadline was %s", elapsed, deadline)
	}
}
