package icebergreg

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
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
