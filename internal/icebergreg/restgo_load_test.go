package icebergreg

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// loadFailureCatalog is a REST catalog whose table load always fails with
// the given response. It records every other table request, so a test can
// prove whether registration tried to drop or create the table.
type loadFailureCatalog struct {
	status  int
	errType string
	message string

	mu       sync.Mutex
	requests []string
}

func (c *loadFailureCatalog) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/v1/config":
		_ = json.NewEncoder(w).Encode(map[string]any{"defaults": map[string]string{}, "overrides": map[string]string{}})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/namespaces/ns/tables/orders":
		w.WriteHeader(c.status)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": c.message, "type": c.errType, "code": c.status}})
	default:
		c.mu.Lock()
		c.requests = append(c.requests, r.Method+" "+r.URL.Path)
		c.mu.Unlock()
		// Create and drop are refused, so a test that reaches them fails fast.
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "not implemented", "type": "ServerError", "code": 500}})
	}
}

func (c *loadFailureCatalog) tableRequests() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.requests...)
}

func loadTableThroughCatalog(t *testing.T, c *loadFailureCatalog) error {
	t.Helper()
	server := httptest.NewServer(c)
	defer server.Close()
	ctx := context.Background()
	reg := RunConfig{Enabled: true, Engine: "rest-go", URI: server.URL}
	cat, ident, err := newRESTCatalog(ctx, "bucket", reg, s3io.Config{}, "ns.orders")
	if err != nil {
		t.Fatal(err)
	}
	schema, err := json.Marshal(iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true}))
	if err != nil {
		t.Fatal(err)
	}
	req := RunRequest{RunID: "run-1", SourceEngine: "postgres", SourceMode: "table", DurableIcebergSchema: schema}
	req.DatasetS3.Bucket = "bucket"
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err = loadOrCreateRESTGoTable(ctx, log, cat, ident, req, reg, "ns.orders", "postgres/orders")
	return err
}

// TID-950: a table whose metadata cannot be read is never dropped or
// recreated. Its catalog entry, snapshots and properties stay as they were,
// and the registration fails with a class and a next step for the operator.
func TestMetadataLoadFailureNeverDropsOrRecreatesTheTable(t *testing.T) {
	const missing = "Location does not exist: s3://bucket/postgres/orders/metadata/00003-abc.metadata.json"
	for _, tc := range []struct {
		name      string
		status    int
		errType   string
		message   string
		class     failure.FailureClass
		retryable bool
		contains  string
	}{
		// Ice reports a missing metadata file as 404, which iceberg-go maps
		// to ErrNoSuchTable although the table exists.
		{"missing metadata as 404", http.StatusNotFound, "NotFoundException", missing, failure.FailureTableMetadataUnreadable, false, "registration/retry"},
		{"missing metadata as 500", http.StatusInternalServerError, "NotFoundException", missing, failure.FailureTableMetadataUnreadable, false, "postgres/orders/metadata/"},
		{"catalog unavailable", http.StatusServiceUnavailable, "ServiceUnavailableException", "S3 is not reachable", failure.FailureCatalogUnavailable, true, "load iceberg table ns.orders: ServiceUnavailableException: S3 is not reachable"},
		{"catalog server error", http.StatusBadGateway, "ServerError", "storage read timed out", failure.FailureCatalogUnavailable, true, "storage read timed out"},
		{"forbidden", http.StatusForbidden, "ForbiddenException", "denied", failure.FailureAuthorization, false, "denied"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &loadFailureCatalog{status: tc.status, errType: tc.errType, message: tc.message}
			err := loadTableThroughCatalog(t, c)
			if got := c.tableRequests(); len(got) != 0 {
				t.Fatalf("table requests=%v, want no create or drop", got)
			}
			// Load failures happen after the external-commit boundary; they
			// must still be definite, not reported as an ambiguous commit.
			got := ClassifyFailure(err, true)
			if got.Class != tc.class || got.Retryable != tc.retryable || !got.DefiniteRejection {
				t.Fatalf("classified=%+v err=%v, want %s retryable=%t definite", got, err, tc.class, tc.retryable)
			}
			if !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("err=%v, want it to mention %q", err, tc.contains)
			}
		})
	}
}

// Only the catalog saying the table does not exist creates it.
func TestNoSuchTableCreatesTheTable(t *testing.T) {
	c := &loadFailureCatalog{status: http.StatusNotFound, errType: "NoSuchTableException", message: "Table does not exist: ns.orders"}
	_ = loadTableThroughCatalog(t, c) // the fake catalog refuses the create
	got := c.tableRequests()
	if len(got) != 1 || got[0] != "POST /v1/namespaces/ns/tables" {
		t.Fatalf("table requests=%v, want exactly one create and no drop", got)
	}
}
