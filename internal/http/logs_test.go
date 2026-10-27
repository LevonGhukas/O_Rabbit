package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/logbuffer"
)

func seedLogEvents(t *testing.T, st *db.Store) {
	t.Helper()
	ctx := context.Background()
	if err := st.CreateRun(ctx, db.Run{ID: "run-1", JobID: "job-1", DatasetKey: "k", Status: "RUNNING", CorrelationID: "c", StartedAt: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	for _, e := range []db.Event{
		{ID: "e1", RunID: "run-1", TS: "2026-10-01T10:00:00Z", Level: "INFO", Message: "planned 4 tasks", FieldsJSON: []byte(`{}`)},
		{ID: "e2", RunID: "run-1", TS: "2026-10-01T10:01:00Z", Level: "ERROR", Message: "dial postgres://app:hunter2@db/x failed", FieldsJSON: []byte(`{"dsn":"postgres://app:hunter2@db/x","worker_id":"w1"}`)},
	} {
		if err := st.InsertEvent(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEventsEndpointFiltersCountsAndMasks(t *testing.T) {
	st := openTestStore(t)
	seedLogEvents(t, st)
	srv := NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "")

	rec := serveRoute(srv, http.MethodGet, "/api/v1/events?level=error&run_id=run-1&count=1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if strings.Contains(body, "hunter2") {
		t.Fatalf("secret leaked: %s", body)
	}
	var resp struct {
		Items []db.EventRecord `json:"items"`
		Total int              `json:"total"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "e2" || resp.Total != 1 || resp.Items[0].WorkerID != "w1" {
		t.Fatalf("unexpected response: %+v", resp)
	}

	for _, bad := range []string{"level=LOUD", "since=yesterday", "limit=5000", "before=-1"} {
		if rec := serveRoute(srv, http.MethodGet, "/api/v1/events?"+bad, ""); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status=%d, want 400", bad, rec.Code)
		}
	}
}

func TestMasterLogsEndpoint(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, testCryptoKey, StatusInfo{}, "")
	if rec := serveRoute(srv, http.MethodGet, "/api/v1/logs/master", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("without buffer: status=%d, want 503", rec.Code)
	}
	buf := logbuffer.New(10)
	log := slog.New(logbuffer.NewHandler(slog.NewTextHandler(&strings.Builder{}, nil), buf))
	log.Info("started", "token", "abc")
	log.Error("boom")
	srv.SetLogBuffer(buf)

	rec := serveRoute(srv, http.MethodGet, "/api/v1/logs/master?level=error", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "boom") || strings.Contains(rec.Body.String(), "started") {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := serveRoute(srv, http.MethodGet, "/api/v1/logs/master", ""); strings.Contains(rec.Body.String(), "abc") {
		t.Fatalf("token leaked: %s", rec.Body.String())
	}
}

// readLogEvent returns the data of the next "log" SSE event.
func readLogEvent(t *testing.T, sc *bufio.Scanner) (id string, data string) {
	t.Helper()
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			return id, strings.TrimPrefix(line, "data: ")
		}
	}
	t.Fatalf("stream ended: %v", sc.Err())
	return "", ""
}

func TestEventsStreamTailsNewEventsAndResumes(t *testing.T) {
	st := openTestStore(t)
	seedLogEvents(t, st)
	ts := httptest.NewServer(NewServer(nil, st, nil, testCryptoKey, StatusInfo{}, "").Handler())
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/v1/events/stream?run_id=run-1", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", res.StatusCode)
	}

	// Existing events are history; only new ones are streamed.
	if err := st.InsertEvent(ctx, db.Event{ID: "e3", RunID: "run-1", TS: "2026-10-01T10:02:00Z", Level: "WARN", Message: "retrying password=s3cret", FieldsJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	id, data := readLogEvent(t, bufio.NewScanner(res.Body))
	if !strings.Contains(data, `"e3"`) || strings.Contains(data, "s3cret") {
		t.Fatalf("got %s", data)
	}

	// A reconnect with Last-Event-ID gets what it missed, nothing older.
	if err := st.InsertEvent(ctx, db.Event{ID: "e4", RunID: "run-1", TS: "2026-10-01T10:03:00Z", Level: "INFO", Message: "after reconnect", FieldsJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	req2, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/v1/events/stream?run_id=run-1", nil)
	req2.Header.Set("Last-Event-ID", id)
	res2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer res2.Body.Close()
	if _, data := readLogEvent(t, bufio.NewScanner(res2.Body)); !strings.Contains(data, `"e4"`) {
		t.Fatalf("resume got %s, want e4", data)
	}
}
