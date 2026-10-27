package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/logbuffer"
	"github.com/LevonGhukas/O_Rabbit/internal/redact"
)

// Log viewer endpoints (GET /events, /events/stream, /logs/master,
// /logs/master/stream). Everything they return is masked with package redact.

const (
	// maxLogStreams caps concurrent live log streams so viewers cannot load the master.
	maxLogStreams = 16
	// logTailInterval is how often a stream checks for new entries; one indexed
	// query per stream per interval.
	logTailInterval = time.Second
	logTailBatch    = 500
	logKeepalive    = 15 * time.Second
)

// SetLogBuffer enables GET /logs/master. Without it the route answers 503.
func (s *Server) SetLogBuffer(buf *logbuffer.Buffer) {
	s.logBuf = buf
}

type logQuery struct {
	levels []string
	query  string
	since  time.Time
	until  time.Time
	before int64
	after  int64
	limit  int
}

// parseLogQuery reads the filters shared by every log route.
func parseLogQuery(r *http.Request) (logQuery, error) {
	q := r.URL.Query()
	var out logQuery
	for _, raw := range q["level"] {
		for _, l := range strings.Split(raw, ",") {
			if l = strings.TrimSpace(l); l == "" {
				continue
			}
			if db.NormalizeEventLevel(l) == "" {
				return out, fmt.Errorf("level must be DEBUG, INFO, WARN, ERROR or CRITICAL, got %q", l)
			}
			out.levels = append(out.levels, db.NormalizeEventLevel(l))
		}
	}
	out.query = strings.TrimSpace(q.Get("q"))
	if len(out.query) > 200 {
		return out, fmt.Errorf("q must be at most 200 characters")
	}
	for name, dst := range map[string]*time.Time{"since": &out.since, "until": &out.until} {
		if raw := strings.TrimSpace(q.Get(name)); raw != "" {
			t, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				return out, fmt.Errorf("%s must be an RFC 3339 time, got %q", name, raw)
			}
			*dst = t.UTC()
		}
	}
	for name, dst := range map[string]*int64{"before": &out.before, "after": &out.after} {
		if raw := strings.TrimSpace(q.Get(name)); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || n < 0 {
				return out, fmt.Errorf("%s must be a non-negative sequence number", name)
			}
			*dst = n
		}
	}
	// A reconnecting EventSource sends the last id it saw.
	if raw := strings.TrimSpace(r.Header.Get("Last-Event-ID")); raw != "" && out.after == 0 {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			out.after = n
		}
	}
	out.limit = 200
	if raw := strings.TrimSpace(q.Get("limit")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 1000 {
			return out, fmt.Errorf("limit must be between 1 and 1000")
		}
		out.limit = n
	}
	return out, nil
}

func (q logQuery) eventFilter(r *http.Request) db.EventFilter {
	p := r.URL.Query()
	f := db.EventFilter{
		Levels:   q.levels,
		RunID:    p.Get("run_id"),
		JobID:    p.Get("job_id"),
		TaskID:   p.Get("task_id"),
		WorkerID: p.Get("worker_id"),
		Query:    q.query,
		Before:   q.before,
		After:    q.after,
		Limit:    q.limit,
	}
	if !q.since.IsZero() {
		f.Since = q.since.Format(time.RFC3339Nano)
	}
	if !q.until.IsZero() {
		f.Until = q.until.Format(time.RFC3339Nano)
	}
	return f
}

func maskEvent(e db.EventRecord) db.EventRecord {
	e.Message = redact.Text(e.Message)
	e.FieldsJSON = redact.JSON(e.FieldsJSON)
	return e
}

// handleEvents serves GET /events: run, task and job events across all runs.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	q, err := parseLogQuery(r)
	if err != nil {
		writeInvalidInput(w, err.Error(), nil)
		return
	}
	f := q.eventFilter(r)
	f.After = 0 // history pages go backwards; the stream handles "after"
	items, err := s.st.QueryEvents(r.Context(), f)
	if err != nil {
		writeInternalError(w, "failed to query events")
		return
	}
	for i := range items {
		items[i] = maskEvent(items[i])
	}
	resp := map[string]any{"items": items, "next_before": int64(0)}
	if len(items) == f.Limit {
		resp["next_before"] = items[len(items)-1].Seq
	}
	// Counting is optional: it is the expensive part of a broad filter.
	if r.URL.Query().Get("count") == "1" {
		total, capped, err := s.st.CountEvents(r.Context(), f)
		if err != nil {
			writeInternalError(w, "failed to count events")
			return
		}
		resp["total"] = total
		resp["total_capped"] = capped
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleEventsStream serves GET /events/stream: new matching events as SSE.
// It tails the table instead of the in-memory broadcaster, so it sees every
// stored event and applies exactly the same filters as GET /events.
func (s *Server) handleEventsStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	q, err := parseLogQuery(r)
	if err != nil {
		writeInvalidInput(w, err.Error(), nil)
		return
	}
	f := q.eventFilter(r)
	f.Before = 0
	if f.After == 0 {
		// Start from "now": history comes from GET /events.
		if f.After, err = s.st.LatestEventSeq(r.Context()); err != nil {
			writeInternalError(w, "failed to start event stream")
			return
		}
	}
	f.Limit = logTailBatch
	s.serveLogStream(w, r, nil, func() (int, error) {
		items, err := s.st.QueryEvents(r.Context(), f)
		if err != nil {
			return 0, err
		}
		for _, e := range items {
			writeLogEvent(w, e.Seq, maskEvent(e))
			f.After = e.Seq
		}
		return len(items), nil
	})
}

// handleMasterLogs serves GET /logs/master from the in-memory buffer.
func (s *Server) handleMasterLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	if s.logBuf == nil {
		writeDependencyUnavailable(w, "master log buffer is not enabled")
		return
	}
	q, err := parseLogQuery(r)
	if err != nil {
		writeInvalidInput(w, err.Error(), nil)
		return
	}
	f := q.bufferFilter()
	f.After = 0
	items, total := s.logBuf.Query(f)
	resp := map[string]any{"items": items, "total": total, "total_capped": false, "next_before": int64(0), "capacity": logbuffer.DefaultCapacity}
	if len(items) == f.Limit {
		resp["next_before"] = items[len(items)-1].Seq
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleMasterLogsStream serves GET /logs/master/stream as SSE.
func (s *Server) handleMasterLogsStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	if s.logBuf == nil {
		writeDependencyUnavailable(w, "master log buffer is not enabled")
		return
	}
	q, err := parseLogQuery(r)
	if err != nil {
		writeInvalidInput(w, err.Error(), nil)
		return
	}
	f := q.bufferFilter()
	f.Before = 0
	if f.After == 0 {
		f.After = s.logBuf.LatestSeq()
	}
	f.Limit = logTailBatch
	wake, cancel := s.logBuf.Subscribe()
	defer cancel()
	s.serveLogStream(w, r, wake, func() (int, error) {
		items, _ := s.logBuf.Query(f)
		for _, e := range items {
			writeLogEvent(w, e.Seq, e)
			f.After = e.Seq
		}
		return len(items), nil
	})
}

func (q logQuery) bufferFilter() logbuffer.Filter {
	return logbuffer.Filter{Levels: q.levels, Query: q.query, Since: q.since, Until: q.until, Before: q.before, After: q.after, Limit: q.limit}
}

// serveLogStream runs the SSE loop: poll on a timer (and right away when wake
// fires), keep the connection alive, stop when the client goes away.
func (s *Server) serveLogStream(w http.ResponseWriter, r *http.Request, wake <-chan logbuffer.Entry, poll func() (int, error)) {
	fl, ok := w.(http.Flusher)
	if !ok {
		writeInternalError(w, "streaming unavailable")
		return
	}
	if s.logStreams.Add(1) > maxLogStreams {
		s.logStreams.Add(-1)
		writeDependencyUnavailable(w, fmt.Sprintf("too many live log viewers (limit %d); close another log tab and retry", maxLogStreams))
		return
	}
	defer s.logStreams.Add(-1)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Tell EventSource clients how long to wait before reconnecting.
	fmt.Fprint(w, "retry: 3000\n\n")
	fl.Flush()

	tick := time.NewTicker(logTailInterval)
	defer tick.Stop()
	keep := time.NewTicker(logKeepalive)
	defer keep.Stop()
	drain := func() bool {
		// Keep reading while full batches come back, so a burst is sent at once.
		for {
			n, err := poll()
			if err != nil {
				if r.Context().Err() == nil {
					s.log.Warn("log stream query failed", "err", err.Error())
				}
				return false
			}
			if n > 0 {
				fl.Flush()
			}
			if n < logTailBatch {
				return true
			}
		}
	}
	if !drain() {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case <-wake:
			if !drain() {
				return
			}
		case <-tick.C:
			if !drain() {
				return
			}
		case <-keep.C:
			fmt.Fprint(w, ": keepalive\n\n")
			fl.Flush()
		}
	}
}

func writeLogEvent(w http.ResponseWriter, seq int64, payload any) {
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "id: %d\nevent: log\ndata: %s\n\n", seq, b)
}
