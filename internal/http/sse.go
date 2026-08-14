// internal/http/sse.go
// This file implements a simple Server-Sent Events (SSE) broadcaster and handler for streaming real-time events to clients.
// The Broadcaster allows multiple clients to subscribe to events for a specific run ID, and the SSEHandler serves these events over HTTP.
// The SSEHandler also performs a best-effort replay of past events for a run when a new client connects, allowing the client to render the initial state.

package httpapi

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

// Broadcaster manages subscriptions and publishing of events to multiple clients based on run IDs.
type Broadcaster struct {
	log *slog.Logger

	mu   sync.Mutex
	subs map[string]map[chan db.Event]struct{}
}

// NewBroadcaster creates a new Broadcaster instance with an optional logger.
// If no logger is provided, it uses the default logger.
func NewBroadcaster(log *slog.Logger) *Broadcaster {
	if log == nil {
		log = slog.Default()
	}
	return &Broadcaster{log: log, subs: make(map[string]map[chan db.Event]struct{})}
}

// Publish sends an event to all subscribed channels for the event's run ID.
// It locks the broadcaster to safely access the subscribers map, collects the channels for the run ID,
// and then sends the event to each channel without blocking (dropping if a consumer is slow).
func (b *Broadcaster) Publish(e db.Event) {
	b.mu.Lock()
	m := b.subs[e.RunID]
	chs := make([]chan db.Event, 0, len(m))
	for ch := range m {
		chs = append(chs, ch)
	}
	b.mu.Unlock()

	for _, ch := range chs {
		select {
		case ch <- e:
		default:
			// Drop if slow consumer.
		}
	}
}

// Subscribe adds a new subscriber channel for a specific run ID and returns the channel along with an unsubscribe function.
// The subscriber will receive events published for that run ID until they unsubscribe.
func (b *Broadcaster) Subscribe(runID string) (chan db.Event, func()) {
	ch := make(chan db.Event, 128)
	b.mu.Lock()
	if b.subs[runID] == nil {
		b.subs[runID] = make(map[chan db.Event]struct{})
	}
	b.subs[runID][ch] = struct{}{}
	b.mu.Unlock()

	unsubscribe := func() {
		b.mu.Lock()
		if b.subs[runID] != nil {
			delete(b.subs[runID], ch)
			if len(b.subs[runID]) == 0 {
				delete(b.subs, runID)
			}
		}
		b.mu.Unlock()
		// Do not close subscriber channels here. Publish may have already
		// snapshotted this channel and a concurrent send to a closed channel panics.
	}
	return ch, unsubscribe
}

// SSEHandler returns an HTTP handler function that serves Server-Sent Events (SSE) for a specific run ID.
// It checks for the run_id query parameter, sets the appropriate headers for SSE, and subscribes to events for that run ID using the Broadcaster.
// It also performs a best-effort replay of past events for the run from the database store when a new client connects.
// The handler keeps the connection alive by sending periodic keep-alive comments if no events are published.
func SSEHandler(log *slog.Logger, store *db.Store, b *Broadcaster) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		runID := r.URL.Query().Get("run_id")
		if runID == "" {
			writeInvalidInput(w, "missing run_id", map[string]any{"field": "run_id"})
			return
		}
		if b == nil {
			writeDependencyUnavailable(w, "event stream unavailable")
			return
		}
		if store != nil {
			if _, err := store.GetRun(r.Context(), runID); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					writeNotFound(w, "run", nil)
					return
				}
				writeInternalError(w, "failed to fetch run")
				return
			}
		}

		fl, ok := w.(http.Flusher)
		if !ok {
			writeInternalError(w, "streaming unavailable")
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ctx := r.Context()

		ch, unsub := b.Subscribe(runID)
		defer unsub()

		writeEvent := func(e db.Event) {
			payload, _ := json.Marshal(e)
			fmt.Fprintf(w, "event: event\n")
			fmt.Fprintf(w, "data: %s\n\n", payload)
			fl.Flush()
		}

		// Best-effort replay so a UI can render initial state.
		if store != nil {
			evs, err := store.ListEventsForRun(ctx, runID, 500)
			if err == nil {
				for _, e := range evs {
					writeEvent(e)
				}
			}
		}

		keep := time.NewTicker(15 * time.Second)
		defer keep.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case e := <-ch:
				writeEvent(e)
			case <-keep.C:
				fmt.Fprintf(w, ": keepalive\n\n")
				fl.Flush()
			}
		}
	}
}
