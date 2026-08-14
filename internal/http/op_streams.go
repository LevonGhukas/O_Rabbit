package httpapi

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type StreamEnvelope struct {
	Type      string `json:"type"`
	StreamID  string `json:"stream_id"`
	Sequence  int64  `json:"sequence"`
	Timestamp string `json:"timestamp"`
	Level     string `json:"level"`
	ServerID  string `json:"server_id,omitempty"`
	Data      any    `json:"data,omitempty"`
}

type StreamHub struct {
	log *slog.Logger

	mu       sync.Mutex
	subs     map[string]map[chan StreamEnvelope]struct{}
	sequence map[string]int64
}

func NewStreamHub(log *slog.Logger) *StreamHub {
	if log == nil {
		log = slog.Default()
	}
	return &StreamHub{
		log:      log,
		subs:     make(map[string]map[chan StreamEnvelope]struct{}),
		sequence: make(map[string]int64),
	}
}

func (h *StreamHub) Publish(streamID string, eventType string, level string, serverID string, data any) StreamEnvelope {
	h.mu.Lock()
	h.sequence[streamID]++
	event := StreamEnvelope{
		Type:      eventType,
		StreamID:  streamID,
		Sequence:  h.sequence[streamID],
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Level:     level,
		ServerID:  serverID,
		Data:      data,
	}
	subs := h.subs[streamID]
	chs := make([]chan StreamEnvelope, 0, len(subs))
	for ch := range subs {
		chs = append(chs, ch)
	}
	h.mu.Unlock()

	for _, ch := range chs {
		select {
		case ch <- event:
		default:
		}
	}
	return event
}

func (h *StreamHub) Subscribe(streamID string) (chan StreamEnvelope, func()) {
	ch := make(chan StreamEnvelope, 128)
	h.mu.Lock()
	if h.subs[streamID] == nil {
		h.subs[streamID] = make(map[chan StreamEnvelope]struct{})
	}
	h.subs[streamID][ch] = struct{}{}
	h.mu.Unlock()

	return ch, func() {
		h.mu.Lock()
		if h.subs[streamID] != nil {
			delete(h.subs[streamID], ch)
			if len(h.subs[streamID]) == 0 {
				delete(h.subs, streamID)
			}
		}
		h.mu.Unlock()
	}
}

func writeSSEEnvelope(w http.ResponseWriter, fl http.Flusher, eventType string, payload any) {
	b, _ := json.Marshal(payload)
	fmt.Fprintf(w, "event: %s\n", eventType)
	fmt.Fprintf(w, "data: %s\n\n", b)
	fl.Flush()
}
