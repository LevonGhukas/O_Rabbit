// Package logbuffer keeps the most recent master log records in memory so the
// web console can show them, without changing what goes to stdout.
package logbuffer

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/redact"
)

// DefaultCapacity bounds memory: about 10k recent records.
const DefaultCapacity = 10000

// Entry is one buffered log record. Message and Attrs are already masked.
type Entry struct {
	Seq     int64          `json:"seq"`
	Time    time.Time      `json:"ts"`
	Level   string         `json:"level"`
	Message string         `json:"message"`
	Attrs   map[string]any `json:"attrs,omitempty"`
}

// Filter selects buffered entries; empty fields do not filter.
type Filter struct {
	Levels []string // DEBUG, INFO, WARN, ERROR (case-insensitive; WARNING accepted)
	Query  string   // case-insensitive substring of the message or attribute values
	Since  time.Time
	Until  time.Time
	Before int64 // entries with seq < Before (older page)
	After  int64 // entries with seq > After (live tail, oldest first)
	Limit  int
}

// Buffer is a fixed-size ring of entries with non-blocking subscribers.
type Buffer struct {
	mu      sync.RWMutex
	entries []Entry
	next    int // write position in entries
	full    bool
	seq     int64
	subs    map[chan Entry]struct{}
}

func New(capacity int) *Buffer {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &Buffer{entries: make([]Entry, capacity), subs: make(map[chan Entry]struct{})}
}

func (b *Buffer) add(e Entry) {
	b.mu.Lock()
	b.seq++
	e.Seq = b.seq
	b.entries[b.next] = e
	b.next = (b.next + 1) % len(b.entries)
	if b.next == 0 {
		b.full = true
	}
	subs := make([]chan Entry, 0, len(b.subs))
	for ch := range b.subs {
		subs = append(subs, ch)
	}
	b.mu.Unlock()

	// Never block logging on a slow viewer: a full subscriber misses the entry
	// and can catch up from the buffer with After.
	for _, ch := range subs {
		select {
		case ch <- e:
		default:
		}
	}
}

// Subscribe returns new entries as they are logged. Call cancel when done.
func (b *Buffer) Subscribe() (<-chan Entry, func()) {
	ch := make(chan Entry, 256)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
	}
}

// LatestSeq is the sequence number of the newest entry (0 when empty).
func (b *Buffer) LatestSeq() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.seq
}

// Query returns matching entries newest first, or oldest first when After is set.
// total is the number of matching entries in the whole buffer (ignoring paging).
func (b *Buffer) Query(f Filter) (items []Entry, total int) {
	limit := f.Limit
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	levels := map[string]bool{}
	for _, l := range f.Levels {
		levels[canonicalLevel(l)] = true
	}
	q := strings.ToLower(strings.TrimSpace(f.Query))

	b.mu.RLock()
	defer b.mu.RUnlock()
	n := b.next
	if b.full {
		n = len(b.entries)
	}
	// Walk newest to oldest.
	for i := 0; i < n; i++ {
		idx := (b.next - 1 - i + len(b.entries)) % len(b.entries)
		e := b.entries[idx]
		if len(levels) > 0 && !levels[e.Level] {
			continue
		}
		if !f.Since.IsZero() && e.Time.Before(f.Since) || !f.Until.IsZero() && e.Time.After(f.Until) {
			continue
		}
		if q != "" && !matches(e, q) {
			continue
		}
		total++
		if f.Before > 0 && e.Seq >= f.Before || f.After > 0 && e.Seq <= f.After {
			continue
		}
		items = append(items, e)
	}
	if f.After > 0 {
		// Tail: oldest new entries first, then cut to the page size.
		for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
			items[i], items[j] = items[j], items[i]
		}
	}
	if len(items) > limit {
		items = items[:limit]
	}
	return items, total
}

func matches(e Entry, q string) bool {
	if strings.Contains(strings.ToLower(e.Message), q) {
		return true
	}
	for k, v := range e.Attrs {
		if strings.Contains(strings.ToLower(k), q) {
			return true
		}
		if s, ok := v.(string); ok && strings.Contains(strings.ToLower(s), q) {
			return true
		}
	}
	return false
}

func canonicalLevel(l string) string {
	switch up := strings.ToUpper(strings.TrimSpace(l)); up {
	case "WARNING":
		return "WARN"
	default:
		return up
	}
}

// Handler tees records into a Buffer and passes them unchanged to next.
type Handler struct {
	next   slog.Handler
	buf    *Buffer
	attrs  []slog.Attr
	groups []string
}

// NewHandler wraps next. Records below next's level are neither printed nor buffered.
func NewHandler(next slog.Handler, buf *Buffer) *Handler {
	return &Handler{next: next, buf: buf}
}

func (h *Handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *Handler) Handle(ctx context.Context, r slog.Record) error {
	err := h.next.Handle(ctx, r)

	attrs := make(map[string]any, len(h.attrs)+r.NumAttrs())
	// h.attrs already carry their full keys (see WithAttrs); only record attrs
	// belong to the groups opened so far.
	for _, a := range h.attrs {
		addAttr(attrs, "", a)
	}
	prefix := strings.Join(h.groups, ".")
	r.Attrs(func(a slog.Attr) bool {
		addAttr(attrs, prefix, a)
		return true
	})
	masked, _ := redact.Value(attrs).(map[string]any)
	h.buf.add(Entry{Time: r.Time, Level: canonicalLevel(r.Level.String()), Message: redact.Text(r.Message), Attrs: masked})
	return err
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	// Attributes added under a group belong to that group.
	prefixed := make([]slog.Attr, 0, len(attrs))
	for _, a := range attrs {
		if len(h.groups) > 0 {
			a.Key = strings.Join(h.groups, ".") + "." + a.Key
		}
		prefixed = append(prefixed, a)
	}
	return &Handler{next: h.next.WithAttrs(attrs), buf: h.buf, attrs: append(append([]slog.Attr{}, h.attrs...), prefixed...), groups: h.groups}
}

func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &Handler{next: h.next.WithGroup(name), buf: h.buf, attrs: h.attrs, groups: append(append([]string{}, h.groups...), name)}
}

// addAttr flattens groups into dotted keys and resolves lazy values.
func addAttr(out map[string]any, prefix string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	key := a.Key
	if prefix != "" && key != "" {
		key = prefix + "." + key
	}
	if a.Value.Kind() == slog.KindGroup {
		for _, g := range a.Value.Group() {
			addAttr(out, key, g)
		}
		return
	}
	switch a.Value.Kind() {
	case slog.KindString:
		out[key] = a.Value.String()
	case slog.KindInt64:
		out[key] = a.Value.Int64()
	case slog.KindUint64:
		out[key] = a.Value.Uint64()
	case slog.KindFloat64:
		out[key] = a.Value.Float64()
	case slog.KindBool:
		out[key] = a.Value.Bool()
	case slog.KindDuration:
		out[key] = a.Value.Duration().String()
	case slog.KindTime:
		out[key] = a.Value.Time().UTC().Format(time.RFC3339Nano)
	default:
		out[key] = a.Value.String()
	}
}
