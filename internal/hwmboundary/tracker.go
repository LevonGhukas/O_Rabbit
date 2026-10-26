package hwmboundary

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"reflect"
	"sort"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

// rowHashBytes is the length of a row hash before hex encoding (128 bits).
const rowHashBytes = 16

// Tracker filters and observes the rows of one task. It drops rows listed in
// the task's skip set and records the rows at the largest cursor value read,
// whether kept or dropped, so the boundary describes every row at that value.
type Tracker struct {
	domain    connectors.CursorDomain
	skip      *Skip
	remaining map[string]int64
	limit     int

	max      string
	atMax    map[string]int64
	overflow bool
}

// NewTracker returns a tracker for a task; skip may be nil.
func NewTracker(domain connectors.CursorDomain, skip *Skip) *Tracker {
	return newTrackerWithLimit(domain, skip, MaxRowHashes)
}

func newTrackerWithLimit(domain connectors.CursorDomain, skip *Skip, limit int) *Tracker {
	t := &Tracker{domain: connectors.NormalizeCursorDomain(string(domain)), skip: skip, limit: limit, atMax: map[string]int64{}}
	if skip != nil && !skip.Seed {
		t.remaining = make(map[string]int64, len(skip.RowHashes))
		for h, n := range skip.RowHashes {
			t.remaining[h] = n
		}
	}
	return t
}

// Keep reports whether the row should be exported. cursor is the row's
// encoded cursor value and cursorOK is false for a NULL cursor; vals is the
// scanned row and is only read during the call.
func (t *Tracker) Keep(cursor string, cursorOK bool, vals []any) bool {
	if !cursorOK {
		return true
	}
	rowHash := ""
	hashRow := func() string {
		if rowHash == "" {
			rowHash = RowHash(vals)
		}
		return rowHash
	}
	keep := true
	if t.skip != nil && connectors.CompareCursorValues(t.domain, cursor, t.skip.CursorValue) == 0 {
		if t.skip.Seed {
			keep = false
		} else if h := hashRow(); t.remaining[h] > 0 {
			t.remaining[h]--
			keep = false
		}
	}
	t.observe(cursor, hashRow)
	return keep
}

func (t *Tracker) observe(cursor string, hashRow func() string) {
	if t.max != "" {
		switch c := connectors.CompareCursorValues(t.domain, cursor, t.max); {
		case c < 0:
			return
		case c > 0:
			t.atMax, t.overflow = map[string]int64{}, false
		}
	}
	t.max = cursor
	if t.overflow {
		return
	}
	h := hashRow()
	if _, seen := t.atMax[h]; !seen && len(t.atMax) >= t.limit {
		t.atMax, t.overflow = nil, true
		return
	}
	t.atMax[h]++
}

// Boundary returns the rows read at the task's largest cursor value.
func (t *Tracker) Boundary() Boundary {
	if t.overflow {
		return Boundary{CursorValue: t.max, Overflow: true}
	}
	var hashes map[string]int64
	if len(t.atMax) > 0 {
		hashes = make(map[string]int64, len(t.atMax))
		for h, n := range t.atMax {
			hashes[h] = n
		}
	}
	return Boundary{CursorValue: t.max, RowHashes: hashes}
}

// RowHash returns a 128-bit hex digest of a scanned row. Values are hashed
// with their type so "1" and 1 differ; pointers are followed, and timestamps
// are taken in UTC so the session time zone does not change the hash.
func RowHash(vals []any) string {
	h := sha256.New()
	for _, v := range vals {
		writeValue(h, v)
	}
	return hex.EncodeToString(h.Sum(nil)[:rowHashBytes])
}

func writeValue(h hash.Hash, v any) {
	v = deref(v)
	switch x := v.(type) {
	case nil:
		writeField(h, 'N', nil)
	case []byte:
		writeField(h, 'B', x)
	case string:
		writeField(h, 'S', []byte(x))
	case time.Time:
		writeField(h, 'T', []byte(x.UTC().Format(time.RFC3339Nano)))
	default:
		writeComposite(h, x)
	}
}

// writeComposite hashes arrays and maps element by element, so pointers
// inside them are followed instead of printed as addresses. Map entries are
// ordered by the hash of their key.
func writeComposite(h hash.Hash, v any) {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		writeField(h, 'L', []byte(fmt.Sprintf("%T:%d", v, rv.Len())))
		for i := 0; i < rv.Len(); i++ {
			writeValue(h, rv.Index(i).Interface())
		}
	case reflect.Map:
		writeField(h, 'M', []byte(fmt.Sprintf("%T:%d", v, rv.Len())))
		type entry struct{ key, val []byte }
		entries := make([]entry, 0, rv.Len())
		for it := rv.MapRange(); it.Next(); {
			entries = append(entries, entry{key: valueDigest(it.Key().Interface()), val: valueDigest(it.Value().Interface())})
		}
		sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].key, entries[j].key) < 0 })
		for _, e := range entries {
			writeField(h, 'K', e.key)
			writeField(h, 'E', e.val)
		}
	default:
		writeField(h, 'V', []byte(fmt.Sprintf("%T:%v", v, v)))
	}
}

func valueDigest(v any) []byte {
	h := sha256.New()
	writeValue(h, v)
	return h.Sum(nil)
}

func writeField(h hash.Hash, tag byte, data []byte) {
	var hdr [1 + binary.MaxVarintLen64]byte
	hdr[0] = tag
	n := binary.PutUvarint(hdr[1:], uint64(len(data)))
	_, _ = h.Write(hdr[:1+n])
	_, _ = h.Write(data)
}

func deref(v any) any {
	rv := reflect.ValueOf(v)
	for rv.IsValid() && rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return nil
	}
	return rv.Interface()
}
