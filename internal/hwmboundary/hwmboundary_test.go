package hwmboundary

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

const ts = connectors.CursorDomainTimestamp

func TestRowHashIsStableAcrossEquivalentScans(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 3, 0, time.UTC)
	yerevan := time.FixedZone("AMT", 4*60*60)
	n := int64(7)
	first := RowHash([]any{int64(1), "a", []byte("b"), at, nil, &n})
	second := RowHash([]any{int64(1), "a", []byte("b"), at.In(yerevan), nil, &n})
	if first != second {
		t.Fatalf("same row hashed differently: %s vs %s", first, second)
	}
	other := int64(7)
	if got := RowHash([]any{int64(1), "a", []byte("b"), at, nil, &other}); got != first {
		t.Fatalf("pointer address leaked into hash: %s vs %s", got, first)
	}
	if len(first) != 32 {
		t.Fatalf("hash length = %d, want 32 hex chars", len(first))
	}
}

func TestRowHashFollowsPointersInsideCollections(t *testing.T) {
	// Array(Nullable(String)) and Map columns scan into collections of pointers.
	a1, a2, b1, b2 := "x", "x", int64(1), int64(1)
	first := RowHash([]any{[]*string{&a1, nil}, map[string]*int64{"k": &b1, "j": nil}})
	second := RowHash([]any{[]*string{&a2, nil}, map[string]*int64{"j": nil, "k": &b2}})
	if first != second {
		t.Fatalf("collection of pointers hashed by address: %s vs %s", first, second)
	}
	c := "y"
	if RowHash([]any{[]*string{&c, nil}}) == RowHash([]any{[]*string{&a1, nil}}) {
		t.Fatal("different array elements hashed the same")
	}
	if RowHash([]any{[]string{"ab"}}) == RowHash([]any{[]string{"a", "b"}}) {
		t.Fatal("array element boundaries are not part of the hash")
	}
}

func TestRowHashDistinguishesValues(t *testing.T) {
	base := RowHash([]any{int64(1), "a"})
	for name, row := range map[string][]any{
		"value":       {int64(2), "a"},
		"bytes_type":  {int64(1), []byte("a")},
		"null":        {int64(1), nil},
		"empty":       {int64(1), ""},
		"column_join": {int64(1), "a", ""},
		"split":       {"1a"},
	} {
		if RowHash(row) == base {
			t.Errorf("%s: hash collided with base row", name)
		}
	}
	var nilPtr *int64
	if RowHash([]any{nilPtr}) != RowHash([]any{nil}) {
		t.Error("nil pointer must hash like NULL")
	}
}

func TestTrackerDropsOnlyAlreadyExportedBoundaryRows(t *testing.T) {
	hwm := "2026-01-01T00:00:03Z"
	old := []any{int64(1), hwm}
	late := []any{int64(2), hwm}
	skip := &Skip{CursorValue: hwm, RowHashes: map[string]int64{RowHash(old): 1}}
	tr := NewTracker(ts, skip)

	if tr.Keep(hwm, true, old) {
		t.Fatal("row exported by the previous run was kept")
	}
	if !tr.Keep(hwm, true, late) {
		t.Fatal("late row with the same cursor value as the HWM was dropped")
	}

	b := tr.Boundary()
	if b.CursorValue != hwm || b.Overflow {
		t.Fatalf("boundary = %+v", b)
	}
	want := map[string]int64{RowHash(old): 1, RowHash(late): 1}
	if !sameCounts(b.RowHashes, want) {
		t.Fatalf("boundary hashes = %v, want %v (skipped and kept rows at the max)", b.RowHashes, want)
	}
}

func TestTrackerDropsDuplicateContentOnlyUpToStoredCount(t *testing.T) {
	hwm := "2026-01-01T00:00:03Z"
	row := []any{"same", hwm}
	tr := NewTracker(ts, &Skip{CursorValue: hwm, RowHashes: map[string]int64{RowHash(row): 1}})
	if tr.Keep(hwm, true, row) {
		t.Fatal("first identical row should be dropped as already exported")
	}
	if !tr.Keep(hwm, true, row) {
		t.Fatal("second identical row is new and must be kept")
	}
	if got := tr.Boundary().RowHashes[RowHash(row)]; got != 2 {
		t.Fatalf("boundary count = %d, want 2", got)
	}
}

func TestTrackerSeedDropsEveryRowAtTheHWM(t *testing.T) {
	hwm := "2026-01-01T00:00:03Z"
	tr := NewTracker(ts, &Skip{CursorValue: hwm, Seed: true})
	if tr.Keep(hwm, true, []any{int64(1), hwm}) || tr.Keep(hwm, true, []any{int64(2), hwm}) {
		t.Fatal("seed run must treat every row at the HWM as already exported")
	}
	newer := "2026-01-01T00:00:04Z"
	if !tr.Keep(newer, true, []any{int64(3), newer}) {
		t.Fatal("seed run dropped a row above the HWM")
	}
	if b := tr.Boundary(); b.CursorValue != newer || len(b.RowHashes) != 1 {
		t.Fatalf("boundary = %+v, want only the newer row", b)
	}
}

func TestTrackerSeedRecordsBoundaryWhenNothingIsNewer(t *testing.T) {
	hwm := "2026-01-01T00:00:03Z"
	tr := NewTracker(ts, &Skip{CursorValue: hwm, Seed: true})
	tr.Keep(hwm, true, []any{int64(1), hwm})
	b := tr.Boundary()
	if b.CursorValue != hwm || len(b.RowHashes) != 1 {
		t.Fatalf("boundary = %+v, want the seeded HWM row", b)
	}
}

func TestTrackerKeepsMaxOnlyAndHandlesUnorderedRows(t *testing.T) {
	tr := NewTracker(connectors.CursorDomainInt64, nil)
	for _, c := range []string{"5", "9", "3", "9", "7"} {
		if !tr.Keep(c, true, []any{c}) {
			t.Fatalf("row %s dropped without a skip set", c)
		}
	}
	if !tr.Keep("", false, []any{nil}) {
		t.Fatal("NULL-cursor row must be exported")
	}
	b := tr.Boundary()
	if b.CursorValue != "9" || b.RowHashes[RowHash([]any{"9"})] != 2 || len(b.RowHashes) != 1 {
		t.Fatalf("boundary = %+v, want two rows at 9", b)
	}
}

func TestTrackerOverflowsAndRecoversOnNewMax(t *testing.T) {
	tr := newTrackerWithLimit(connectors.CursorDomainInt64, nil, 2)
	for i := 0; i < 3; i++ {
		tr.Keep("1", true, []any{i})
	}
	if b := tr.Boundary(); !b.Overflow || b.RowHashes != nil {
		t.Fatalf("boundary = %+v, want overflow without hashes", b)
	}
	tr.Keep("2", true, []any{"x"})
	if b := tr.Boundary(); b.Overflow || b.CursorValue != "2" || len(b.RowHashes) != 1 {
		t.Fatalf("boundary = %+v, want a fresh set at the new max", b)
	}
}

func TestTrackerEmptyRunReportsEmptyBoundary(t *testing.T) {
	b := NewTracker(ts, nil).Boundary()
	if b.CursorValue != "" || b.Overflow || len(b.RowHashes) != 0 {
		t.Fatalf("boundary = %+v, want empty", b)
	}
}

func TestForStateMergesTasksAtTheNewHWM(t *testing.T) {
	low := &Boundary{CursorValue: "5", RowHashes: map[string]int64{"a": 1}}
	high1 := &Boundary{CursorValue: "9", RowHashes: map[string]int64{"b": 1}}
	high2 := &Boundary{CursorValue: "9", RowHashes: map[string]int64{"b": 1, "c": 2}}
	got := ForState(connectors.CursorDomainInt64, "9", nil, []*Boundary{low, high1, high2})
	want := map[string]int64{"b": 2, "c": 2}
	if got == nil || got.CursorValue != "9" || !sameCounts(got.RowHashes, want) {
		t.Fatalf("state boundary = %+v, want %v at 9", got, want)
	}
}

func TestForStateCarriesExistingWhenNoTaskReachedTheHWM(t *testing.T) {
	existing := &Boundary{CursorValue: "9", RowHashes: map[string]int64{"b": 1}}
	empty := &Boundary{}
	if got := ForState(connectors.CursorDomainInt64, "9", existing, []*Boundary{empty}); got != existing {
		t.Fatalf("state boundary = %+v, want the existing one", got)
	}
	if got := ForState(connectors.CursorDomainInt64, "10", existing, []*Boundary{empty}); got != nil {
		t.Fatalf("stale existing boundary reused: %+v", got)
	}
}

func TestForStateDropsBoundaryWhenIncomplete(t *testing.T) {
	high := &Boundary{CursorValue: "9", RowHashes: map[string]int64{"b": 1}}
	for name, tc := range map[string]struct {
		domain   connectors.CursorDomain
		hwm      string
		reported []*Boundary
	}{
		"untracked_task": {connectors.CursorDomainInt64, "9", []*Boundary{high, nil}},
		"unknown_domain": {connectors.CursorDomainUnknown, "9", []*Boundary{high}},
		"no_hwm":         {connectors.CursorDomainInt64, "", []*Boundary{high}},
	} {
		if got := ForState(tc.domain, tc.hwm, nil, tc.reported); got != nil {
			t.Errorf("%s: state boundary = %+v, want none", name, got)
		}
	}
}

func TestForStateMarksOverflow(t *testing.T) {
	over := &Boundary{CursorValue: "9", Overflow: true}
	ok := &Boundary{CursorValue: "9", RowHashes: map[string]int64{"b": 1}}
	got := ForState(connectors.CursorDomainInt64, "9", nil, []*Boundary{ok, over})
	if got == nil || !got.Overflow || got.RowHashes != nil {
		t.Fatalf("state boundary = %+v, want overflow", got)
	}
}

func TestPlanSkip(t *testing.T) {
	hwm := "2026-01-01T00:00:03Z"
	hashes := map[string]int64{"a": 1}
	for name, tc := range map[string]struct {
		state         *Boundary
		wantSkip      *Skip
		wantExclusive bool
	}{
		"dedupe":       {&Boundary{CursorValue: "2026-01-01T00:00:03.000Z", RowHashes: hashes}, &Skip{CursorValue: hwm, RowHashes: hashes}, false},
		"legacy_state": {nil, &Skip{CursorValue: hwm, Seed: true}, false},
		"stale_state":  {&Boundary{CursorValue: "2026-01-01T00:00:02Z", RowHashes: hashes}, &Skip{CursorValue: hwm, Seed: true}, false},
		"overflow":     {&Boundary{CursorValue: hwm, Overflow: true}, nil, true},
		"empty_hashes": {&Boundary{CursorValue: hwm}, &Skip{CursorValue: hwm, Seed: true}, false},
	} {
		skip, exclusive, reason := PlanSkip(ts, hwm, tc.state)
		if exclusive != tc.wantExclusive || reason == "" {
			t.Errorf("%s: exclusive=%v reason=%q", name, exclusive, reason)
		}
		gotJSON, _ := json.Marshal(skip)
		wantJSON, _ := json.Marshal(tc.wantSkip)
		if string(gotJSON) != string(wantJSON) {
			t.Errorf("%s: skip = %s, want %s", name, gotJSON, wantJSON)
		}
	}
}

func sameCounts(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}
