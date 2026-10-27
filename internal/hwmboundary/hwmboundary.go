// Package hwmboundary keeps incremental runs from losing or duplicating rows
// that share the cursor value of the dataset's high-water mark (HWM).
//
// A run reads rows with cursor >= HWM instead of > HWM, so rows committed late
// with the same cursor value as the HWM are picked up. To avoid exporting the
// rows already written by an earlier run again, the commit stores a hash of
// every row sitting exactly at the HWM in the dataset state, and the next run
// drops boundary rows whose hash is in that set.
package hwmboundary

import (
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

const (
	// StateKey is the _state.json key holding the boundary of max_hwm_value.
	StateKey = "hwm_boundary"
	// SpecSkipKey is the partition spec key with the rows a task must drop.
	SpecSkipKey = "hwm_boundary_skip"
	// SpecTrackKey is the partition spec key asking a task to report its boundary.
	SpecTrackKey = "track_hwm_boundary"
	// MaxRowHashes bounds the distinct rows kept for one boundary value, so the
	// state, partition spec and task result stay small. Above it the boundary
	// overflows and the next run falls back to resuming strictly after the HWM.
	MaxRowHashes = 10_000
)

// Boundary lists the rows a task (or a committed dataset) holds at its largest
// cursor value, as row hash -> number of rows with that content.
type Boundary struct {
	CursorValue string           `json:"cursor_value"`
	RowHashes   map[string]int64 `json:"row_hashes,omitempty"`
	Overflow    bool             `json:"overflow,omitempty"`
}

// Skip tells the task that reads the HWM which rows at CursorValue were
// already exported. Seed marks every row at CursorValue as exported; it is
// used when the dataset state predates boundary tracking.
type Skip struct {
	CursorValue string           `json:"cursor_value"`
	RowHashes   map[string]int64 `json:"row_hashes,omitempty"`
	Seed        bool             `json:"seed,omitempty"`
}

// PlanSkip decides how a run resumes from hwm given the stored boundary. It
// returns the skip set for an inclusive lower bound, or exclusive=true when
// the run must resume strictly after hwm, plus a reason for the plan event.
func PlanSkip(domain connectors.CursorDomain, hwm string, state *Boundary) (skip *Skip, exclusive bool, reason string) {
	switch {
	case state == nil || strings.TrimSpace(state.CursorValue) == "":
		return &Skip{CursorValue: hwm, Seed: true}, false, "dataset state has no HWM boundary; seeding it from rows at the HWM"
	case connectors.CompareCursorValues(domain, state.CursorValue, hwm) != 0:
		return &Skip{CursorValue: hwm, Seed: true}, false, "stored HWM boundary belongs to another cursor value; seeding it from rows at the HWM"
	case state.Overflow:
		return nil, true, "more rows share the HWM value than the boundary can hold; resuming strictly after the HWM, rows committed later with the same cursor value may be missed"
	case len(state.RowHashes) == 0:
		return &Skip{CursorValue: hwm, Seed: true}, false, "stored HWM boundary is empty; seeding it from rows at the HWM"
	default:
		return &Skip{CursorValue: hwm, RowHashes: state.RowHashes}, false, "resuming at the HWM and dropping rows already exported"
	}
}

// ForState returns the boundary to store with the committed high-water mark
// hwm. reported holds one entry per task of the run; nil means the task did
// not track its boundary. existing is the boundary already stored, kept when
// no task read rows at hwm. It returns nil when the boundary is unknown, and
// the next run then seeds it.
func ForState(domain connectors.CursorDomain, hwm string, existing *Boundary, reported []*Boundary) *Boundary {
	if strings.TrimSpace(hwm) == "" || connectors.NormalizeCursorDomain(string(domain)) == connectors.CursorDomainUnknown {
		return nil
	}
	merged := map[string]int64{}
	contributed, overflow := false, false
	for _, b := range reported {
		if b == nil {
			return nil
		}
		if strings.TrimSpace(b.CursorValue) == "" || connectors.CompareCursorValues(domain, b.CursorValue, hwm) != 0 {
			continue
		}
		contributed = true
		overflow = overflow || b.Overflow
		for h, n := range b.RowHashes {
			merged[h] += n
		}
	}
	if !contributed {
		if existing != nil && connectors.CompareCursorValues(domain, existing.CursorValue, hwm) == 0 {
			return existing
		}
		return nil
	}
	if overflow || len(merged) > MaxRowHashes {
		return &Boundary{CursorValue: hwm, Overflow: true}
	}
	return &Boundary{CursorValue: hwm, RowHashes: merged}
}
