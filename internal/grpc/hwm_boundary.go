package grpcapi

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/hwmboundary"
)

// encodeReportedHWMBoundary validates a worker's reported boundary and returns
// the JSON stored with the task, or nil when the task did not track one.
// Map keys are marshaled sorted, so a retried identical result has an
// identical digest.
func encodeReportedHWMBoundary(b *grpcpb.HWMBoundary) ([]byte, error) {
	if b == nil {
		return nil, nil
	}
	if len(b.RowHashes) > hwmboundary.MaxRowHashes {
		return nil, fmt.Errorf("hwm_boundary lists %d rows, above the limit of %d", len(b.RowHashes), hwmboundary.MaxRowHashes)
	}
	if len(b.RowHashes) > 0 && (b.Overflow || strings.TrimSpace(b.CursorValue) == "") {
		return nil, fmt.Errorf("hwm_boundary row hashes require a cursor value and no overflow")
	}
	for h, n := range b.RowHashes {
		if strings.TrimSpace(h) == "" || n <= 0 {
			return nil, fmt.Errorf("hwm_boundary row hash %q has invalid count %d", h, n)
		}
	}
	return json.Marshal(hwmboundary.Boundary{CursorValue: b.CursorValue, RowHashes: b.RowHashes, Overflow: b.Overflow})
}

// reportedHWMBoundaries decodes each task's reported boundary; an untracked
// or unreadable one is nil, which keeps the commit from storing a boundary.
func reportedHWMBoundaries(log *slog.Logger, tasks []db.Task) []*hwmboundary.Boundary {
	out := make([]*hwmboundary.Boundary, len(tasks))
	for i, t := range tasks {
		if len(t.HWMBoundary) == 0 {
			continue
		}
		var b hwmboundary.Boundary
		if err := json.Unmarshal(t.HWMBoundary, &b); err != nil {
			if log != nil {
				log.Warn("ignoring unreadable task hwm boundary", slog.String("task_id", t.ID), slog.String("err", err.Error()))
			}
			continue
		}
		out[i] = &b
	}
	return out
}

// parseExistingHWMBoundary returns the boundary stored in _state.json, if any.
func parseExistingHWMBoundary(stateBytes []byte) *hwmboundary.Boundary {
	if len(stateBytes) == 0 {
		return nil
	}
	var s map[string]json.RawMessage
	if err := json.Unmarshal(stateBytes, &s); err != nil || len(s[hwmboundary.StateKey]) == 0 {
		return nil
	}
	var b hwmboundary.Boundary
	if err := json.Unmarshal(s[hwmboundary.StateKey], &b); err != nil || strings.TrimSpace(b.CursorValue) == "" {
		return nil
	}
	return &b
}
