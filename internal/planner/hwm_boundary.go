package planner

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/hwmboundary"
)

// hwmResume is where an incremental run starts reading.
type hwmResume struct {
	lower          string
	lowerExclusive bool
	// skip lists rows at the mark that the first task must drop; nil when
	// the run does not re-read the mark.
	skip        *hwmboundary.Skip
	eventLevel  string
	eventReason string
}

// planHWMResume picks the run's lower bound. Without a mark the run reads
// everything. With cursor_lookback it starts at an inclusive bound below the
// mark and re-exports that window by design. Otherwise it starts at the mark
// itself (cursor >= mark) and drops the rows the previous run already
// exported, so rows committed late with the mark's cursor value are not lost.
// Cursors of unknown type keep resuming strictly after the mark.
func planHWMResume(domain connectors.CursorDomain, fromHWM, lookback string, state *hwmboundary.Boundary) (hwmResume, error) {
	fromHWM = strings.TrimSpace(fromHWM)
	if fromHWM == "" {
		return hwmResume{}, nil
	}
	if strings.TrimSpace(lookback) != "" {
		start, err := connectors.CursorLookbackStart(domain, fromHWM, lookback)
		if err != nil {
			return hwmResume{}, err
		}
		return hwmResume{lower: start, eventLevel: "INFO", eventReason: "cursor lookback applied"}, nil
	}
	if connectors.NormalizeCursorDomain(string(domain)) == connectors.CursorDomainUnknown {
		return hwmResume{lower: fromHWM, lowerExclusive: true}, nil
	}
	skip, exclusive, reason := hwmboundary.PlanSkip(domain, fromHWM, state)
	if exclusive {
		return hwmResume{lower: fromHWM, lowerExclusive: true, eventLevel: "WARN", eventReason: reason}, nil
	}
	return hwmResume{lower: fromHWM, skip: skip, eventLevel: "INFO", eventReason: reason}, nil
}

// withHWMBoundarySpec returns copies of tasks whose partition specs ask the
// worker to report its boundary. The first task, the only one whose range
// starts at the mark, also gets the skip set. Existing spec values are kept
// byte for byte.
func withHWMBoundarySpec(tasks []db.TaskInsert, skip *hwmboundary.Skip) ([]db.TaskInsert, error) {
	var skipJSON json.RawMessage
	if skip != nil {
		b, err := json.Marshal(skip)
		if err != nil {
			return nil, fmt.Errorf("hwm boundary skip: %w", err)
		}
		skipJSON = b
	}
	out := make([]db.TaskInsert, len(tasks))
	for i, task := range tasks {
		var spec map[string]json.RawMessage
		if err := json.Unmarshal(task.PartitionSpec, &spec); err != nil {
			return nil, fmt.Errorf("task %d partition spec: %w", task.TaskIndex, err)
		}
		spec[hwmboundary.SpecTrackKey] = json.RawMessage("true")
		if i == 0 && skipJSON != nil {
			spec[hwmboundary.SpecSkipKey] = skipJSON
		}
		b, err := json.Marshal(spec)
		if err != nil {
			return nil, fmt.Errorf("task %d partition spec: %w", task.TaskIndex, err)
		}
		task.PartitionSpec = b
		out[i] = task
	}
	return out, nil
}
