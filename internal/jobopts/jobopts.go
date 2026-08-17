package jobopts

import (
	"encoding/json"
	"strings"
)

// Options represents job.options_json.
//
// This struct is shared by the planner (master) and worker assignment.
// Keep JSON tags stable and preserve legacy aliases where practical.
type Options struct {
	PartitionStrategy string `json:"partition_strategy"` // "single" (default) or "ordered_cursor"; legacy alias: int_range

	// Auto-tuning (master-side). When enabled, the master may fill in missing (0)
	// values for partitioning, fetch batch size, and effective concurrency.
	AutoTune          bool  `json:"auto_tune"`
	MaxInFlightTasks  int   `json:"max_in_flight_tasks"`
	TargetRowsPerTask int64 `json:"target_rows_per_task"`

	// Optional: encourage planning more tasks than concurrency for better load balancing.
	// If 0, planner will pick a good default.
	MinTasksMultiplier int `json:"min_tasks_multiplier"`

	// Ordered-cursor SQL extraction.
	SourceMode   string `json:"source_mode,omitempty"` // "table" (default) or "query"
	SourceName   string `json:"source_name,omitempty"`
	Table        string `json:"table"`
	Query        string `json:"query,omitempty"`
	QueryHash    string `json:"query_hash,omitempty"`
	CursorColumn string `json:"cursor_column,omitempty"`
	CursorDomain string `json:"cursor_domain,omitempty"`
	PlannedTasks int    `json:"planned_tasks,omitempty"`

	// Legacy alias kept for older jobs/clients.
	IDColumn string `json:"id_column,omitempty"`

	// Legacy manual chunk hint for older int-range jobs. New planning prefers PlannedTasks.
	ChunkSize int64 `json:"chunk_size,omitempty"`

	// Downstream configurations
	TargetFileBytes int64    `json:"target_file_bytes"`
	PartitionKeys   []string `json:"partition_keys,omitempty"`

	// Consistency configuration
	ConsistencyMode string `json:"consistency_mode,omitempty"`
}

func (o Options) NormalizedSourceMode() string {
	mode := strings.ToLower(strings.TrimSpace(o.SourceMode))
	switch mode {
	case "", "table":
		return "table"
	case "query", "sql":
		return "query"
	default:
		return mode
	}
}

func (o Options) EffectiveCursorColumn() string {
	if v := strings.TrimSpace(o.CursorColumn); v != "" {
		return v
	}
	return strings.TrimSpace(o.IDColumn)
}

func (o Options) NormalizedPartitionStrategy() string {
	s := strings.ToLower(strings.TrimSpace(o.PartitionStrategy))
	switch s {
	case "", "single":
		return "single"
	case "int_range", "ordered_cursor", "cursor_range", "cursor":
		return "ordered_cursor"
	default:
		return s
	}
}

// Parse parses job options JSON.
func Parse(raw json.RawMessage) (Options, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var o Options
	if err := json.Unmarshal(raw, &o); err != nil {
		return Options{}, err
	}
	o.PartitionStrategy = o.NormalizedPartitionStrategy()
	o.SourceMode = o.NormalizedSourceMode()
	if strings.TrimSpace(o.CursorColumn) == "" {
		o.CursorColumn = strings.TrimSpace(o.IDColumn)
	}
	if strings.TrimSpace(o.IDColumn) == "" {
		o.IDColumn = strings.TrimSpace(o.CursorColumn)
	}
	if strings.TrimSpace(o.CursorColumn) == "" {
		o.CursorColumn = "ID"
		o.IDColumn = o.CursorColumn
	}

	if o.TargetRowsPerTask <= 0 {
		o.TargetRowsPerTask = 200_000
	}
	if o.MinTasksMultiplier <= 0 {
		o.MinTasksMultiplier = 2
	}
	if o.TargetFileBytes <= 0 {
		o.TargetFileBytes = 256 * 1024 * 1024
	}

	// A missing PlannedTasks deliberately remains zero.  It means "let the
	// planner choose task ranges" in both automatic and manual file-size mode;
	// it must not be confused with the worker file-size target.
	return o, nil
}

// MergeInto merges these options into an existing raw job options map.
func (o Options) MergeInto(existing map[string]any) map[string]any {
	m := existing
	if m == nil {
		m = map[string]any{}
	}
	m["partition_strategy"] = o.NormalizedPartitionStrategy()
	m["source_mode"] = o.NormalizedSourceMode()
	m["source_name"] = strings.TrimSpace(o.SourceName)
	m["auto_tune"] = o.AutoTune
	m["max_in_flight_tasks"] = o.MaxInFlightTasks
	m["target_rows_per_task"] = o.TargetRowsPerTask
	m["min_tasks_multiplier"] = o.MinTasksMultiplier
	m["table"] = o.Table
	m["query"] = strings.TrimSpace(o.Query)
	m["query_hash"] = strings.TrimSpace(o.QueryHash)
	m["cursor_column"] = o.EffectiveCursorColumn()
	m["cursor_domain"] = strings.TrimSpace(o.CursorDomain)
	m["planned_tasks"] = o.PlannedTasks
	m["id_column"] = o.EffectiveCursorColumn()
	m["chunk_size"] = o.ChunkSize
	m["target_file_bytes"] = o.TargetFileBytes
	if len(o.PartitionKeys) > 0 {
		m["partition_keys"] = o.PartitionKeys
	}
	if v := strings.TrimSpace(o.ConsistencyMode); v != "" {
		m["consistency_mode"] = strings.ToUpper(v)
	}
	return m
}
