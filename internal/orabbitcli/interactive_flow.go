package orabbitcli

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
)

func isRemoteMasterURL(base string) bool {
	base = normalizeHTTPBase(base)
	if strings.TrimSpace(base) == "" {
		return false
	}
	u, err := url.Parse(base)
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	switch host {
	case "", "127.0.0.1", "localhost", "::1":
		return false
	default:
		return true
	}
}

func defaultStartLocalWorkers(cfg ranConfig) bool {
	if cfg.StartStack {
		return true
	}
	return !isRemoteMasterURL(cfg.HTTPBase)
}

func buildRunReviewSummary(cfg ranConfig, availableWorkers int, workersKnown bool, advanced bool) []string {
	prefix := dataset.Prefix(cfg.S3Prefix, normalizeSourceEngine(cfg.SourceEngine), cfg.Table)
	lines := []string{
		"Source database type: " + strings.TrimSpace(cfg.SourceEngine),
		"Source table: " + summaryOrUnknown(cfg.Table),
		"Cursor / ordering column: " + summaryOrUnknown(cfg.IDColumn),
		"Incremental mode: " + yesNo(cfg.Incremental),
		"Target bucket: " + summaryOrUnknown(cfg.S3Bucket),
		"Derived target prefix: " + summaryOrUnknown(prefix),
		"Iceberg registration: " + yesNo(cfg.AutoIceberg),
	}
	if cfg.AutoIceberg {
		lines = append(lines, "Iceberg destination table: "+summaryOrUnknown(effectiveIceTable(cfg)))
		if cfg.IceOptions.URI != nil {
			lines = append(lines, "Iceberg REST catalog: "+summaryOrUnknown(*cfg.IceOptions.URI))
		}
	}
	lines = append(lines,
		"Automatic performance tuning: "+yesNo(cfg.AutoTune),
		"Local workers: "+yesNo(cfg.StartLocalWorkers),
	)
	if workersKnown {
		lines = append(lines, fmt.Sprintf("Available workers: %d", availableWorkers))
	}
	if advanced {
		lines = append(lines,
			"S3 region: "+summaryOrUnknown(cfg.S3Region),
			"S3 prefix override: "+summaryOrUnknown(strings.TrimSpace(cfg.S3Prefix)),
			"S3 force path style: "+yesNo(cfg.S3ForcePathStyle),
			"Iceberg engine: "+summaryOrUnknown(registrationEngine(cfg)),
			"Iceberg defaults file: "+summaryOrUnknown(cfg.IceConfig),
		)
		if !cfg.AutoTune {
			lines = append(lines,
				fmt.Sprintf("max_in_flight_tasks: %d", cfg.MaxInFlightTasks),
				fmt.Sprintf("planned_tasks: %d", cfg.PlannedTasks),
				fmt.Sprintf("fetch_limit_rows: %d", cfg.FetchLimit),
			)
		}
	}
	return lines
}

func summaryOrUnknown(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "(auto)"
	}
	return v
}

func yesNo(v bool) string {
	if v {
		return "yes"
	}
	return "no"
}

func flagWasProvided(name string, visited map[string]bool) bool {
	return visited[name]
}
