package orabbitcli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

type taskBenchPayload struct {
	DBConnectMS     int64  `json:"db_connect_ms"`
	QueryMS         int64  `json:"query_ms"`
	WorkerCount     int    `json:"worker_count"`
	S3InitMS        int64  `json:"s3_init_ms"`
	ConvertMS       int64  `json:"convert_ms"`
	ParquetCloseMS  int64  `json:"parquet_close_ms"`
	ParquetMetaMS   int64  `json:"parquet_meta_ms"`
	UploadMS        int64  `json:"minio_upload_ms"`
	TaskTotalMS     int64  `json:"task_total_ms"`
	Rows            int64  `json:"rows"`
	ParquetBytes    int64  `json:"parquet_bytes"`
	ParquetFiles    int64  `json:"parquet_files"`
	TargetFileBytes int64  `json:"target_file_bytes"`
	MinFileBytes    int64  `json:"min_file_bytes"`
	MaxFileBytes    int64  `json:"max_file_bytes"`
	AvgFileBytes    int64  `json:"avg_file_bytes"`
	UploadSkipped   bool   `json:"upload_skipped"`
	CursorDomain    string `json:"cursor_domain"`
	PartitionLower  string `json:"partition_lower"`
	PartitionUpper  string `json:"partition_upper"`
}

type taskBenchEvent struct {
	Bench *taskBenchPayload `json:"bench"`
}

type registrationBench struct {
	Seen       bool
	Status     string
	DurationMS int64
	Objects    int
	Error      string
}

type benchAgg struct {
	ByTask       map[string]taskBenchPayload
	CommitMS     int64
	CommitTS     string
	Registration registrationBench
}

type benchReportConfig struct {
	WorkersAvailable   int
	MaxInFlightTasks   int
	AutoTune           bool
	PlannedTasks       int
	FetchLimit         int
	PlanningMS         int64
	Source             string
	Target             string
	RegistrationEnable bool
	RegistrationEngine string
}

type workerBench struct {
	ID    string
	Tasks int
	Rows  int64
	Bytes int64
}

// newBenchAgg handles new bench agg behavior.
// It exists to keep this logic isolated and reusable.
func newBenchAgg() *benchAgg {
	return &benchAgg{ByTask: map[string]taskBenchPayload{}}
}

// streamSSE streams sse from a long-lived source.
// It exists to provide continuous run state without polling loops.
func streamSSE(ctx context.Context, url string, out *cliOutput, waitForRegistration bool) (string, *benchAgg, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if tok := strings.TrimSpace(os.Getenv("ORABBIT_HTTP_AUTH_TOKEN")); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return "", nil, fmt.Errorf("sse http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	bench := newBenchAgg()
	registrationDone := !waitForRegistration

	var (
		runStatus string
		committed bool
	)

	s := bufio.NewScanner(resp.Body)
	s.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for s.Scan() {
		line := s.Text()
		if strings.HasPrefix(line, "data: ") {
			payload := strings.TrimPrefix(line, "data: ")
			var ev struct {
				Message string          `json:"message"`
				Level   string          `json:"level"`
				RunID   string          `json:"run_id"`
				TaskID  *string         `json:"task_id"`
				TS      string          `json:"ts"`
				Fields  json.RawMessage `json:"fields_json"`
			}
			_ = json.Unmarshal([]byte(payload), &ev)
			msg := strings.TrimSpace(ev.Message)
			if msg != "" {
				if out != nil {
					out.eventln(msg)
				} else {
					fmt.Println(msg)
				}
			}

			if ev.TaskID != nil && strings.TrimSpace(*ev.TaskID) != "" {
				var bf taskBenchEvent
				if err := json.Unmarshal(ev.Fields, &bf); err == nil && bf.Bench != nil {
					bench.ByTask[*ev.TaskID] = *bf.Bench
				}
			}

			switch {
			case strings.EqualFold(msg, "run committed"):
				var fields struct {
					CommitMS int64 `json:"commit_ms"`
				}
				_ = json.Unmarshal(ev.Fields, &fields)
				bench.CommitMS = fields.CommitMS
				bench.CommitTS = strings.TrimSpace(ev.TS)
				committed = true
				if strings.EqualFold(runStatus, "SUCCEEDED") && registrationDone {
					return "SUCCEEDED", bench, nil
				}
				continue

			case strings.EqualFold(msg, "iceberg registration SUCCEEDED"):
				var fields struct {
					DurationMS int64 `json:"duration_ms"`
					Objects    int   `json:"objects"`
				}
				_ = json.Unmarshal(ev.Fields, &fields)
				bench.Registration = registrationBench{
					Seen:       true,
					Status:     "SUCCEEDED",
					DurationMS: fields.DurationMS,
					Objects:    fields.Objects,
				}
				registrationDone = true
				if strings.EqualFold(runStatus, "SUCCEEDED") && committed {
					return "SUCCEEDED", bench, nil
				}
				continue

			case strings.EqualFold(msg, "iceberg registration FAILED"):
				var fields struct {
					DurationMS int64  `json:"duration_ms"`
					Objects    int    `json:"objects"`
					Error      string `json:"error"`
				}
				_ = json.Unmarshal(ev.Fields, &fields)
				bench.Registration = registrationBench{
					Seen:       true,
					Status:     "FAILED",
					DurationMS: fields.DurationMS,
					Objects:    fields.Objects,
					Error:      strings.TrimSpace(fields.Error),
				}
				registrationDone = true
				if strings.EqualFold(runStatus, "SUCCEEDED") && committed {
					return "SUCCEEDED", bench, nil
				}
				continue
			}

			if strings.HasPrefix(strings.ToLower(msg), "run ") {
				_, st, ok := strings.Cut(msg, " ")
				if ok {
					st = strings.TrimSpace(st)
					if strings.EqualFold(st, "SUCCEEDED") {
						runStatus = "SUCCEEDED"
						if committed && registrationDone {
							return "SUCCEEDED", bench, nil
						}
						continue
					}
					if strings.EqualFold(st, "FAILED") || strings.EqualFold(st, "CANCELED") {
						return strings.ToUpper(st), bench, nil
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return "", bench, ctx.Err()
		default:
		}
	}
	if err := s.Err(); err != nil {
		return "", bench, err
	}
	if err := ctx.Err(); err != nil {
		return "", bench, err
	}
	return "", bench, nil
}

// medianInt64 handles median int 64 behavior.
// It exists to keep this logic isolated and reusable.
func medianInt64(xs []int64) int64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]int64(nil), xs...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	mid := len(s) / 2
	if len(s)%2 == 0 {
		return (s[mid-1] + s[mid]) / 2
	}
	return s[mid]
}

// modeInt handles mode int behavior.
// It exists to keep this logic isolated and reusable.
func modeInt(xs []int) int {
	if len(xs) == 0 {
		return 0
	}
	counts := map[int]int{}
	bestV := xs[0]
	bestC := 0
	for _, v := range xs {
		counts[v]++
		c := counts[v]
		if c > bestC || (c == bestC && v > bestV) {
			bestC = c
			bestV = v
		}
	}
	return bestV
}

// sumInt64 handles sum int 64 behavior.
// It exists to keep this logic isolated and reusable.
func sumInt64(xs []int64) int64 {
	var s int64
	for _, v := range xs {
		s += v
	}
	return s
}

func parseTime(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

func countParquetFiles(tasks []taskInfo) int {
	seen := map[string]struct{}{}
	for _, task := range tasks {
		if len(task.ParquetObjects) == 0 {
			continue
		}
		var objs []map[string]any
		if err := json.Unmarshal(task.ParquetObjects, &objs); err != nil {
			continue
		}
		for _, obj := range objs {
			key, _ := obj["key"].(string)
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			seen[key] = struct{}{}
		}
	}
	return len(seen)
}

func workerUtilization(tasks []taskInfo) []workerBench {
	byWorker := map[string]*workerBench{}
	for _, task := range tasks {
		if task.WorkerID == nil {
			continue
		}
		workerID := strings.TrimSpace(*task.WorkerID)
		if workerID == "" {
			continue
		}
		entry := byWorker[workerID]
		if entry == nil {
			entry = &workerBench{ID: workerID}
			byWorker[workerID] = entry
		}
		entry.Tasks++
		entry.Rows += task.RowsRead
		entry.Bytes += task.BytesWritten
	}

	out := make([]workerBench, 0, len(byWorker))
	for _, entry := range byWorker {
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func taskDurationMS(task taskInfo, bench *benchAgg) (int64, bool) {
	if task.StartedAt != nil && task.FinishedAt != nil {
		start, okStart := parseTime(*task.StartedAt)
		finish, okFinish := parseTime(*task.FinishedAt)
		if okStart && okFinish && !finish.Before(start) {
			return finish.Sub(start).Milliseconds(), true
		}
	}
	// Fall back to the task-local benchmark payload when persisted timestamps are absent
	// or malformed. This keeps task stats available for older runs without fabricating data.
	if bench != nil {
		if tb, ok := bench.ByTask[task.ID]; ok && tb.TaskTotalMS > 0 {
			return tb.TaskTotalMS, true
		}
	}
	return 0, false
}

// exportWallMS measures the export stage from the earliest task start until the
// commit event when present. If commit timing is unavailable, it falls back to
// the latest finished task, then finally to the longest observed task duration.
func exportWallMS(tasks []taskInfo, bench *benchAgg, taskDurations []int64) int64 {
	var (
		earliestStart time.Time
		latestFinish  time.Time
		haveStart     bool
		haveFinish    bool
	)
	for _, task := range tasks {
		if task.StartedAt != nil {
			if ts, ok := parseTime(*task.StartedAt); ok {
				if !haveStart || ts.Before(earliestStart) {
					earliestStart = ts
					haveStart = true
				}
			}
		}
		if task.FinishedAt != nil {
			if ts, ok := parseTime(*task.FinishedAt); ok {
				if !haveFinish || ts.After(latestFinish) {
					latestFinish = ts
					haveFinish = true
				}
			}
		}
	}

	if haveStart && bench != nil {
		if commitTS, ok := parseTime(bench.CommitTS); ok && !commitTS.Before(earliestStart) {
			return commitTS.Sub(earliestStart).Milliseconds()
		}
	}
	if haveStart && haveFinish && !latestFinish.Before(earliestStart) {
		return latestFinish.Sub(earliestStart).Milliseconds()
	}
	if len(taskDurations) != 0 {
		var max int64
		for _, duration := range taskDurations {
			if duration > max {
				max = duration
			}
		}
		return max
	}
	return 0
}

func formatRate(v float64) string {
	return fmt.Sprintf("%.2f", v)
}

func registrationStatus(cfg benchReportConfig, runStatus string, bench *benchAgg) string {
	if !cfg.RegistrationEnable {
		return "DISABLED"
	}
	if bench != nil && bench.Registration.Seen {
		return bench.Registration.Status
	}
	switch strings.ToUpper(strings.TrimSpace(runStatus)) {
	case "FAILED", "CANCELED":
		return "SKIPPED"
	default:
		return "UNKNOWN"
	}
}

// renderBenchReport builds the final benchmark summary block.
// It exists to keep formatting deterministic and reusable for tests.
func renderBenchReport(cfg benchReportConfig, runID, runStatus string, details runDetails, bench *benchAgg, overallWallMS int64) string {
	if strings.TrimSpace(runID) == "" {
		return ""
	}

	taskCount := len(details.Tasks)
	if taskCount == 0 {
		return ""
	}

	var (
		dbConn        []int64
		query         []int64
		convert       []int64
		parquetMeta   []int64
		upload        []int64
		spans         []int64
		fetchLimits   []int
		rows          int64
		bytes         int64
		taskDurations []int64
		minTaskMS     int64
		maxTaskMS     int64
	)
	if bench != nil {
		for _, tb := range bench.ByTask {
			dbConn = append(dbConn, tb.DBConnectMS)
			query = append(query, tb.QueryMS)
			convert = append(convert, tb.ConvertMS)
			parquetMeta = append(parquetMeta, tb.ParquetMetaMS)
			upload = append(upload, tb.UploadMS)
			if strings.TrimSpace(tb.PartitionLower) != "" {
				if span, ok := connectors.CursorSpanUnits(connectors.NormalizeCursorDomain(tb.CursorDomain), tb.PartitionLower, tb.PartitionUpper); ok && span > 0 {
					spans = append(spans, span)
				}
			}
		}
	}

	for _, task := range details.Tasks {
		rows += task.RowsRead
		bytes += task.BytesWritten
		if durationMS, ok := taskDurationMS(task, bench); ok {
			taskDurations = append(taskDurations, durationMS)
			if minTaskMS == 0 || durationMS < minTaskMS {
				minTaskMS = durationMS
			}
			if durationMS > maxTaskMS {
				maxTaskMS = durationMS
			}
		}
	}

	if len(fetchLimits) == 0 && cfg.FetchLimit > 0 {
		fetchLimits = append(fetchLimits, cfg.FetchLimit)
	}

	parquetFiles := countParquetFiles(details.Tasks)
	avgRowsPerFile := int64(0)
	avgBytesPerFile := int64(0)
	if parquetFiles > 0 {
		avgRowsPerFile = rows / int64(parquetFiles)
		avgBytesPerFile = bytes / int64(parquetFiles)
	}

	exportWall := exportWallMS(details.Tasks, bench, taskDurations)
	rowsPerSec := 0.0
	mibPerSec := 0.0
	filesPerSec := 0.0
	if exportWall > 0 {
		secs := float64(exportWall) / 1000.0
		rowsPerSec = float64(rows) / secs
		mibPerSec = (float64(bytes) / (1024.0 * 1024.0)) / secs
		filesPerSec = float64(parquetFiles) / secs
	}

	workerStats := workerUtilization(details.Tasks)
	regStatus := registrationStatus(cfg, runStatus, bench)
	regDurationMS := int64(0)
	regObjects := 0
	regError := ""
	if bench != nil && bench.Registration.Seen {
		regDurationMS = bench.Registration.DurationMS
		regObjects = bench.Registration.Objects
		regError = bench.Registration.Error
	}

	plannedTasks := "auto"
	if !cfg.AutoTune && cfg.PlannedTasks > 0 {
		plannedTasks = fmt.Sprintf("%d", cfg.PlannedTasks)
	}

	var buf strings.Builder
	buf.WriteString("Run summary\n")
	fmt.Fprintf(&buf, "  run_id=%s\n", runID)
	fmt.Fprintf(&buf, "  status=%s\n", strings.ToUpper(strings.TrimSpace(runStatus)))
	fmt.Fprintf(&buf, "  registration=%s\n", regStatus)
	fmt.Fprintf(&buf, "  overall_wall_ms=%d\n", overallWallMS)
	fmt.Fprintf(&buf, "  source=%s\n", cfg.Source)
	fmt.Fprintf(&buf, "  target=%s\n", cfg.Target)

	buf.WriteString("\nPlanning\n")
	fmt.Fprintf(&buf, "  workers_available=%d\n", cfg.WorkersAvailable)
	fmt.Fprintf(&buf, "  workers_used=%d\n", len(workerStats))
	fmt.Fprintf(&buf, "  tasks=%d\n", taskCount)
	fmt.Fprintf(&buf, "  max_in_flight=%d\n", cfg.MaxInFlightTasks)
	fmt.Fprintf(&buf, "  planned_tasks=%s\n", plannedTasks)
	fmt.Fprintf(&buf, "  effective_partition_span=%d\n", medianInt64(spans))
	fmt.Fprintf(&buf, "  fetch_limit_rows=%d\n", modeInt(fetchLimits))
	fmt.Fprintf(&buf, "  planning_ms=%d\n", cfg.PlanningMS)

	buf.WriteString("\nData\n")
	fmt.Fprintf(&buf, "  rows=%d\n", rows)
	fmt.Fprintf(&buf, "  parquet_bytes=%d\n", bytes)
	fmt.Fprintf(&buf, "  parquet_files=%d\n", parquetFiles)
	fmt.Fprintf(&buf, "  avg_rows_per_file=%d\n", avgRowsPerFile)
	fmt.Fprintf(&buf, "  avg_bytes_per_file=%d\n", avgBytesPerFile)

	buf.WriteString("\nExport timings\n")
	fmt.Fprintf(&buf, "  db_connect_ms=%d\n", sumInt64(dbConn))
	fmt.Fprintf(&buf, "  query_ms=%d\n", sumInt64(query))
	fmt.Fprintf(&buf, "  convert_ms=%d\n", sumInt64(convert))
	fmt.Fprintf(&buf, "  parquet_meta_ms=%d\n", sumInt64(parquetMeta))
	fmt.Fprintf(&buf, "  upload_ms=%d\n", sumInt64(upload))
	if bench != nil {
		fmt.Fprintf(&buf, "  commit_ms=%d\n", bench.CommitMS)
	} else {
		fmt.Fprintf(&buf, "  commit_ms=0\n")
	}
	fmt.Fprintf(&buf, "  export_wall_ms=%d\n", exportWall)

	buf.WriteString("\nTask stats\n")
	fmt.Fprintf(&buf, "  avg_task_ms=%d\n", func() int64 {
		if len(taskDurations) == 0 {
			return 0
		}
		return sumInt64(taskDurations) / int64(len(taskDurations))
	}())
	fmt.Fprintf(&buf, "  median_task_ms=%d\n", medianInt64(taskDurations))
	fmt.Fprintf(&buf, "  max_task_ms=%d\n", maxTaskMS)
	fmt.Fprintf(&buf, "  min_task_ms=%d\n", minTaskMS)

	buf.WriteString("\nThroughput\n")
	fmt.Fprintf(&buf, "  rows_per_sec=%s\n", formatRate(rowsPerSec))
	fmt.Fprintf(&buf, "  mib_per_sec=%s\n", formatRate(mibPerSec))
	fmt.Fprintf(&buf, "  files_per_sec=%s\n", formatRate(filesPerSec))

	buf.WriteString("\nWorker utilization\n")
	fmt.Fprintf(&buf, "  worker_count=%d\n", len(workerStats))
	for _, worker := range workerStats {
		fmt.Fprintf(&buf, "  %s tasks=%d rows=%d bytes=%d\n", worker.ID, worker.Tasks, worker.Rows, worker.Bytes)
	}

	buf.WriteString("\nIceberg registration\n")
	if cfg.RegistrationEnable {
		fmt.Fprintf(&buf, "  enabled=yes\n")
		fmt.Fprintf(&buf, "  engine=%s\n", cfg.RegistrationEngine)
	} else {
		fmt.Fprintf(&buf, "  enabled=no\n")
		fmt.Fprintf(&buf, "  engine=n/a\n")
	}
	fmt.Fprintf(&buf, "  status=%s\n", regStatus)
	fmt.Fprintf(&buf, "  duration_ms=%d\n", regDurationMS)
	fmt.Fprintf(&buf, "  objects=%d\n", regObjects)
	if strings.EqualFold(regStatus, "FAILED") && regError != "" {
		fmt.Fprintf(&buf, "  error=%s\n", regError)
	}

	return buf.String()
}

// printBenchReport renders bench report for the operator.
// It exists to keep reporting logic separate from core execution.
func printBenchReport(out *cliOutput, cfg benchReportConfig, runID, runStatus string, details runDetails, bench *benchAgg, observedStart time.Time) {
	report := renderBenchReport(cfg, runID, runStatus, details, bench, time.Since(observedStart).Milliseconds())
	if report == "" {
		return
	}
	if out != nil {
		out.writeBlock(out.stdout, report)
		return
	}
	_, _ = io.WriteString(os.Stdout, report)
}
