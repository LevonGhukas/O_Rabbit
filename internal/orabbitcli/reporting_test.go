package orabbitcli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRenderBenchReportFormatsRequestedSections(t *testing.T) {
	worker1 := "docker-worker-1"
	worker2 := "docker-worker-2"
	report := renderBenchReport(
		benchReportConfig{
			WorkersAvailable:   2,
			MaxInFlightTasks:   2,
			AutoTune:           true,
			FetchLimit:         50000,
			PlanningMS:         123,
			Source:             "postgres public.big_orders",
			Target:             "s3://bucket1/postgres/public__big_orders",
			RegistrationEnable: true,
			RegistrationEngine: "rest-go",
		},
		"run-123",
		"SUCCEEDED",
		runDetails{
			Run: runState{
				ID:        "run-123",
				Status:    "SUCCEEDED",
				StartedAt: "2026-04-17T12:00:00Z",
			},
			Tasks: []taskInfo{
				{
					ID:             "task-1",
					TaskIndex:      1,
					Status:         "SUCCEEDED",
					WorkerID:       &worker1,
					RowsRead:       4,
					BytesWritten:   2000,
					ParquetObjects: []byte(`[{"key":"postgres/public__big_orders/part-000001.parquet"}]`),
					StartedAt:      stringPtr("2026-04-17T12:00:01.000Z"),
					FinishedAt:     stringPtr("2026-04-17T12:00:01.030Z"),
				},
				{
					ID:             "task-2",
					TaskIndex:      2,
					Status:         "SUCCEEDED",
					WorkerID:       &worker2,
					RowsRead:       6,
					BytesWritten:   3417,
					ParquetObjects: []byte(`[{"key":"postgres/public__big_orders/part-000002.parquet"}]`),
					StartedAt:      stringPtr("2026-04-17T12:00:01.040Z"),
					FinishedAt:     stringPtr("2026-04-17T12:00:01.090Z"),
				},
			},
		},
		&benchAgg{
			ByTask: map[string]taskBenchPayload{
				"task-1": {
					DBConnectMS:    11,
					QueryMS:        1,
					ConvertMS:      2,
					ParquetMetaMS:  0,
					UploadMS:       14,
					TaskTotalMS:    30,
					Rows:           4,
					ParquetBytes:   2000,
					CursorDomain:   "int64",
					PartitionLower: "0",
					PartitionUpper: "2",
				},
				"task-2": {
					DBConnectMS:    11,
					QueryMS:        1,
					ConvertMS:      2,
					ParquetMetaMS:  0,
					UploadMS:       14,
					TaskTotalMS:    50,
					Rows:           6,
					ParquetBytes:   3417,
					CursorDomain:   "int64",
					PartitionLower: "2",
					PartitionUpper: "4",
				},
			},
			CommitMS: 17,
			CommitTS: "2026-04-17T12:00:01.090Z",
			Registration: registrationBench{
				Seen:       true,
				Status:     "SUCCEEDED",
				DurationMS: 2382,
				Objects:    2,
			},
		},
		3478,
	)

	want := "" +
		"Run summary\n" +
		"  run_id=run-123\n" +
		"  status=SUCCEEDED\n" +
		"  registration=SUCCEEDED\n" +
		"  overall_wall_ms=3478\n" +
		"  source=postgres public.big_orders\n" +
		"  target=s3://bucket1/postgres/public__big_orders\n\n" +
		"Planning\n" +
		"  workers_available=2\n" +
		"  workers_used=2\n" +
		"  tasks=2\n" +
		"  max_in_flight=2\n" +
		"  planned_tasks=auto\n" +
		"  effective_partition_span=2\n" +
		"  fetch_limit_rows=50000\n" +
		"  planning_ms=123\n\n" +
		"Data\n" +
		"  rows=10\n" +
		"  parquet_bytes=5417\n" +
		"  parquet_files=2\n" +
		"  avg_rows_per_file=5\n" +
		"  avg_bytes_per_file=2708\n\n" +
		"Export timings\n" +
		"  db_connect_ms=22\n" +
		"  query_ms=2\n" +
		"  convert_ms=4\n" +
		"  parquet_meta_ms=0\n" +
		"  upload_ms=28\n" +
		"  commit_ms=17\n" +
		"  export_wall_ms=90\n\n" +
		"Task stats\n" +
		"  avg_task_ms=40\n" +
		"  median_task_ms=40\n" +
		"  max_task_ms=50\n" +
		"  min_task_ms=30\n\n" +
		"Throughput\n" +
		"  rows_per_sec=111.11\n" +
		"  mib_per_sec=0.06\n" +
		"  files_per_sec=22.22\n\n" +
		"Worker utilization\n" +
		"  worker_count=2\n" +
		"  docker-worker-1 tasks=1 rows=4 bytes=2000\n" +
		"  docker-worker-2 tasks=1 rows=6 bytes=3417\n\n" +
		"Iceberg registration\n" +
		"  enabled=yes\n" +
		"  engine=rest-go\n" +
		"  status=SUCCEEDED\n" +
		"  duration_ms=2382\n" +
		"  objects=2\n"

	if report != want {
		t.Fatalf("renderBenchReport mismatch\nwant:\n%s\ngot:\n%s", want, report)
	}
	if strings.Contains(report, "\t") {
		t.Fatalf("renderBenchReport used tabs: %q", report)
	}
	if strings.Contains(report, "\r") {
		t.Fatalf("renderBenchReport used carriage returns: %q", report)
	}
}

func TestPrintBenchReportWritesSingleBufferedBlock(t *testing.T) {
	writer := &countingWriter{}
	out := &cliOutput{
		stdout: writer,
		stderr: writer,
	}

	printBenchReport(out, benchReportConfig{
		WorkersAvailable:   1,
		MaxInFlightTasks:   1,
		AutoTune:           false,
		PlannedTasks:       1,
		FetchLimit:         50000,
		PlanningMS:         55,
		Source:             "mssql SalesDB.dbo.Orders",
		Target:             "s3://bucket1/mssql/SalesDB__dbo__Orders",
		RegistrationEnable: false,
	}, "run-buffered", "SUCCEEDED", runDetails{
		Tasks: []taskInfo{{
			ID:             "task-1",
			Status:         "SUCCEEDED",
			RowsRead:       4,
			BytesWritten:   2000,
			ParquetObjects: []byte(`[{"key":"part-000001.parquet"}]`),
			StartedAt:      stringPtr("2026-04-17T12:00:01.000Z"),
			FinishedAt:     stringPtr("2026-04-17T12:00:01.032Z"),
		}},
	}, &benchAgg{
		ByTask: map[string]taskBenchPayload{
			"task-1": {
				DBConnectMS:   11,
				QueryMS:       1,
				ConvertMS:     2,
				ParquetMetaMS: 0,
				UploadMS:      14,
				TaskTotalMS:   32,
			},
		},
		CommitMS: 9,
		CommitTS: "2026-04-17T12:00:01.040Z",
	}, time.Now().Add(-time.Second))

	got := writer.String()
	if !strings.HasPrefix(got, "Run summary\n") {
		t.Fatalf("benchmark block missing Run summary header: %q", got)
	}
	if writer.writes != 1 {
		t.Fatalf("printBenchReport wrote %d times, want 1", writer.writes)
	}
	if strings.Contains(got, "\t") {
		t.Fatalf("printBenchReport output used tabs: %q", got)
	}
	if strings.Contains(got, "\r") {
		t.Fatalf("printBenchReport output used carriage returns: %q", got)
	}
}

func TestStreamSSEWaitsForRegistrationWhenRequested(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)

		fmt.Fprintf(w, "data: %s\n\n", `{"message":"run committed","ts":"2026-04-17T12:00:01.090Z","fields_json":{"commit_ms":17}}`)
		fl.Flush()
		fmt.Fprintf(w, "data: %s\n\n", `{"message":"run SUCCEEDED","ts":"2026-04-17T12:00:01.091Z","fields_json":{}}`)
		fl.Flush()

		time.Sleep(25 * time.Millisecond)

		fmt.Fprintf(w, "data: %s\n\n", `{"message":"iceberg registration SUCCEEDED","ts":"2026-04-17T12:00:03.473Z","fields_json":{"duration_ms":2382,"objects":101}}`)
		fl.Flush()
	}))
	defer server.Close()

	status, bench, err := streamSSE(context.Background(), server.URL, nil, true)
	if err != nil {
		t.Fatalf("streamSSE: %v", err)
	}
	if status != "SUCCEEDED" {
		t.Fatalf("status=%q want SUCCEEDED", status)
	}
	if bench == nil {
		t.Fatal("bench is nil")
	}
	if bench.CommitMS != 17 {
		t.Fatalf("commit_ms=%d want 17", bench.CommitMS)
	}
	if !bench.Registration.Seen {
		t.Fatal("registration event not observed")
	}
	if bench.Registration.Status != "SUCCEEDED" {
		t.Fatalf("registration status=%q", bench.Registration.Status)
	}
	if bench.Registration.DurationMS != 2382 {
		t.Fatalf("registration duration_ms=%d want 2382", bench.Registration.DurationMS)
	}
	if bench.Registration.Objects != 101 {
		t.Fatalf("registration objects=%d want 101", bench.Registration.Objects)
	}
}

func stringPtr(v string) *string {
	return &v
}

type countingWriter struct {
	writes int
	buf    bytes.Buffer
}

func (w *countingWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.buf.Write(p)
}

func (w *countingWriter) String() string {
	return w.buf.String()
}
