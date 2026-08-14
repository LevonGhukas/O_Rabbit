package httpapi

import (
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

func TestSummarizeRunProgress(t *testing.T) {
	workerID := "worker-1"
	summary := summarizeRunProgress([]db.Task{
		{Status: "PENDING", RowsRead: 10, BytesWritten: 100},
		{Status: "RUNNING", RowsRead: 20, BytesWritten: 200, WorkerID: &workerID},
		{Status: "SUCCEEDED", RowsRead: 30, BytesWritten: 300},
		{Status: "FAILED", RowsRead: 40, BytesWritten: 400},
	}, []db.Worker{{ID: "worker-1"}, {ID: "worker-2"}})

	if summary["tasks_total"].(int) != 4 {
		t.Fatalf("tasks_total=%v want 4", summary["tasks_total"])
	}
	if summary["tasks_pending"].(int) != 1 {
		t.Fatalf("tasks_pending=%v want 1", summary["tasks_pending"])
	}
	if summary["tasks_running"].(int) != 1 {
		t.Fatalf("tasks_running=%v want 1", summary["tasks_running"])
	}
	if summary["tasks_succeeded"].(int) != 1 {
		t.Fatalf("tasks_succeeded=%v want 1", summary["tasks_succeeded"])
	}
	if summary["tasks_failed"].(int) != 1 {
		t.Fatalf("tasks_failed=%v want 1", summary["tasks_failed"])
	}
	if summary["rows_read"].(int64) != 100 {
		t.Fatalf("rows_read=%v want 100", summary["rows_read"])
	}
	if summary["bytes_written"].(int64) != 1000 {
		t.Fatalf("bytes_written=%v want 1000", summary["bytes_written"])
	}
	if summary["workers_active"].(int) != 1 {
		t.Fatalf("workers_active=%v want 1", summary["workers_active"])
	}
}
