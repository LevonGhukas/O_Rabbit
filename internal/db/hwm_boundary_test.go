package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

func TestCompleteTaskAttemptPersistsHWMBoundary(t *testing.T) {
	st := openTestStore(t)
	createLeaseTestTask(t, st, "hwm-boundary")
	ctx := context.Background()
	t0 := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", t0, LeasePolicy{Duration: time.Minute}, fixedGenerator("attempt-hwm"), fixedGenerator("token-hwm"))
	if err != nil || !ok {
		t.Fatal(err)
	}
	boundary := []byte(`{"cursor_value":"2026-01-01T00:00:03Z","row_hashes":{"aa":1}}`)

	// A seed task that dropped every row it read writes no Parquet files.
	accepted, _, _, err := st.CompleteTaskAttemptWithFailureClassAt(ctx, "", a.ID, a.AttemptID, a.FencingToken, "worker", "SUCCEEDED", nil, "", []byte("[]"), 0, 0, 0, t0.Add(time.Second), boundary)
	if err != nil || !accepted {
		t.Fatalf("accepted=%v err=%v", accepted, err)
	}

	tasks, err := st.ListTasksForRun(ctx, a.RunID)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks=%+v err=%v", tasks, err)
	}
	if string(tasks[0].HWMBoundary) != string(boundary) {
		t.Fatalf("hwm boundary = %s, want %s", tasks[0].HWMBoundary, boundary)
	}

	if accepted, msg, _, err := st.CompleteTaskAttemptWithFailureClassAt(ctx, "", a.ID, a.AttemptID, a.FencingToken, "worker", "SUCCEEDED", nil, "", []byte("[]"), 0, 0, 0, t0.Add(time.Second), boundary); err != nil || !accepted || msg != "already accepted" {
		t.Fatalf("identical retry: accepted=%v msg=%q err=%v", accepted, msg, err)
	}
	if _, _, _, err := st.CompleteTaskAttemptWithFailureClassAt(ctx, "", a.ID, a.AttemptID, a.FencingToken, "worker", "SUCCEEDED", nil, "", []byte("[]"), 0, 0, 0, t0.Add(time.Second), []byte(`{"cursor_value":"x"}`)); err == nil {
		t.Fatal("retry with a different boundary was accepted")
	}
}

func TestResultDigestWithoutBoundaryIsUnchanged(t *testing.T) {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%d\x00%d\x00%d\x00%s", "SUCCEEDED", []byte("[]"), 1, 2, 3, "")
	legacy := hex.EncodeToString(h.Sum(nil))
	if got := resultDigest("SUCCEEDED", []byte("[]"), 1, 2, 3, "", nil); got != legacy {
		t.Fatalf("digest without boundary = %s, want the pre-boundary digest %s", got, legacy)
	}
	if legacy == resultDigest("SUCCEEDED", []byte("[]"), 1, 2, 3, "", []byte(`{}`)) {
		t.Fatal("boundary must be part of the digest")
	}
}
