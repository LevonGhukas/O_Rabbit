package db

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestUploadCapacityLeasesAreGlobalReusableAndExpire(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	policy := LeasePolicy{Duration: 10 * time.Minute, MaxAttempts: 3}

	assignments := make([]Task, 4)
	for i := range assignments {
		suffix := fmt.Sprintf("upload-%d", i)
		createLeaseTestTask(t, st, suffix)
		task, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-"+suffix, now, policy, fixedGenerator("attempt-"+suffix), fixedGenerator("fence-"+suffix))
		if err != nil || !ok {
			t.Fatalf("assign %d ok=%v err=%v", i, ok, err)
		}
		assignments[i] = task
	}

	type result struct {
		index    int
		lease    UploadCapacityLease
		acquired bool
		err      error
	}
	results := make(chan result, len(assignments))
	var wg sync.WaitGroup
	for i, task := range assignments {
		i, task := i, task
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, acquired, err := st.AcquireUploadCapacity(ctx, "", task.ID, task.AttemptID, task.FencingToken, *task.WorkerID, now, 30*time.Second, 2, fixedGenerator(fmt.Sprintf("upload-lease-%d", i)), fixedGenerator(fmt.Sprintf("upload-token-%d", i)))
			results <- result{index: i, lease: lease, acquired: acquired, err: err}
		}()
	}
	wg.Wait()
	close(results)

	acquired := map[int]UploadCapacityLease{}
	var waiting []int
	for r := range results {
		if r.err != nil {
			t.Fatalf("acquire %d: %v", r.index, r.err)
		}
		if r.acquired {
			acquired[r.index] = r.lease
		} else {
			waiting = append(waiting, r.index)
		}
	}
	if len(acquired) != 2 || len(waiting) != 2 {
		t.Fatalf("acquired=%d waiting=%d want 2/2", len(acquired), len(waiting))
	}

	var releasedIndex int
	var released UploadCapacityLease
	for releasedIndex, released = range acquired {
		break
	}
	task := assignments[releasedIndex]
	if err := st.ReleaseUploadCapacity(ctx, "", task.ID, task.AttemptID, *task.WorkerID, released.ID, released.Token, now.Add(time.Second)); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := st.ReleaseUploadCapacity(ctx, "", task.ID, task.AttemptID, *task.WorkerID, released.ID, released.Token, now.Add(2*time.Second)); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}

	waitingIndex := waiting[0]
	waitingTask := assignments[waitingIndex]
	reused, ok, err := st.AcquireUploadCapacity(ctx, "", waitingTask.ID, waitingTask.AttemptID, waitingTask.FencingToken, *waitingTask.WorkerID, now.Add(2*time.Second), 30*time.Second, 2, fixedGenerator("reused-upload-lease"), fixedGenerator("reused-upload-token"))
	if err != nil || !ok {
		t.Fatalf("reuse released capacity ok=%v err=%v", ok, err)
	}
	renewed, ok, err := st.AcquireUploadCapacity(ctx, "", waitingTask.ID, waitingTask.AttemptID, waitingTask.FencingToken, *waitingTask.WorkerID, now.Add(3*time.Second), 30*time.Second, 2, nil, nil)
	if err != nil || !ok || renewed.ID != reused.ID || renewed.Token != reused.Token {
		t.Fatalf("idempotent renewal lease=%+v ok=%v err=%v", renewed, ok, err)
	}

	lastIndex := waiting[1]
	lastTask := assignments[lastIndex]
	if _, ok, err := st.AcquireUploadCapacity(ctx, "", lastTask.ID, lastTask.AttemptID, lastTask.FencingToken, *lastTask.WorkerID, now.Add(3*time.Second), 30*time.Second, 2, fixedGenerator("blocked"), fixedGenerator("blocked")); err != nil || ok {
		t.Fatalf("capacity should remain full ok=%v err=%v", ok, err)
	}
	if _, ok, err := st.AcquireUploadCapacity(ctx, "", lastTask.ID, lastTask.AttemptID, lastTask.FencingToken, *lastTask.WorkerID, now.Add(40*time.Second), 30*time.Second, 2, fixedGenerator("after-expiry"), fixedGenerator("after-expiry-token")); err != nil || !ok {
		t.Fatalf("expired crash lease was not reclaimed ok=%v err=%v", ok, err)
	}

	var attemptCount int
	if err := st.db.QueryRow(`SELECT COALESCE(SUM(attempt_count),0) FROM tasks`).Scan(&attemptCount); err != nil {
		t.Fatal(err)
	}
	if attemptCount != len(assignments) {
		t.Fatalf("capacity admission changed task attempts: got %d want %d", attemptCount, len(assignments))
	}
}
