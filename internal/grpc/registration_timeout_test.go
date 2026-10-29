package grpcapi

import (
	"context"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	grpcpb "github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
)

const (
	hungRegistrationTimeout = 200 * time.Millisecond
	hungRegistrationReturn  = 5 * time.Second
)

// hungRegistrar blocks until its context ends, like an ice process that never
// answers. It can first mark the external commit as started.
type hungRegistrar struct {
	startExternal bool
	started       chan struct{}
}

func (h *hungRegistrar) RegisterRun(ctx context.Context, req icebergreg.RunRequest) (icebergreg.RunResult, error) {
	if h.startExternal {
		if err := req.BeforeExternalCommit(); err != nil {
			return icebergreg.RunResult{}, err
		}
	}
	select {
	case h.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return icebergreg.RunResult{}, ctx.Err()
}

// claimableHungRegistration leaves a registration whose first attempt was
// abandoned by a stale leader and expired, so a fresh server can claim it.
func claimableHungRegistration(t *testing.T, runID string, registrar icebergRegistrationRunner) (*db.Store, time.Time) {
	t.Helper()
	st := openGRPCTestStore(t)
	createGRPCTestRegistrableRunAndTask(t, st, runID, "job-"+runID, "task-"+runID)
	assignGRPCTestAttempt(t, st, "task-"+runID, "worker")

	workCtx, loseLeadership := context.WithCancel(context.Background())
	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, registrar)
	srv.SetLeadershipGuard(cancellableLeadership{ctx: workCtx})
	srv.commitRunFn = func(ctx context.Context, id string) error {
		return saveGRPCTestVerifiedEmptyIntent(ctx, st, id, "exports/orders")
	}
	if _, err := srv.ReportTaskResult(context.Background(), &grpcpb.ReportTaskResultRequest{
		WorkerId: "worker", TaskId: "task-" + runID, RunId: runID,
		AttemptId: "attempt-task-" + runID, FencingToken: "token-task-" + runID, Status: "SUCCEEDED",
	}); err != nil {
		t.Fatalf("ReportTaskResult: %v", err)
	}
	waitRegistrationStarted(t, registrar)
	loseLeadership()
	time.Sleep(100 * time.Millisecond)

	policy := db.RegistrationPolicy{LeaseDuration: 30 * time.Second, MaxAttempts: 5, BackoffBase: time.Second, BackoffMax: time.Minute}
	expiredAt := time.Now().Add(time.Minute)
	if n, err := st.ExpireRegistrationAttempts(context.Background(), expiredAt, policy); err != nil || n != 1 {
		t.Fatalf("expire n=%d err=%v", n, err)
	}
	return st, expiredAt
}

func waitRegistrationStarted(t *testing.T, registrar icebergRegistrationRunner) {
	t.Helper()
	h := registrar.(*hungRegistrar)
	select {
	case <-h.started:
	case <-time.After(hungRegistrationReturn):
		t.Fatal("registration did not start")
	}
}

func TestProcessRegistrationOnceBoundsHungAttemptAndPersistsFailure(t *testing.T) {
	cases := []struct {
		name          string
		startExternal bool
		outerDeadline bool
		wantStatus    string
	}{
		{"before external commit", false, false, db.RegistrationRetryRequired},
		{"after external commit", true, false, db.RegistrationReconciling},
		{"outer deadline before external commit", false, true, db.RegistrationRetryRequired},
		{"outer deadline after external commit", true, true, db.RegistrationReconciling},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runID := "run-hung-" + string(rune('a'+i))
			// The abandoned first attempt never reaches the external commit, so the
			// registration stays claimable from PREPARED.
			stale := &hungRegistrar{started: make(chan struct{}, 1)}
			st, expiredAt := claimableHungRegistration(t, runID, stale)
			registrar := &hungRegistrar{startExternal: tc.startExternal, started: make(chan struct{}, 1)}

			fresh := NewServer(nil, st, nil, testCryptoKey, time.Second, registrar)
			fresh.SetCommitCatalogPolicy(CommitCatalogPolicy{RegistrationTimeout: hungRegistrationTimeout})
			fresh.nowFn = func() time.Time { return expiredAt.Add(2 * time.Second) }
			ctx := context.Background()
			if tc.outerDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, hungRegistrationTimeout)
				defer cancel()
			}

			type result struct {
				processed bool
				err       error
			}
			done := make(chan result, 1)
			go func() {
				processed, err := fresh.ProcessRegistrationOnce(ctx)
				done <- result{processed, err}
			}()
			select {
			case res := <-done:
				if res.err != nil || !res.processed {
					t.Fatalf("ProcessRegistrationOnce processed=%v err=%v", res.processed, res.err)
				}
			case <-time.After(hungRegistrationReturn):
				t.Fatal("ProcessRegistrationOnce hung past the registration timeout")
			}

			reg, err := st.GetRegistrationForRun(context.Background(), runID)
			if err != nil {
				t.Fatal(err)
			}
			if reg.Status != tc.wantStatus {
				t.Fatalf("status=%s want %s", reg.Status, tc.wantStatus)
			}
			if reg.LastErrorMessage == nil || *reg.LastErrorMessage == "" {
				t.Fatalf("last_error_message not recorded: %+v", reg)
			}
		})
	}
}
