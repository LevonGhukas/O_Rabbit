package db

import (
	"context"
	"testing"
	"time"
)

// insertJobFixture adds a job and points runID at it.
func insertJobFixture(t *testing.T, st *Store, jobID, runID, writeMode string, incremental bool) {
	t.Helper()
	inc := 0
	if incremental {
		inc = 1
	}
	now := "2026-07-23T00:00:00Z"
	if _, err := st.db.Exec(`INSERT OR IGNORE INTO jobs(id,name,source_connection_id,target_connection_id,source_sql,target_namespace,target_table,write_mode,incremental,options_json,created_at,updated_at) VALUES(?,?,'src','dst','SELECT 1','ns','tbl',?,?,'{}',?,?)`, jobID, jobID, writeMode, inc, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`UPDATE runs SET job_id=? WHERE id=?`, jobID, runID); err != nil {
		t.Fatal(err)
	}
}

// An earlier registration that definitely ended without registering must not
// block a later full refresh, which replaces the whole table anyway. Append
// jobs and earlier registrations with an unknown outcome keep strict order.
func TestRegistrationBlockingFullRefreshSkipsFinishedFailures(t *testing.T) {
	cases := []struct {
		name        string
		oldStatus   string
		writeMode   string
		incremental bool
		blocked     bool
	}{
		{"full refresh after FAILED", RegistrationFailed, "overwrite", false, false},
		{"full refresh after CANCELED", RegistrationCanceled, "", false, false},
		{"incremental overwrite after FAILED", RegistrationFailed, "overwrite", true, false},
		{"append after FAILED", RegistrationFailed, "append", true, true},
		{"full refresh after RECONCILING", RegistrationReconciling, "overwrite", false, true},
		{"full refresh after QUARANTINED", RegistrationQuarantined, "overwrite", false, true},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := openTestStore(t)
			dataset := "dataset-" + string(rune('a'+i))
			old := insertRegistrationFixture(t, st, "old-"+dataset, dataset, 1, tc.oldStatus)
			newer := insertRegistrationFixture(t, st, "new-"+dataset, dataset, 2, RegistrationPending)
			insertJobFixture(t, st, "job-"+dataset, newer.RunID, tc.writeMode, tc.incremental)

			// The run's projection, which /diagnose shows as "blocked by".
			run, err := st.GetRun(ctx, newer.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if got := run.RegistrationBlockedBy != ""; got != tc.blocked {
				t.Fatalf("blocked_by=%q, want blocked=%t", run.RegistrationBlockedBy, tc.blocked)
			}
			if tc.blocked && run.RegistrationBlockedBy != old.ID {
				t.Fatalf("blocked_by=%q, want %q", run.RegistrationBlockedBy, old.ID)
			}

			// The scheduler must agree with the projection.
			reg, _, ok, err := st.ClaimRegistration(ctx, time.Now(), RegistrationPolicy{MaxAttempts: 3, LeaseDuration: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			claimedNewer := ok && reg.ID == newer.ID
			if claimedNewer == tc.blocked {
				t.Fatalf("claimed newer=%t (ok=%t id=%s), want blocked=%t", claimedNewer, ok, reg.ID, tc.blocked)
			}
		})
	}
}
