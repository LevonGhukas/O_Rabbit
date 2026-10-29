package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// drillReport is what one restore drill measured. It is written to the
// artifacts directory so every drill leaves a record.
type drillReport struct {
	StartedAt  time.Time `json:"started_at"`
	DisasterAt time.Time `json:"disaster_at"`
	ReadyAt    time.Time `json:"master_ready_at"`
	// RestoreSeconds is restore plus checks plus start-up, as run by
	// scripts/master-restore.sh. RTOSeconds adds everything between losing
	// the master and it serving again, except human reaction time.
	RestoreSeconds float64 `json:"restore_seconds"`
	RTOSeconds     float64 `json:"rto_seconds"`
	// LastReplicated is the newest master activity in the restored copy;
	// RPOSeconds is how much earlier than the disaster that was.
	LastReplicated     time.Time `json:"last_replicated_activity"`
	RPOSeconds         float64   `json:"rpo_seconds"`
	InFlightAtDisaster string    `json:"in_flight_at_disaster"`
	InFlightOutcome    string    `json:"in_flight_outcome"`
	Checks             []string  `json:"checks_passed"`
	Outcome            string    `json:"outcome"`
}

// TestDisasterRecoveryDrill loses the master host while a registration is in
// flight, restores the database from Litestream with the operator script
// onto a fresh volume, and checks that the API, workers, catalog and in-flight
// work recover. A second, point-in-time restore then misses the last run and
// checks what a stale backup loses. It needs the backup overlay:
// ORABBIT_E2E_DR=1 scripts/e2e.sh.
func TestDisasterRecoveryDrill(t *testing.T) {
	if os.Getenv("ORABBIT_E2E_DR") != "1" {
		t.Skip("restore drill needs the backup overlay: ORABBIT_E2E_DR=1 scripts/e2e.sh")
	}
	e := stack
	ctx := context.Background()
	report := &drillReport{StartedAt: time.Now().UTC()}
	defer report.write(t)
	spec := runSpec{job: "dr-orders", sourceTable: "orders", icebergName: "orders", incremental: true}
	commitPath := "/v1/namespaces/e2e/tables/orders"

	t.Log("before the disaster: one run committed and registered")
	first := e.publish(ctx, t, spec)

	t.Log("before the disaster: a second run is committed and its catalog commit is in flight")
	e.sourceExec(ctx, t, insertFixtureRows("orders", "order", 1001, 1200))
	// Held far longer than the drill, so the catalog commit is still open
	// when the master disappears.
	fault := e.proxy.inject(faultBeforeCommit, http.MethodPost, commitPath, 10*time.Minute)
	runCtx, cancel := context.WithTimeout(ctx, runDeadline)
	defer cancel()
	inFlight := e.submit(runCtx, t, spec)
	select {
	case <-fault.hit:
	case <-runCtx.Done():
		e.proxy.clear()
		t.Fatalf("run %s never reached the catalog commit", inFlight)
	}
	// Let the backup capture the committed run before the disaster. A backup
	// that still shows it RUNNING cannot recover it: its tasks re-run into
	// the already committed objects and the run fails, leaving committed
	// rows unregistered (docs/master-backup-restore.md, "Runs committed
	// after the backup point"). That gap is documented, not tested here.
	time.Sleep(5 * time.Second)
	report.InFlightAtDisaster = e.runState(ctx, t, inFlight).String()

	t.Log("disaster: the master host and its disk are lost")
	report.DisasterAt = time.Now().UTC()
	compose(t, "kill", "master", "litestream")
	compose(t, "rm", "-f", "master", "litestream")
	command(t, nil, "docker", "volume", "rm", "orabbit-e2e_master_data")
	e.proxy.clear()

	t.Log("recovery: restore onto a fresh volume with scripts/master-restore.sh")
	// The restored master replicates to a new path, as the runbook requires.
	t.Setenv("E2E_LITESTREAM_PATH", "master-b")
	t.Setenv("MASTER_SERVICE", "master")
	t.Setenv("MASTER_HTTP", e.master.base)
	checkFile := filepath.Join(t.TempDir(), "check-db.json")
	restoreStart := time.Now()
	command(t, nil, "scripts/master-restore.sh",
		"--from-path", "master-a", "--old-master-fenced", "--start", "--report", checkFile)
	report.ReadyAt = time.Now().UTC()
	report.RestoreSeconds = report.ReadyAt.Sub(restoreStart).Seconds()
	report.RTOSeconds = report.ReadyAt.Sub(report.DisasterAt).Seconds()

	var check struct {
		LastActivity *time.Time `json:"last_leadership_activity"`
		OK           bool       `json:"ok"`
	}
	raw, err := os.ReadFile(checkFile)
	if err != nil {
		t.Fatalf("read check-db report: %v", err)
	}
	if err := json.Unmarshal(raw, &check); err != nil || check.LastActivity == nil || !check.OK {
		t.Fatalf("check-db report unusable (%v): %s", err, raw)
	}
	report.LastReplicated = check.LastActivity.UTC()
	report.RPOSeconds = report.DisasterAt.Sub(report.LastReplicated).Seconds()
	report.Checks = append(report.Checks, "restored copy passed check-db (integrity, schema, master key)", "master API ready")

	t.Log("check: the worker reconnects to the restored master")
	e.waitWorkerHeartbeatAfter(ctx, t, report.ReadyAt)
	report.Checks = append(report.Checks, "worker reconnected")

	t.Log("check: the in-flight run finishes registration exactly once")
	waitCtx, waitCancel := context.WithTimeout(ctx, runDeadline)
	defer waitCancel()
	resolved := e.master.waitPublished(waitCtx, t, inFlight)
	report.InFlightOutcome = resolved.String()
	view := e.assertTable(ctx, t, "orders", expectedRowsFor("order", 1, 1200), []string{first, inFlight})
	snaps := view.snapshotsByRun()
	if snaps[inFlight] != snaps[first] {
		t.Fatalf("in-flight run committed %d snapshots, the run before the disaster %d (history %v)", snaps[inFlight], snaps[first], snaps)
	}
	report.Checks = append(report.Checks, "in-flight registration resolved", "catalog rows, files and snapshots consistent")

	t.Log("check: a new incremental run continues without replaying or losing rows")
	e.sourceExec(ctx, t, insertFixtureRows("orders", "order", 1201, 1300))
	after := e.publish(ctx, t, spec)
	e.assertTable(ctx, t, "orders", expectedRowsFor("order", 1, 1300), []string{first, inFlight, after})
	report.Checks = append(report.Checks, "post-restore incremental run exported exactly the new rows")

	t.Log("check: the restored master is backed up again and the backup verifies")
	command(t, nil, "scripts/verify-master-backup.sh", "--max-age", "60s")
	report.Checks = append(report.Checks, "new backup lineage verified with verify-master-backup.sh")

	t.Logf("drill: RTO %.0fs (restore script %.0fs), RPO %.1fs", report.RTOSeconds, report.RestoreSeconds, report.RPOSeconds)

	// A backup older than the last commit, as after a replication outage or
	// a point-in-time restore. The run in the gap is committed in S3 and
	// registered in Iceberg, but the restored database has never heard of it.
	t.Log("stale restore: restore to a point before the last run")
	time.Sleep(2 * time.Second) // let replication settle before the restore point
	pointInTime := time.Now().UTC().Truncate(time.Second)
	time.Sleep(2 * time.Second)
	e.sourceExec(ctx, t, insertFixtureRows("orders", "order", 1301, 1350))
	missed := e.publish(ctx, t, spec)
	missedFiles := e.runFiles(ctx, t, []string{missed})
	knownFiles := e.runFiles(ctx, t, []string{first, inFlight, after})

	compose(t, "kill", "master", "litestream")
	compose(t, "rm", "-f", "master", "litestream")
	command(t, nil, "docker", "volume", "rm", "orabbit-e2e_master_data")
	t.Setenv("E2E_LITESTREAM_PATH", "master-c")
	command(t, nil, "scripts/master-restore.sh", "--from-path", "master-b",
		"--timestamp", pointInTime.Format(time.RFC3339), "--old-master-fenced", "--start")

	if _, err := e.master.run(ctx, missed); err == nil {
		t.Fatalf("run %s committed after the restore point is still known; the restore was not stale", missed)
	}
	// Its rows stay published: the catalog and the dataset state in S3 are
	// not rolled back with the database.
	e.assertTableFiles(ctx, t, "orders", expectedRowsFor("order", 1, 1350), append(append([]string{}, knownFiles...), missedFiles...))

	t.Log("stale restore: the next incremental run neither replays nor loses rows")
	e.sourceExec(ctx, t, insertFixtureRows("orders", "order", 1351, 1400))
	next := e.publish(ctx, t, spec)
	e.assertTableFiles(ctx, t, "orders", expectedRowsFor("order", 1, 1400),
		append(append(append([]string{}, knownFiles...), missedFiles...), e.runFiles(ctx, t, []string{next})...))
	report.Checks = append(report.Checks, "stale restore: a run missing from the backup stays published and is not replayed")
}

func (r *drillReport) write(t *testing.T) {
	r.Outcome = "PASS"
	if t.Failed() {
		r.Outcome = "FAIL"
	}
	dir := envOr("ORABBIT_E2E_ARTIFACTS", "e2e-artifacts")
	if !filepath.IsAbs(dir) {
		// go test runs in test/e2e; the artifacts directory is relative to
		// the repository root, like in scripts/e2e.sh.
		dir = filepath.Join("..", "..", dir)
	}
	body, err := json.MarshalIndent(r, "", "  ")
	if err == nil {
		err = os.MkdirAll(dir, 0o755)
	}
	if err == nil {
		err = os.WriteFile(filepath.Join(dir, "dr-drill-report.json"), append(body, '\n'), 0o644)
	}
	if err != nil {
		t.Errorf("write drill report: %v", err)
	}
}

// waitWorkerHeartbeatAfter waits until a worker has reported to the master
// after since, which proves it reconnected.
func (e *env) waitWorkerHeartbeatAfter(ctx context.Context, t *testing.T, since time.Time) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		var workers []struct {
			ID            string `json:"id"`
			LastHeartbeat string `json:"last_heartbeat"`
		}
		if err := e.master.getJSON(ctx, "/api/v1/workers", &workers); err == nil {
			for _, w := range workers {
				if at, err := time.Parse(time.RFC3339Nano, w.LastHeartbeat); err == nil && at.After(since) {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no worker heartbeat after %s", since.Format(time.RFC3339))
		}
		time.Sleep(time.Second)
	}
}

func expectedRowsFor(prefix string, from, to int64) []exportedRow {
	return expectedRows(prefix, from, to, nil)
}

func insertFixtureRows(table, prefix string, from, to int64) string {
	return fmt.Sprintf(`INSERT INTO %s (id, name, created_at)
SELECT g, '%s-' || g, timestamptz '2026-01-01 00:00:00+00' + g * interval '1 minute'
FROM generate_series(%d, %d) AS g`, table, prefix, from, to)
}

// compose runs docker compose against the e2e stack (COMPOSE_FILE is set
// by scripts/e2e.sh).
func compose(t *testing.T, args ...string) {
	t.Helper()
	command(t, nil, "docker", append([]string{"compose"}, args...)...)
}

// command runs a program from the repository root, where scripts/e2e.sh
// runs and COMPOSE_FILE paths are relative to, and fails the test with its
// output on error.
func command(t *testing.T, extraEnv []string, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = filepath.Join("..", "..")
	cmd.Env = append(os.Environ(), extraEnv...)
	out, err := cmd.CombinedOutput()
	t.Logf("$ %s %s\n%s", name, strings.Join(args, " "), out)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}
