// Package e2e runs O_Rabbit end to end: a real master and worker export a
// PostgreSQL fixture to MinIO and register it in an Iceberg REST catalog,
// and the tests read the result back through that catalog.
//
// The tests need the stack in test/e2e/docker-compose.yml and are skipped
// unless ORABBIT_E2E=1. scripts/e2e.sh starts the stack, runs them and
// removes it again.
package e2e

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// env is the running stack shared by every test.
type env struct {
	master    *masterClient
	catalog   *catalogReader
	proxy     *catalogProxy
	sourceDSN string
	clientBin string
	// proxyURLForMaster is the fault proxy as seen from the master container.
	proxyURLForMaster string
}

var stack *env

const (
	// registrationTimeout must match ORABBIT_REGISTRATION_TIMEOUT in
	// docker-compose.yml. Faults hold the catalog longer than this.
	registrationTimeout = 10 * time.Second
	faultHold           = registrationTimeout + 5*time.Second
	// faultSettle is how long past its hold a fault that was hit may take to
	// finish dropping the connection.
	faultSettle = 5 * time.Second
	// runDeadline bounds one run, including a fault and its recovery.
	runDeadline = 3 * time.Minute
	bucket      = "e2e-lake"
)

func TestMain(m *testing.M) {
	if os.Getenv("ORABBIT_E2E") != "1" {
		fmt.Println("skipping end-to-end tests: set ORABBIT_E2E=1 and start the stack (scripts/e2e.sh)")
		os.Exit(0)
	}
	e, cleanup, err := setup()
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e setup:", err)
		cleanup()
		os.Exit(1)
	}
	stack = e
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func setup() (*env, func(), error) {
	var cleanups []func()
	cleanup := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	token := strings.TrimSpace(os.Getenv("ORABBIT_HTTP_AUTH_TOKEN"))
	if token == "" {
		return nil, cleanup, fmt.Errorf("ORABBIT_HTTP_AUTH_TOKEN must match the stack's master")
	}
	binDir, err := os.MkdirTemp("", "orabbit-e2e-")
	if err != nil {
		return nil, cleanup, err
	}
	cleanups = append(cleanups, func() { _ = os.RemoveAll(binDir) })

	// The CLI under test is built from this checkout, like the images.
	clientBin := filepath.Join(binDir, "orabbit-client")
	build := exec.Command("go", "build", "-o", clientBin, "./cmd/orabbit-client")
	build.Dir = filepath.Join("..", "..")
	if out, err := build.CombinedOutput(); err != nil {
		return nil, cleanup, fmt.Errorf("build orabbit-client: %w\n%s", err, out)
	}

	catalogURL := envOr("ORABBIT_E2E_CATALOG_URL", "http://127.0.0.1:15001")
	// Must be reachable from the master container: it listens on all
	// interfaces because Linux Docker routes host.docker.internal to the
	// bridge gateway, not to loopback.
	proxy, err := startCatalogProxy(envOr("ORABBIT_E2E_PROXY_LISTEN", "0.0.0.0:18181"), catalogURL)
	if err != nil {
		return nil, cleanup, err
	}
	cleanups = append(cleanups, func() { _ = proxy.Close() })

	e := &env{
		master: &masterClient{
			base:  envOr("ORABBIT_E2E_MASTER_URL", "http://127.0.0.1:19100"),
			token: token,
			http:  &http.Client{Timeout: 15 * time.Second},
		},
		catalog: &catalogReader{
			catalogURL: catalogURL,
			s3Props: map[string]string{
				"s3.endpoint":                 envOr("ORABBIT_E2E_S3_ENDPOINT", "http://127.0.0.1:19000"),
				"s3.region":                   "us-east-1",
				"s3.access-key-id":            "e2e-access",
				"s3.secret-access-key":        "e2e-secret-key",
				"s3.force-virtual-addressing": "false",
			},
			http: &http.Client{Timeout: 15 * time.Second},
		},
		proxy:             proxy,
		sourceDSN:         envOr("ORABBIT_E2E_SOURCE_DSN", "postgres://e2e:e2e@127.0.0.1:15432/e2e?sslmode=disable"),
		clientBin:         clientBin,
		proxyURLForMaster: envOr("ORABBIT_E2E_PROXY_URL_FOR_MASTER", "http://host.docker.internal:18181"),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := e.master.waitReady(ctx); err != nil {
		return nil, cleanup, err
	}
	if err := waitHTTP(ctx, catalogURL+"/v1/config"); err != nil {
		return nil, cleanup, fmt.Errorf("iceberg catalog: %w", err)
	}
	if err := e.waitWorker(ctx); err != nil {
		return nil, cleanup, err
	}
	return e, cleanup, nil
}

// TestFullExportOverwritesTable exports a table twice with full overwrite
// runs and checks that the second snapshot replaces the first one's rows.
func TestFullExportOverwritesTable(t *testing.T) {
	e := stack
	ctx := context.Background()
	spec := runSpec{job: "e2e-people-full", sourceTable: "people", icebergName: "people", incremental: false}

	first := e.publish(ctx, t, spec)
	e.assertTable(ctx, t, "people", expectedRows("person", 1, 1000, nil), []string{first})

	e.sourceExec(ctx, t,
		`DELETE FROM people WHERE id > 900`,
		`UPDATE people SET name = 'renamed-1' WHERE id = 1`,
	)
	second := e.publish(ctx, t, spec)
	view := e.assertTable(ctx, t, "people", expectedRows("person", 1, 900, map[int64]string{1: "renamed-1"}), []string{second})

	if got := view.currentRunID(); got != second {
		t.Fatalf("current snapshot belongs to run %q, want %q", got, second)
	}
	snaps := view.snapshotsByRun()
	if snaps[first] == 0 || snaps[second] == 0 {
		t.Fatalf("snapshot history %v must keep both runs", snaps)
	}
}

// TestIncrementalExportSurvivesCatalogFaults appends to one table with
// incremental runs. Two of them hit a catalog timeout on the commit: once
// before the catalog applied it and once after. Both must end registered,
// with every row exactly once and one commit per run.
func TestIncrementalExportSurvivesCatalogFaults(t *testing.T) {
	e := stack
	ctx := context.Background()
	spec := runSpec{job: "e2e-events-incremental", sourceTable: "events", icebergName: "events", incremental: true}
	commitPath := "/v1/namespaces/e2e/tables/events"
	var runs []string

	t.Log("initial load")
	initial := e.publish(ctx, t, spec)
	runs = append(runs, initial)
	e.assertTable(ctx, t, "events", expectedRows("event", 1, 1000, nil), runs)

	t.Log("incremental run without faults")
	e.sourceExec(ctx, t, insertEvents(1001, 1250))
	clean := e.publish(ctx, t, spec)
	runs = append(runs, clean)
	view := e.assertTable(ctx, t, "events", expectedRows("event", 1, 1250, nil), runs)
	commitsPerRun := view.snapshotsByRun()[clean]
	if commitsPerRun == 0 {
		t.Fatalf("incremental run %s left no snapshot", clean)
	}

	t.Log("commit times out before the catalog applies it")
	e.sourceExec(ctx, t, insertEvents(1251, 1300))
	commitsBefore := e.proxy.reachedCatalog(http.MethodPost, commitPath)
	fault := e.proxy.inject(faultBeforeCommit, http.MethodPost, commitPath, faultHold)
	notApplied := e.publish(ctx, t, spec)
	e.assertFaultFired(t, fault)
	runs = append(runs, notApplied)
	view = e.assertTable(ctx, t, "events", expectedRows("event", 1, 1300, nil), runs)
	notAppliedRun := e.runState(ctx, t, notApplied)
	if notAppliedRun.Reconciliation.Outcome != "DEFINITELY_NOT_COMMITTED" {
		t.Fatalf("a commit the catalog never applied must be reconciled as DEFINITELY_NOT_COMMITTED: %s", notAppliedRun)
	}
	// The held commit never reached the catalog; the retry is the only one.
	if got := e.proxy.reachedCatalog(http.MethodPost, commitPath) - commitsBefore; got != 1 {
		t.Fatalf("%d commits reached the catalog, want 1 (the retry)", got)
	}

	t.Log("commit applied but its reply is lost")
	e.sourceExec(ctx, t, insertEvents(1301, 1350))
	commitsBefore = e.proxy.reachedCatalog(http.MethodPost, commitPath)
	fault = e.proxy.inject(faultLoseCommitReply, http.MethodPost, commitPath, faultHold)
	lostReply := e.publish(ctx, t, spec)
	e.assertFaultFired(t, fault)
	runs = append(runs, lostReply)
	view = e.assertTable(ctx, t, "events", expectedRows("event", 1, 1350, nil), runs)
	lostReplyRun := e.runState(ctx, t, lostReply)
	if lostReplyRun.Reconciliation.Outcome != "EXACTLY_COMMITTED" {
		t.Fatalf("an applied commit with a lost reply must be reconciled as EXACTLY_COMMITTED: %s", lostReplyRun)
	}
	// The applied commit must not be sent again.
	if got := e.proxy.reachedCatalog(http.MethodPost, commitPath) - commitsBefore; got != 1 {
		t.Fatalf("%d commits reached the catalog, want 1 (the one whose reply was lost)", got)
	}

	// No duplicate publication: each faulted run committed exactly as many
	// snapshots as the fault-free run did.
	snaps := view.snapshotsByRun()
	for _, runID := range []string{notApplied, lostReply} {
		if snaps[runID] != commitsPerRun {
			t.Fatalf("run %s committed %d snapshots, want %d like a fault-free run (history %v)", runID, snaps[runID], commitsPerRun, snaps)
		}
	}
}

// publish submits a run and waits until it is committed and registered.
func (e *env) publish(ctx context.Context, t *testing.T, spec runSpec) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, runDeadline)
	defer cancel()
	runID := e.submit(ctx, t, spec)
	t.Logf("submitted run %s (%s)", runID, spec.job)
	e.master.waitPublished(ctx, t, runID)
	return runID
}

func (e *env) runState(ctx context.Context, t *testing.T, runID string) runView {
	t.Helper()
	run, err := e.master.run(ctx, runID)
	if err != nil {
		t.Fatalf("read run %s: %v", runID, err)
	}
	return run
}

// assertFaultFired fails unless the fault matched a request, then waits for it
// to finish. The run can recover before the hold ends, so a hit fault may still
// be holding its connection when the run is already registered.
func (e *env) assertFaultFired(t *testing.T, f *catalogFault) {
	t.Helper()
	select {
	case <-f.hit:
	default:
		e.proxy.clear()
		t.Fatalf("injected catalog fault on %s %s never fired; the scenario did not exercise a timeout", f.method, f.path)
	}
	select {
	case <-f.fired:
	case <-time.After(f.hold + faultSettle):
		t.Fatalf("injected catalog fault on %s %s did not finish within %s", f.method, f.path, f.hold+faultSettle)
	}
}

// assertTable reads the table through the catalog and checks its rows, and
// that its live data files are exactly the files the master accepted for
// runs, each once.
func (e *env) assertTable(ctx context.Context, t *testing.T, table string, want []exportedRow, runs []string) tableView {
	t.Helper()
	return e.assertTableFiles(ctx, t, table, want, e.runFiles(ctx, t, runs))
}

// runFiles returns the Iceberg paths of the files the master accepted for
// runs.
func (e *env) runFiles(ctx context.Context, t *testing.T, runs []string) []string {
	t.Helper()
	var out []string
	for _, runID := range runs {
		keys, err := e.master.artifactKeys(ctx, runID)
		if err != nil {
			t.Fatalf("artifacts of run %s: %v", runID, err)
		}
		for _, key := range keys {
			out = append(out, "s3://"+bucket+"/"+strings.TrimPrefix(key, "/"))
		}
	}
	return out
}

// assertTableFiles is assertTable with the expected data files given
// directly, for runs the master no longer knows.
func (e *env) assertTableFiles(ctx context.Context, t *testing.T, table string, want []exportedRow, wantFiles []string) tableView {
	t.Helper()
	view, err := e.catalog.load(ctx, "e2e", table)
	if err != nil {
		t.Fatalf("load e2e.%s from the catalog: %v", table, err)
	}
	got, err := view.rows(ctx)
	if err != nil {
		t.Fatalf("read e2e.%s: %v", table, err)
	}
	if len(got) != len(want) {
		t.Fatalf("e2e.%s has %d rows, want %d", table, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("e2e.%s row %d = %+v, want %+v", table, i, got[i], want[i])
		}
	}

	files, err := view.dataFiles(ctx)
	if err != nil {
		t.Fatalf("list data files of e2e.%s: %v", table, err)
	}
	wantFiles = append([]string(nil), wantFiles...)
	sort.Strings(wantFiles)
	gotFiles := make([]string, 0, len(files))
	var records int64
	for _, f := range files {
		gotFiles = append(gotFiles, f.path)
		records += f.records
	}
	if strings.Join(gotFiles, "\n") != strings.Join(wantFiles, "\n") {
		t.Fatalf("e2e.%s data files differ from the accepted worker artifacts\n got: %v\nwant: %v", table, gotFiles, wantFiles)
	}
	if records != int64(len(want)) {
		t.Fatalf("e2e.%s data files hold %d records, want %d", table, records, len(want))
	}
	return view
}

// expectedRow mirrors the fixture in fixtures/postgres-init.sql.
func expectedRow(prefix string, id int64) exportedRow {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return exportedRow{ID: id, Name: fmt.Sprintf("%s-%d", prefix, id), CreatedAt: base.Add(time.Duration(id) * time.Minute)}
}

func expectedRows(prefix string, from, to int64, renamed map[int64]string) []exportedRow {
	out := make([]exportedRow, 0, to-from+1)
	for id := from; id <= to; id++ {
		row := expectedRow(prefix, id)
		if name, ok := renamed[id]; ok {
			row.Name = name
		}
		out = append(out, row)
	}
	return out
}

func insertEvents(from, to int64) string {
	return fmt.Sprintf(`INSERT INTO events (id, name, created_at)
SELECT g, 'event-' || g, timestamptz '2026-01-01 00:00:00+00' + g * interval '1 minute'
FROM generate_series(%d, %d) AS g`, from, to)
}

func (e *env) sourceExec(ctx context.Context, t *testing.T, statements ...string) {
	t.Helper()
	conn, err := pgx.Connect(ctx, e.sourceDSN)
	if err != nil {
		t.Fatalf("connect to the source: %v", err)
	}
	defer conn.Close(ctx)
	for _, stmt := range statements {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("source statement %q: %v", stmt, err)
		}
	}
}

// waitWorker waits until a worker has registered with the master, so the
// first run does not wait on worker start-up.
func (e *env) waitWorker(ctx context.Context) error {
	for {
		var workers []map[string]any
		if err := e.master.getJSON(ctx, "/api/v1/workers", &workers); err == nil && len(workers) > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("no worker registered with the master: %w", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func waitHTTP(ctx context.Context, url string) error {
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not reachable: %w", url, ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
