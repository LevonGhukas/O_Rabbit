package grpcapi

// Master load harness (issue #71, docs/master-scaling.md).
//
// It starts a real master in-process: a SQLite file with the durable
// leadership lease and write fence, the production lease, catalog and upload
// policies, the background loops of cmd/master, and the gRPC server with
// worker authentication on loopback. Simulated workers then follow the real
// worker protocol over gRPC (register, heartbeat, request a task, fetch its
// credentials, report progress, renew the lease, take upload capacity,
// report the result with verified artifacts). Object storage and the Iceberg
// catalog are in-memory fakes with configurable latency, so the results
// describe the master, not MinIO, the catalog or the source databases.
//
// Run all profiles:
//
//	ORABBIT_LOAD=1 go test ./internal/grpc -run TestMasterLoad -timeout 60m -v
//
// ORABBIT_LOAD_PROFILES=baseline,workers-64 selects profiles, and
// ORABBIT_LOAD_REPORT=<file> writes the JSON report.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// loadProfile is one load scenario. Zero values take production defaults.
type loadProfile struct {
	Name        string        `json:"name"`
	Workers     int           `json:"workers"`
	Runs        int           `json:"runs"`
	TasksPerRun int           `json:"tasks_per_run"`
	TaskWork    time.Duration `json:"task_work"`
	// ProgressEvery is how often a worker reports progress while working.
	ProgressEvery time.Duration `json:"progress_every"`
	// Poll is how long an idle worker waits before asking again.
	Poll           time.Duration `json:"poll"`
	ObjectLatency  time.Duration `json:"object_store_latency"`
	CatalogLatency time.Duration `json:"catalog_latency"`
	// SlowCatalogFor keeps the catalog at SlowCatalogLatency for this long
	// after the start, then returns it to CatalogLatency.
	SlowCatalogFor     time.Duration `json:"slow_catalog_for,omitempty"`
	SlowCatalogLatency time.Duration `json:"slow_catalog_latency,omitempty"`
	CatalogWorkLimit   int           `json:"catalog_work_limit"`
	MaxActiveTasks     int           `json:"max_active_tasks"`
	Deadline           time.Duration `json:"deadline"`
}

func (p loadProfile) withDefaults() loadProfile {
	def := func(v *time.Duration, d time.Duration) {
		if *v == 0 {
			*v = d
		}
	}
	defInt := func(v *int, d int) {
		if *v == 0 {
			*v = d
		}
	}
	def(&p.TaskWork, 2*time.Second)
	def(&p.ProgressEvery, time.Second)
	def(&p.Poll, time.Second)
	def(&p.ObjectLatency, 5*time.Millisecond)
	def(&p.CatalogLatency, 200*time.Millisecond)
	def(&p.Deadline, 15*time.Minute)
	defInt(&p.CatalogWorkLimit, 2) // ORABBIT_CATALOG_WORK_LIMIT
	defInt(&p.MaxActiveTasks, 64)  // ORABBIT_MAX_ACTIVE_TASKS
	return p
}

// loadProfiles grow the worker pool with the task count, so each worker does
// about the same work; the remaining profiles each stress one dimension.
var loadProfiles = []loadProfile{
	{Name: "smoke", Workers: 2, Runs: 2, TasksPerRun: 2, TaskWork: 200 * time.Millisecond, Deadline: time.Minute},
	{Name: "baseline", Workers: 8, Runs: 16, TasksPerRun: 8},
	{Name: "workers-32", Workers: 32, Runs: 32, TasksPerRun: 16},
	{Name: "workers-64", Workers: 64, Runs: 64, TasksPerRun: 16},
	{Name: "workers-128", Workers: 128, Runs: 64, TasksPerRun: 32},
	{Name: "workers-128-active-256", Workers: 128, Runs: 64, TasksPerRun: 32, MaxActiveTasks: 256},
	{Name: "workers-256-active-512", Workers: 256, Runs: 128, TasksPerRun: 32, MaxActiveTasks: 512},
	{Name: "progress-100ms", Workers: 64, Runs: 64, TasksPerRun: 16, ProgressEvery: 100 * time.Millisecond},
	{Name: "short-tasks", Workers: 64, Runs: 128, TasksPerRun: 32, TaskWork: 100 * time.Millisecond},
	{Name: "many-small-runs", Workers: 64, Runs: 512, TasksPerRun: 2, TaskWork: 500 * time.Millisecond},
	{Name: "catalog-2s", Workers: 32, Runs: 128, TasksPerRun: 2, TaskWork: 500 * time.Millisecond, CatalogLatency: 2 * time.Second},
	{Name: "catalog-2s-limit-8", Workers: 32, Runs: 128, TasksPerRun: 2, TaskWork: 500 * time.Millisecond, CatalogLatency: 2 * time.Second, CatalogWorkLimit: 8},
	{Name: "catalog-outage-60s", Workers: 32, Runs: 64, TasksPerRun: 4, TaskWork: 500 * time.Millisecond, SlowCatalogFor: 60 * time.Second, SlowCatalogLatency: 45 * time.Second},
}

func TestMasterLoad(t *testing.T) {
	if os.Getenv("ORABBIT_LOAD") != "1" {
		t.Skip("load harness: set ORABBIT_LOAD=1 (see docs/master-scaling.md)")
	}
	selected := map[string]bool{}
	for _, name := range strings.Split(os.Getenv("ORABBIT_LOAD_PROFILES"), ",") {
		if name = strings.TrimSpace(name); name != "" {
			selected[name] = true
		}
	}
	var results []loadResult
	for _, p := range loadProfiles {
		if len(selected) > 0 && !selected[p.Name] {
			continue
		}
		p := p.withDefaults()
		t.Run(p.Name, func(t *testing.T) {
			r := runLoadProfile(t, p)
			results = append(results, r)
			t.Log(r.summary())
			if len(r.RPCErrors) > 0 {
				t.Logf("%s RPC errors: %v", p.Name, r.RPCErrors)
			}
		})
	}
	t.Log("\n" + loadResultsTable(results))
	if path := os.Getenv("ORABBIT_LOAD_REPORT"); path != "" {
		body, err := json.MarshalIndent(map[string]any{
			"go":      runtime.Version(),
			"os_arch": runtime.GOOS + "/" + runtime.GOARCH,
			"cpus":    runtime.NumCPU(),
			"results": results,
		}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(body, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// loadResult is what one profile measured.
type loadResult struct {
	Profile loadProfile `json:"profile"`
	Tasks   int         `json:"tasks"`
	// Elapsed runs from the first task request until every run is
	// registered in the catalog.
	ElapsedSeconds        float64            `json:"elapsed_seconds"`
	TasksPerSecond        float64            `json:"tasks_per_second"`
	IdealSeconds          float64            `json:"ideal_seconds"`
	Efficiency            float64            `json:"efficiency"`
	PeakActiveTasks       int                `json:"peak_active_tasks"`
	RunsCommittedPerSec   float64            `json:"runs_committed_per_second"`
	CommitLatency         latency            `json:"commit_latency"`
	CatalogQueueAge       latency            `json:"catalog_queue_age"`
	PeakPendingCatalog    int                `json:"peak_pending_catalog"`
	CatalogDrainAfterSlow float64            `json:"catalog_drain_after_slow_seconds,omitempty"`
	TaskQueueWait         latency            `json:"task_queue_wait"`
	RPC                   map[string]latency `json:"rpc_latency"`
	RPCErrors             map[string]int     `json:"rpc_errors"`
	WriterWaitCount       int64              `json:"sqlite_writer_wait_count"`
	WriterWaitSeconds     float64            `json:"sqlite_writer_wait_seconds"`
	WriterBusyFraction    float64            `json:"sqlite_writer_wait_per_second"`
	BusyRetries           int64              `json:"sqlite_busy_retries"`
	RetriedAttempts       int                `json:"retried_task_attempts"`
	CPUSeconds            float64            `json:"process_cpu_seconds"`
	CPUCores              float64            `json:"process_cpu_cores"`
	PeakHeapMB            float64            `json:"peak_heap_mb"`
	PeakGoroutines        int                `json:"peak_goroutines"`
	DatabaseMB            float64            `json:"database_mb"`
	Correct               bool               `json:"all_runs_published"`
	Problems              []string           `json:"problems,omitempty"`
}

func (r loadResult) summary() string {
	return fmt.Sprintf("%s: %d tasks in %.1fs (ideal %.1fs, efficiency %.0f%%), peak active %d, RequestTask p99 %.0fms, progress p99 %.0fms, result p99 %.0fms, commit p95 %.0fms, catalog age max %.1fs, writer wait %.2fs/s, cpu %.2f cores, heap %.0fMB, ok=%t %v",
		r.Profile.Name, r.Tasks, r.ElapsedSeconds, r.IdealSeconds, 100*r.Efficiency, r.PeakActiveTasks,
		r.RPC["RequestTask"].P99, r.RPC["ReportTaskProgress"].P99, r.RPC["ReportTaskResult"].P99,
		r.CommitLatency.P95, r.CatalogQueueAge.Max/1000, r.WriterBusyFraction, r.CPUCores, r.PeakHeapMB, r.Correct, r.Problems)
}

func loadResultsTable(results []loadResult) string {
	var b strings.Builder
	b.WriteString("| profile | workers | tasks | elapsed s | efficiency | peak active | RequestTask p99 ms | progress p99 ms | result p99 ms | commit p95 ms | catalog age max s | writer wait s/s | CPU cores | heap MB | ok |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range results {
		fmt.Fprintf(&b, "| %s | %d | %d | %.1f | %.0f%% | %d | %.0f | %.0f | %.0f | %.0f | %.1f | %.2f | %.2f | %.0f | %t |\n",
			r.Profile.Name, r.Profile.Workers, r.Tasks, r.ElapsedSeconds, 100*r.Efficiency, r.PeakActiveTasks,
			r.RPC["RequestTask"].P99, r.RPC["ReportTaskProgress"].P99, r.RPC["ReportTaskResult"].P99,
			r.CommitLatency.P95, r.CatalogQueueAge.Max/1000, r.WriterBusyFraction, r.CPUCores, r.PeakHeapMB, r.Correct)
	}
	return b.String()
}

// latency summarizes samples in milliseconds.
type latency struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

type sampler struct {
	mu sync.Mutex
	v  []float64
}

func (s *sampler) add(d time.Duration) {
	s.mu.Lock()
	s.v = append(s.v, float64(d)/float64(time.Millisecond))
	s.mu.Unlock()
}

func (s *sampler) summary() latency {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.v) == 0 {
		return latency{}
	}
	v := append([]float64(nil), s.v...)
	sort.Float64s(v)
	at := func(q float64) float64 { return v[int(math.Ceil(q*float64(len(v))))-1] }
	return latency{Count: len(v), P50: at(0.50), P95: at(0.95), P99: at(0.99), Max: v[len(v)-1]}
}

// rpcStats times every worker RPC on the client side, including queueing in
// the master, and counts errors by method and status code.
type rpcStats struct {
	mu     sync.Mutex
	lat    map[string]*sampler
	errors map[string]int
}

func newRPCStats() *rpcStats {
	return &rpcStats{lat: map[string]*sampler{}, errors: map[string]int{}}
}

func (r *rpcStats) interceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		name := method[strings.LastIndex(method, "/")+1:]
		r.mu.Lock()
		s := r.lat[name]
		if s == nil {
			s = &sampler{}
			r.lat[name] = s
		}
		if err != nil {
			r.errors[name+":"+grpcstatus.Code(err).String()]++
		}
		r.mu.Unlock()
		s.add(time.Since(start))
		return err
	}
}

// memObjectStore is object storage shared by the simulated workers (who
// upload) and the master (who verifies and writes commit state), with a
// fixed latency per request.
type memObjectStore struct {
	latency time.Duration
	mu      sync.Mutex
	objects map[string][]byte
}

func (m *memObjectStore) wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(m.latency):
		return nil
	}
}

func (m *memObjectStore) get(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objects[key]
	return b, ok
}

func (m *memObjectStore) Head(ctx context.Context, key string) error {
	if err := m.wait(ctx); err != nil {
		return err
	}
	if _, ok := m.get(key); !ok {
		return fmt.Errorf("object %s not found", key)
	}
	return nil
}

func (m *memObjectStore) GetObjectBytes(ctx context.Context, key string) ([]byte, bool, error) {
	if err := m.wait(ctx); err != nil {
		return nil, false, err
	}
	b, ok := m.get(key)
	return append([]byte(nil), b...), ok, nil
}

func (m *memObjectStore) PutObjectBytes(ctx context.Context, key string, b []byte, _ string, _ map[string]string) error {
	if err := m.wait(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[key] = append([]byte(nil), b...)
	m.mu.Unlock()
	return nil
}

func (m *memObjectStore) OpenObject(ctx context.Context, key string) (io.ReadCloser, bool, error) {
	b, ok, err := m.GetObjectBytes(ctx, key)
	if err != nil || !ok {
		return nil, ok, err
	}
	return io.NopCloser(strings.NewReader(string(b))), true, nil
}

func (m *memObjectStore) ObjectChecksum(ctx context.Context, key string) (int64, string, bool, error) {
	if err := m.wait(ctx); err != nil {
		return 0, "", false, err
	}
	b, ok := m.get(key)
	if !ok {
		return 0, "", false, nil
	}
	sum := sha256.Sum256(b)
	return int64(len(b)), base64.StdEncoding.EncodeToString(sum[:]), true, nil
}

// latencyRegistrar plays the Iceberg catalog: it waits, then walks the same
// durable callbacks as a real registration.
type latencyRegistrar struct {
	latency func() time.Duration
	calls   atomic.Int64
}

func (r *latencyRegistrar) RegisterRun(ctx context.Context, req icebergreg.RunRequest) (icebergreg.RunResult, error) {
	r.calls.Add(1)
	if !req.CatalogAlreadyCommitted {
		if req.BeforeExternalCommit != nil {
			if err := req.BeforeExternalCommit(); err != nil {
				return icebergreg.RunResult{}, err
			}
		}
		select {
		case <-ctx.Done():
			return icebergreg.RunResult{}, ctx.Err()
		case <-time.After(r.latency()):
		}
		receipt, err := req.CatalogReceiptFactory()
		if err != nil {
			return icebergreg.RunResult{}, err
		}
		if err := req.CatalogCommitted(receipt); err != nil {
			return icebergreg.RunResult{}, err
		}
	}
	if err := req.IceStateWriting(); err != nil {
		return icebergreg.RunResult{}, err
	}
	return icebergreg.RunResult{Objects: len(req.ExactArtifacts)}, nil
}

// loadMaster is the master under test.
type loadMaster struct {
	st      *db.Store
	srv     *Server
	objects *memObjectStore
	addr    string
	token   string
	dbPath  string
}

func startLoadMaster(t *testing.T, ctx context.Context, p loadProfile, catalogLatency func() time.Duration) *loadMaster {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	dbPath := filepath.Join(t.TempDir(), "master.sqlite")
	st, err := db.Open(ctx, db.Config{Path: dbPath, ReadConns: 4}, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	st.SetMasterKey(testCryptoKey)
	st.SetMaxActiveRuns(p.Runs) // runs are seeded up front; admission is not under test

	// Leadership exactly as cmd/master: lease, write fence, renewal.
	lease, err := st.AcquireLeadership(ctx, "master-load", 15*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ActivateLeadershipFence(ctx, "master-load", lease.Epoch); err != nil {
		t.Fatal(err)
	}
	leadership, err := db.NewLeadershipController(st, lease, 15*time.Second, 5*time.Second, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	leaderCtx := leadership.Start(ctx)
	t.Cleanup(func() { leadership.Stop(context.Background()) })

	objects := &memObjectStore{latency: p.ObjectLatency, objects: map[string][]byte{}}
	srv := NewServer(log, st, nil, testCryptoKey, 5*time.Second, &latencyRegistrar{latency: catalogLatency})
	srv.newCommitObjectStoreFn = func(context.Context, s3io.Config) (commitObjectStore, error) { return objects, nil }
	// Production defaults from cmd/master/config.go.
	srv.SetLeasePolicy(db.LeasePolicy{Duration: 30 * time.Second, MaxAttempts: 3, MaxActiveTasks: p.MaxActiveTasks, BackoffBase: time.Second, BackoffMax: 30 * time.Second})
	srv.SetCatalogWorkLimit(p.CatalogWorkLimit)
	registration := db.RegistrationPolicy{LeaseDuration: 30 * time.Second, MaxAttempts: 5, BackoffBase: time.Second, BackoffMax: time.Minute}
	srv.SetCommitCatalogPolicy(CommitCatalogPolicy{CommitTimeout: 30 * time.Minute, CommitMaxAttempts: 5, RegistrationTimeout: 30 * time.Minute, Registration: registration, ReconciliationLease: 30 * time.Second, ReconciliationMaxAttempts: 5})
	srv.SetUploadCapacityPolicy(8, 2*time.Minute)
	leadership.SetReady(true)
	srv.SetLeadershipGuard(leadership)

	// The background loops of cmd/master/main.go that load reaches.
	every := func(interval time.Duration, tick func()) {
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-leaderCtx.Done():
					return
				case <-ticker.C:
					tick()
				}
			}
		}()
	}
	every(2*time.Second, func() { _ = srv.ReconcileCommittingRuns(leaderCtx) })
	every(2*time.Second, func() {
		for i := 0; i < 2; i++ {
			if ok, err := srv.ProcessReconciliationOnce(leaderCtx); err != nil || !ok {
				break
			}
		}
		for i := 0; i < 4; i++ {
			if ok, err := srv.ProcessRegistrationOnce(leaderCtx); err != nil || !ok {
				break
			}
		}
		_, _ = st.ExpireRegistrationAttempts(leaderCtx, time.Now(), registration)
		_, _ = st.ExpireReconciliationAttempts(leaderCtx, time.Now(), time.Second, 5)
	})
	every(5*time.Second, func() { _, _ = srv.ExpireLeases(leaderCtx) })

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	token := "load-token"
	go func() {
		_ = Serve(leaderCtx, lis, Config{Addr: lis.Addr().String(), Insecure: true, WorkerAuthToken: token, HeartbeatInterval: 5 * time.Second}, srv)
	}()
	return &loadMaster{st: st, srv: srv, objects: objects, addr: lis.Addr().String(), token: token, dbPath: dbPath}
}

// seedRuns creates runs as the planner leaves them: one incremental
// ordered-cursor job and dataset per run (so catalog registrations are not
// serialized on one dataset), with PENDING tasks and a rest-go registration
// snapshot.
func seedRuns(t *testing.T, ctx context.Context, st *db.Store, p loadProfile) []string {
	t.Helper()
	srcSecret, err := crypto.Encrypt(testCryptoKey, []byte(`{"dsn":"postgres://load:load@source:5432/load"}`), []byte("load-src"))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: "load-src", Name: "load-src", Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: srcSecret}); err != nil {
		t.Fatal(err)
	}
	registration, err := json.Marshal(map[string]any{"enabled": true, "engine": "rest-go", "table": "load.table", "uri": "http://catalog:8181"})
	if err != nil {
		t.Fatal(err)
	}
	runIDs := make([]string, 0, p.Runs)
	for i := 0; i < p.Runs; i++ {
		suffix := fmt.Sprintf("%04d", i)
		tgtID, jobID, runID := "load-tgt-"+suffix, "load-job-"+suffix, "load-run-"+suffix
		prefix := "load/" + suffix
		tgtSecret, err := crypto.Encrypt(testCryptoKey, []byte(`{"access_key_id":"a","secret_access_key":"b"}`), []byte(tgtID))
		if err != nil {
			t.Fatal(err)
		}
		meta := fmt.Sprintf(`{"endpoint":"http://minio:9000","region":"us-east-1","bucket":"lake","prefix":%q,"force_path_style":true}`, prefix)
		if err := st.CreateConnection(ctx, db.Connection{ID: tgtID, Name: tgtID, Kind: "target", Engine: "s3", MetadataJSON: []byte(meta), SecretEncBlob: tgtSecret}); err != nil {
			t.Fatal(err)
		}
		hwm := "id"
		options := fmt.Sprintf(`{"table":"public.t%s","partition_strategy":"ordered_cursor","cursor_column":"id","iceberg_registration":{"enabled":true,"engine":"rest-go","table":"load.t%s"}}`, suffix, suffix)
		if err := st.CreateJob(ctx, db.Job{ID: jobID, Name: jobID, SourceConnectionID: "load-src", TargetConnectionID: tgtID, SourceSQL: "select 1", TargetNamespace: "load", TargetTable: "t" + suffix, WriteMode: "append", Incremental: true, HWMColumn: &hwm, OptionsJSON: []byte(options)}); err != nil {
			t.Fatal(err)
		}
		if err := st.CreateRun(ctx, db.Run{ID: runID, JobID: jobID, DatasetKey: dataset.StorageKey("http://minio:9000", "lake", prefix), Status: "RUNNING", CorrelationID: "corr-" + runID, StartedAt: time.Now().UTC().Format(time.RFC3339Nano), RegistrationConfigJSON: registration}); err != nil {
			t.Fatal(err)
		}
		tasks := make([]db.TaskInsert, p.TasksPerRun)
		for j := range tasks {
			tasks[j] = db.TaskInsert{ID: fmt.Sprintf("%s-task-%03d", runID, j+1), RunID: runID, TaskIndex: j + 1, PartitionSpec: []byte(fmt.Sprintf(`{"type":"int_range","start":%d,"end":%d}`, j*1000, (j+1)*1000)), Status: "PENDING"}
		}
		if err := st.InsertTasks(ctx, tasks); err != nil {
			t.Fatal(err)
		}
		runIDs = append(runIDs, runID)
	}
	return runIDs
}

// simWorker follows the real worker protocol (cmd/worker) without reading a
// source: it "works" for TaskWork, uploads one small object and reports it.
type simWorker struct {
	id       string
	cp       grpcpb.ControlPlaneClient
	p        loadProfile
	objects  *memObjectStore
	queued   time.Time
	waits    *sampler
	active   *atomic.Int64
	peak     *atomic.Int64
	results  *sync.Map // run ID -> time of its last accepted result
	rejected *sync.Map // distinct reasons the master rejected a result
}

func (w *simWorker) run(ctx context.Context) {
	if _, err := w.cp.RegisterWorker(ctx, &grpcpb.RegisterWorkerRequest{WorkerId: w.id, Addr: w.id, CapabilitiesJson: `{}`}); err != nil {
		return
	}
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = w.cp.Heartbeat(ctx, &grpcpb.HeartbeatRequest{WorkerId: w.id, NowUnixMs: time.Now().UnixMilli()})
			}
		}
	}()
	for ctx.Err() == nil {
		res, err := w.cp.RequestTask(ctx, &grpcpb.RequestTaskRequest{WorkerId: w.id, CapabilitiesJson: `{}`, ProtocolVersion: WorkerProtocolVersion})
		if err != nil || res.GetTask().GetTaskId() == "" {
			sleepCtx(ctx, w.p.Poll)
			continue
		}
		w.waits.add(time.Since(w.queued))
		n := w.active.Add(1)
		for {
			peak := w.peak.Load()
			if n <= peak || w.peak.CompareAndSwap(peak, n) {
				break
			}
		}
		w.execute(ctx, res.GetTask())
		w.active.Add(-1)
	}
}

func (w *simWorker) execute(ctx context.Context, task *grpcpb.TaskAssignment) {
	taskID, runID, attemptID, token := task.TaskId, task.RunId, task.AttemptId, task.FencingToken
	if _, err := w.cp.GetTaskCredentials(ctx, &grpcpb.GetTaskCredentialsRequest{WorkerId: w.id, TaskId: taskID, AttemptId: attemptID, FencingToken: token}); err != nil {
		return
	}
	deadline := time.UnixMilli(task.LeaseDeadlineUnixMs)
	done := time.Now().Add(w.p.TaskWork)
	nextProgress := time.Now().Add(w.p.ProgressEvery)
	var rows int64
	for time.Now().Before(done) {
		next := done
		if nextProgress.Before(next) {
			next = nextProgress
		}
		renewAt := time.Now().Add(renewalDelay(time.Now(), deadline))
		if renewAt.Before(next) {
			next = renewAt
		}
		sleepCtx(ctx, time.Until(next))
		if ctx.Err() != nil {
			return
		}
		now := time.Now()
		if !now.Before(renewAt) {
			res, err := w.cp.RenewTaskLease(ctx, &grpcpb.RenewTaskLeaseRequest{WorkerId: w.id, TaskId: taskID, AttemptId: attemptID, FencingToken: token})
			if err != nil {
				return // ownership lost
			}
			deadline = time.UnixMilli(res.LeaseDeadlineUnixMs)
		}
		if !now.Before(nextProgress) {
			rows += 1000
			_, _ = w.cp.ReportTaskProgress(ctx, &grpcpb.ReportTaskProgressRequest{WorkerId: w.id, TaskId: taskID, RunId: runID, AttemptId: attemptID, FencingToken: token, RowsRead: rows, BytesRead: rows * 100, BytesWritten: rows * 20})
			nextProgress = now.Add(w.p.ProgressEvery)
		}
	}

	grant, err := w.cp.AcquireUploadCapacity(ctx, &grpcpb.AcquireUploadCapacityRequest{WorkerId: w.id, TaskId: taskID, AttemptId: attemptID, FencingToken: token})
	for err == nil && !grant.GetAcquired() {
		sleepCtx(ctx, time.Duration(max(grant.GetRetryAfterMs(), 250))*time.Millisecond)
		grant, err = w.cp.AcquireUploadCapacity(ctx, &grpcpb.AcquireUploadCapacityRequest{WorkerId: w.id, TaskId: taskID, AttemptId: attemptID, FencingToken: token})
	}
	if err != nil {
		return
	}
	body := make([]byte, 4096)
	_, _ = rand.Read(body)
	key := fmt.Sprintf("%s/_runs/run-%s/part-%06d-000.parquet", strings.TrimSuffix(task.S3Prefix, "/"), runID, task.TaskIndex)
	_ = w.objects.PutObjectBytes(ctx, key, body, "application/octet-stream", nil)
	_, _ = w.cp.ReleaseUploadCapacity(ctx, &grpcpb.ReleaseUploadCapacityRequest{WorkerId: w.id, TaskId: taskID, AttemptId: attemptID, LeaseId: grant.LeaseId, LeaseToken: grant.LeaseToken})

	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	maxHWM := fmt.Sprintf("%d", int64(task.TaskIndex)*1000)
	res, err := w.cp.ReportTaskResult(ctx, &grpcpb.ReportTaskResultRequest{
		WorkerId: w.id, TaskId: taskID, RunId: runID, AttemptId: attemptID, FencingToken: token,
		Status: "SUCCEEDED", RowsRead: rows + 1000, BytesWritten: int64(len(body)),
		ParquetObjectKeys: []string{key}, MaxHwmValue: maxHWM,
		Artifacts: []*grpcpb.ArtifactIntegrity{{
			ObjectKey: key, ByteSize: int64(len(body)), Sha256: digest, RowCount: rows + 1000,
			SchemaFingerprint: strings.Repeat("a", 64), RunId: runID, TaskId: taskID, AttemptId: attemptID,
			AttemptNumber: task.AttemptNumber, FormatVersion: artifact.FormatVersion,
			// As s3io reports an upload verified by the store's own SHA-256,
			// which lets the commit verify with a HEAD instead of a read.
			VerificationMethod: artifact.VerificationProvider, VerificationStatus: artifact.VerificationVerified,
			MaxHwm: maxHWM, ProviderChecksumSha256: base64.StdEncoding.EncodeToString(sum[:]),
		}},
	})
	if err == nil && res.GetAccepted() {
		w.results.Store(runID, time.Now())
		return
	}
	reason := res.GetMessage()
	if err != nil {
		reason = err.Error()
	}
	w.rejected.LoadOrStore(reason, true)
}

func sleepCtx(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// renewalDelay mirrors cmd/worker/task_lease.go: renew at a third of the
// remaining lease, between 1s and 10s.
func renewalDelay(now, deadline time.Time) time.Duration {
	remaining := deadline.Sub(now)
	interval := remaining / 3
	interval = min(max(interval, time.Second), 10*time.Second)
	return min(interval, max(remaining, 0))
}

func runLoadProfile(t *testing.T, p loadProfile) loadResult {
	ctx, cancel := context.WithTimeout(context.Background(), p.Deadline)
	defer cancel()

	var slowUntil time.Time
	catalogLatency := func() time.Duration {
		if time.Now().Before(slowUntil) {
			return p.SlowCatalogLatency
		}
		return p.CatalogLatency
	}
	m := startLoadMaster(t, ctx, p, catalogLatency)
	runIDs := seedRuns(t, ctx, m.st, p)
	result := loadResult{Profile: p, Tasks: p.Runs * p.TasksPerRun, RPC: map[string]latency{}, RPCErrors: map[string]int{}}

	stats := newRPCStats()
	waits := &sampler{}
	var active, peak atomic.Int64
	results, rejected := &sync.Map{}, &sync.Map{}
	workCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()

	poolBefore := m.st.PoolStats()
	cpuBefore := processCPU()
	start := time.Now()
	if p.SlowCatalogFor > 0 {
		slowUntil = start.Add(p.SlowCatalogFor)
	}
	var wg sync.WaitGroup
	for i := 0; i < p.Workers; i++ {
		conn, err := grpc.NewClient(m.addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(stats.interceptor(m.token)))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		w := &simWorker{id: fmt.Sprintf("load-worker-%03d", i), cp: grpcpb.NewControlPlaneClient(conn), p: p, objects: m.objects, queued: start, waits: waits, active: &active, peak: &peak, results: results, rejected: rejected}
		wg.Add(1)
		go func() { defer wg.Done(); w.run(workCtx) }()
	}

	// Watch the runs until every one is registered (or the deadline).
	committed := map[string]time.Time{}
	registered := map[string]time.Time{}
	commitLat, catalogAge := &sampler{}, &sampler{}
	var peakHeap uint64
	peakGoroutines, peakPending := 0, 0
	var drainedAt time.Time
	for len(registered) < len(runIDs) && ctx.Err() == nil {
		time.Sleep(200 * time.Millisecond)
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		peakHeap = max(peakHeap, ms.HeapInuse)
		peakGoroutines = max(peakGoroutines, runtime.NumGoroutine())
		pending := 0
		for _, id := range runIDs {
			if _, ok := registered[id]; ok {
				continue
			}
			run, err := m.st.GetRun(ctx, id)
			if err != nil {
				continue
			}
			if run.Status == "FAILED" || run.Status == "CANCELED" {
				result.Problems = append(result.Problems, fmt.Sprintf("run %s %s: %v", id, run.Status, deref(run.ErrorSummary)))
				registered[id] = time.Now() // stop watching it
				continue
			}
			if run.Status == "SUCCEEDED" && run.CommitPhase == "COMPLETE" {
				if _, ok := committed[id]; !ok {
					committed[id] = time.Now()
					if last, ok := results.Load(id); ok {
						commitLat.add(time.Since(last.(time.Time)))
					}
				}
				if run.CatalogStatus == "REGISTERED" {
					registered[id] = time.Now()
					catalogAge.add(registered[id].Sub(committed[id]))
				} else {
					pending++
				}
			}
		}
		peakPending = max(peakPending, pending)
		if p.SlowCatalogFor > 0 && time.Now().After(slowUntil) && pending == 0 && drainedAt.IsZero() && len(committed) > 0 {
			drainedAt = time.Now()
		}
	}
	elapsed := time.Since(start)
	stopWorkers()
	wg.Wait()

	poolAfter := m.st.PoolStats()
	cpu := processCPU() - cpuBefore
	result.ElapsedSeconds = elapsed.Seconds()
	result.TasksPerSecond = float64(result.Tasks) / elapsed.Seconds()
	concurrency := min(p.Workers, p.MaxActiveTasks)
	result.IdealSeconds = math.Ceil(float64(result.Tasks)/float64(concurrency)) * p.TaskWork.Seconds()
	result.Efficiency = result.IdealSeconds / result.ElapsedSeconds
	result.PeakActiveTasks = int(peak.Load())
	result.RunsCommittedPerSec = float64(len(committed)) / elapsed.Seconds()
	result.CommitLatency = commitLat.summary()
	result.CatalogQueueAge = catalogAge.summary()
	result.PeakPendingCatalog = peakPending
	if !drainedAt.IsZero() {
		result.CatalogDrainAfterSlow = drainedAt.Sub(slowUntil).Seconds()
	}
	result.TaskQueueWait = waits.summary()
	for name, s := range stats.lat {
		result.RPC[name] = s.summary()
	}
	for k, v := range stats.errors {
		result.RPCErrors[k] = v
	}
	result.WriterWaitCount = poolAfter.Writer.WaitCount - poolBefore.Writer.WaitCount
	result.WriterWaitSeconds = (poolAfter.Writer.WaitDuration - poolBefore.Writer.WaitDuration).Seconds()
	result.WriterBusyFraction = result.WriterWaitSeconds / elapsed.Seconds()
	result.BusyRetries = poolAfter.BusyRetries - poolBefore.BusyRetries
	result.CPUSeconds = cpu.Seconds()
	result.CPUCores = cpu.Seconds() / elapsed.Seconds()
	result.PeakHeapMB = float64(peakHeap) / (1 << 20)
	result.PeakGoroutines = peakGoroutines
	if fi, err := os.Stat(m.dbPath); err == nil {
		result.DatabaseMB = float64(fi.Size()) / (1 << 20)
	}

	// Saturation must not strand work: every run published, every task
	// done, and retries explained.
	for _, id := range runIDs {
		tasks, err := m.st.ListTasksForRun(context.Background(), id)
		if err != nil {
			result.Problems = append(result.Problems, err.Error())
			continue
		}
		for _, task := range tasks {
			if task.AttemptCount > 1 {
				result.RetriedAttempts++
			}
			if task.Status != "SUCCEEDED" {
				result.Problems = append(result.Problems, fmt.Sprintf("task %s %s", task.ID, task.Status))
			}
		}
	}
	if ctx.Err() != nil {
		result.Problems = append(result.Problems, fmt.Sprintf("deadline: %d of %d runs registered", len(registered), len(runIDs)))
	}
	var reasons []string
	rejected.Range(func(k, _ any) bool {
		reasons = append(reasons, fmt.Sprintf("result rejected: %v", k))
		return true
	})
	result.Problems = append(reasons, result.Problems...)
	if len(result.Problems) > 10 {
		result.Problems = append(result.Problems[:10], fmt.Sprintf("... and %d more", len(result.Problems)-10))
	}
	result.Correct = len(result.Problems) == 0
	if !result.Correct {
		t.Errorf("%s did not publish all runs: %v", p.Name, result.Problems)
	}
	return result
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// processCPU is user plus system CPU time of this process: the master and
// the simulated workers together.
func processCPU() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
