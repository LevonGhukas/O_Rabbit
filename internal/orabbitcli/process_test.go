package orabbitcli

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestMatchStopTargets_DefaultModeUsesOnlyGOCacheMarkers(t *testing.T) {
	procs := []procInfo{
		{
			PID:      101,
			Kind:     "master",
			Command:  "/tmp/orabbit-client-gocache/orabbit-master -http-addr :9100 -grpc-addr :9102 -db ./master.sqlite",
			HTTPAddr: ":9100",
			GRPCAddr: ":9102",
			DBPath:   "./master.sqlite",
			Insecure: true,
		},
		{
			PID:        202,
			Kind:       "worker",
			Command:    "orabbit-worker -master 127.0.0.1:9102 -worker-id local-01",
			MasterAddr: "127.0.0.1:9102",
			WorkerID:   "local-01",
			Insecure:   true,
		},
	}

	targets := matchStopTargets(procs, []string{"/tmp/orabbit-client-gocache/"}, false, "127.0.0.1:9102", ":9100")
	if len(targets) != 1 {
		t.Fatalf("expected 1 target in conservative mode, got %d", len(targets))
	}
	if targets[0].Proc.PID != 101 {
		t.Fatalf("expected only CLI-managed process to match, got pid=%d", targets[0].Proc.PID)
	}
	if targets[0].MatchReason != "gocache" {
		t.Fatalf("expected gocache match reason, got %q", targets[0].MatchReason)
	}
}

func TestMatchStopTargets_AllModeAddsExplicitBroadMatches(t *testing.T) {
	procs := []procInfo{
		{
			PID:      101,
			Kind:     "master",
			Command:  "/tmp/orabbit-client-gocache/orabbit-master -http-addr :9100 -grpc-addr :9102 -db ./master.sqlite",
			HTTPAddr: ":9100",
			GRPCAddr: ":9102",
			DBPath:   "./master.sqlite",
			Insecure: true,
		},
		{
			PID:        202,
			Kind:       "worker",
			Command:    "orabbit-worker -master 127.0.0.1:9102 -worker-id local-01",
			MasterAddr: "127.0.0.1:9102",
			WorkerID:   "local-01",
			Insecure:   true,
		},
	}

	targets := matchStopTargets(procs, []string{"/tmp/orabbit-client-gocache/"}, true, "127.0.0.1:9102", ":9100")
	if len(targets) != 2 {
		t.Fatalf("expected 2 targets in explicit broad mode, got %d", len(targets))
	}
	if targets[1].Proc.PID != 202 {
		t.Fatalf("expected broad worker match to be included, got pid=%d", targets[1].Proc.PID)
	}
	if targets[1].MatchReason != "all:master-addr" {
		t.Fatalf("expected explicit broad match reason, got %q", targets[1].MatchReason)
	}
}

func TestPrintStopTargetsIncludesIdentifyingMetadata(t *testing.T) {
	targets := []stopTarget{
		{
			Proc: procInfo{
				PID:        202,
				Kind:       "worker",
				Command:    "orabbit-worker -master 127.0.0.1:9102 -worker-id local-01",
				MasterAddr: "127.0.0.1:9102",
				WorkerID:   "local-01",
				Poll:       "200ms",
				Insecure:   true,
			},
			MatchReason: "all:master-addr",
		},
	}

	var buf bytes.Buffer
	printStopTargets(&buf, targets)
	out := buf.String()
	for _, want := range []string{
		"worker pid=202",
		"match=all:master-addr",
		"master=127.0.0.1:9102",
		"worker_id=local-01",
		"cmd=orabbit-worker -master 127.0.0.1:9102 -worker-id local-01",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("expected output to contain %q, got %q", want, out)
		}
	}
}

func TestMatchStopTargets_AllModeDoesNotTreatClientCommandAsMaster(t *testing.T) {
	procs := []procInfo{
		{
			PID:      303,
			Kind:     "master",
			Command:  "go run ./cmd/orabbit-client stack stop --all --grpc-addr 127.0.0.1:9102 --dry-run",
			Insecure: true,
		},
	}

	targets := matchStopTargets(procs, nil, true, "127.0.0.1:9102", ":9100")
	if len(targets) != 0 {
		t.Fatalf("expected client command to be ignored by broad master matching, got %+v", targets)
	}
}

func TestLooksLikeMasterIgnoresSourcePackagePaths(t *testing.T) {
	if looksLikeMaster("go build ./cmd/master") {
		t.Fatalf("expected go build package path not to look like a running master")
	}
	if looksLikeMaster("go run ./cmd/master") {
		t.Fatalf("expected go run package path not to look like a running master")
	}
}

func TestLooksLikeWorkerIgnoresSourcePackagePaths(t *testing.T) {
	if looksLikeWorker("go build ./cmd/worker") {
		t.Fatalf("expected go build package path not to look like a running worker")
	}
	if looksLikeWorker("go run ./cmd/worker") {
		t.Fatalf("expected go run package path not to look like a running worker")
	}
}

func TestCmdStackStatusJSONUsesStableSchemaForMasterAPI(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/status":
			_ = json.NewEncoder(w).Encode(lsMasterStatus{
				PID:      123,
				HTTPAddr: "127.0.0.1:19100",
				GRPCAddr: "127.0.0.1:65535",
				DBPath:   "./master.sqlite",
			})
		case "/workers":
			_ = json.NewEncoder(w).Encode([]lsWorkerStatus{
				{
					ID:            "local-01",
					Addr:          "127.0.0.1:19201",
					Status:        "READY",
					LastHeartbeat: "2026-04-06T12:00:00Z",
					Capabilities:  json.RawMessage(`{"slots":4}`),
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	code, stdout, stderr := captureCommandOutput(t, func() int {
		return cmdStackStatus(context.Background(), []string{"--http-base", srv.URL, "--json"})
	})
	if code != exitSuccess {
		t.Fatalf("cmdStackStatus exit=%d want=%d stderr=%q", code, exitSuccess, stderr)
	}
	if strings.TrimSpace(stderr) != "" {
		t.Fatalf("expected empty stderr, got %q", stderr)
	}

	var report stackStatusJSONReport
	if err := json.Unmarshal([]byte(stdout), &report); err != nil {
		t.Fatalf("unmarshal stdout: %v\nstdout=%s", err, stdout)
	}
	if report.SchemaVersion != stackStatusJSONSchemaVersion {
		t.Fatalf("schema_version=%q want=%q", report.SchemaVersion, stackStatusJSONSchemaVersion)
	}
	if report.Source != stackStatusJSONSourceMaster {
		t.Fatalf("source=%q want=%q", report.Source, stackStatusJSONSourceMaster)
	}
	if report.HTTPBase != srv.URL {
		t.Fatalf("http_base=%q want=%q", report.HTTPBase, srv.URL)
	}
	if len(report.Items) != 2 {
		t.Fatalf("items=%d want=2", len(report.Items))
	}

	master := report.Items[0]
	if master.Kind != "master" {
		t.Fatalf("master kind=%q want=master", master.Kind)
	}
	if master.PID == nil || *master.PID != 123 {
		t.Fatalf("master pid=%v want=123", master.PID)
	}
	if master.HTTPAddr == nil || *master.HTTPAddr != "127.0.0.1:19100" {
		t.Fatalf("master http_addr=%v", master.HTTPAddr)
	}
	if master.GRPCAddr == nil || *master.GRPCAddr != "127.0.0.1:65535" {
		t.Fatalf("master grpc_addr=%v", master.GRPCAddr)
	}
	if master.GRPCReady == nil || *master.GRPCReady {
		t.Fatalf("master grpc_ready=%v want=false", master.GRPCReady)
	}
	if master.Command != nil || master.Status != nil || master.MasterAddr != nil {
		t.Fatalf("expected API master-only fields to stay null, got %+v", master)
	}

	worker := report.Items[1]
	if worker.Kind != "worker" {
		t.Fatalf("worker kind=%q want=worker", worker.Kind)
	}
	if worker.WorkerID == nil || *worker.WorkerID != "local-01" {
		t.Fatalf("worker worker_id=%v", worker.WorkerID)
	}
	if worker.Addr == nil || *worker.Addr != "127.0.0.1:19201" {
		t.Fatalf("worker addr=%v", worker.Addr)
	}
	if worker.Status == nil || *worker.Status != "READY" {
		t.Fatalf("worker status=%v", worker.Status)
	}
	if worker.LastHeartbeat == nil || *worker.LastHeartbeat != "2026-04-06T12:00:00Z" {
		t.Fatalf("worker last_heartbeat=%v", worker.LastHeartbeat)
	}
	var capabilities map[string]any
	if err := json.Unmarshal(worker.CapabilitiesJSON, &capabilities); err != nil {
		t.Fatalf("unmarshal capabilities_json: %v", err)
	}
	if capabilities["slots"] != float64(4) {
		t.Fatalf("worker capabilities_json=%v", capabilities)
	}
	if worker.PID != nil || worker.Command != nil || worker.Insecure != nil {
		t.Fatalf("expected API worker process-only fields to stay null, got %+v", worker)
	}
}

func TestStackStatusJSONSchemaKeysStayStableAcrossSources(t *testing.T) {
	t.Parallel()

	apiReport := buildStackStatusJSONReportFromAPI("http://127.0.0.1:9100", lsMasterStatus{
		PID:      111,
		HTTPAddr: "127.0.0.1:9100",
		GRPCAddr: "127.0.0.1:9102",
		DBPath:   "./master.sqlite",
	}, true, []lsWorkerStatus{
		{
			ID:            "local-01",
			Addr:          "127.0.0.1:9201",
			Status:        "READY",
			LastHeartbeat: "2026-04-06T12:00:00Z",
			Capabilities:  json.RawMessage(`{"slots":2}`),
		},
	})
	procReport := buildStackStatusJSONReportFromProcs("http://127.0.0.1:9100", stackStatusJSONSourceProcess, []procInfo{
		{
			PID:      111,
			Kind:     "master",
			Command:  "/tmp/orabbit-client-gocache/orabbit-master -http-addr :9100 -grpc-addr :9102 -db ./master.sqlite",
			HTTPAddr: ":9100",
			GRPCAddr: ":9102",
			DBPath:   "./master.sqlite",
			Insecure: true,
		},
		{
			PID:        222,
			Kind:       "worker",
			Command:    "/tmp/orabbit-client-gocache/orabbit-worker -master 127.0.0.1:9102 -poll 200ms -worker-id local-01",
			MasterAddr: "127.0.0.1:9102",
			Poll:       "200ms",
			WorkerID:   "local-01",
			Insecure:   true,
		},
	})

	apiTop := marshalToMap(t, apiReport)
	procTop := marshalToMap(t, procReport)
	if !reflect.DeepEqual(sortedMapKeys(apiTop), sortedMapKeys(procTop)) {
		t.Fatalf("top-level keys differ: api=%v proc=%v", sortedMapKeys(apiTop), sortedMapKeys(procTop))
	}

	apiItems, ok := apiTop["items"].([]any)
	if !ok || len(apiItems) != 2 {
		t.Fatalf("api items malformed: %#v", apiTop["items"])
	}
	procItems, ok := procTop["items"].([]any)
	if !ok || len(procItems) != 2 {
		t.Fatalf("proc items malformed: %#v", procTop["items"])
	}

	for i := range apiItems {
		apiItem, ok := apiItems[i].(map[string]any)
		if !ok {
			t.Fatalf("api item %d malformed: %#v", i, apiItems[i])
		}
		procItem, ok := procItems[i].(map[string]any)
		if !ok {
			t.Fatalf("proc item %d malformed: %#v", i, procItems[i])
		}
		if !reflect.DeepEqual(sortedMapKeys(apiItem), sortedMapKeys(procItem)) {
			t.Fatalf("item %d keys differ: api=%v proc=%v", i, sortedMapKeys(apiItem), sortedMapKeys(procItem))
		}
	}
}

func marshalToMap(t *testing.T, v any) map[string]any {
	t.Helper()

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func sortedMapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
