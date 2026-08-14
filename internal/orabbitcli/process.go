package orabbitcli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type procInfo struct {
	PID        int    `json:"pid"`
	Kind       string `json:"kind"` // master|worker
	Command    string `json:"command"`
	HTTPAddr   string `json:"http_addr,omitempty"`
	GRPCAddr   string `json:"grpc_addr,omitempty"`
	DBPath     string `json:"db_path,omitempty"`
	MasterAddr string `json:"master_addr,omitempty"`
	Poll       string `json:"poll,omitempty"`
	WorkerID   string `json:"worker_id,omitempty"`
	Insecure   bool   `json:"insecure"`
}

type stopTarget struct {
	Proc        procInfo
	MatchReason string
}

const (
	// Keep the published schema version stable for existing automation.
	stackStatusJSONSchemaVersion = "orabbit.ls.v1"
	stackStatusJSONSourceMaster  = "master_api"
	stackStatusJSONSourceManaged = "managed_state"
	stackStatusJSONSourceProcess = "process_scan"
)

type lsMasterStatus struct {
	PID      int    `json:"pid"`
	HTTPAddr string `json:"http_addr"`
	GRPCAddr string `json:"grpc_addr"`
	DBPath   string `json:"db_path"`
}

type lsWorkerStatus struct {
	ID            string          `json:"id"`
	Addr          string          `json:"addr"`
	Status        string          `json:"status"`
	LastHeartbeat string          `json:"last_heartbeat"`
	Capabilities  json.RawMessage `json:"capabilities_json"`
}

type stackStatusJSONReport struct {
	SchemaVersion string                `json:"schema_version"`
	Source        string                `json:"source"`
	HTTPBase      string                `json:"http_base"`
	Items         []stackStatusJSONItem `json:"items"`
}

type stackStatusJSONItem struct {
	Kind             string          `json:"kind"`
	PID              *int            `json:"pid"`
	Command          *string         `json:"command"`
	HTTPAddr         *string         `json:"http_addr"`
	GRPCAddr         *string         `json:"grpc_addr"`
	GRPCReady        *bool           `json:"grpc_ready"`
	DBPath           *string         `json:"db_path"`
	Addr             *string         `json:"addr"`
	Status           *string         `json:"status"`
	LastHeartbeat    *string         `json:"last_heartbeat"`
	CapabilitiesJSON json.RawMessage `json:"capabilities_json"`
	MasterAddr       *string         `json:"master_addr"`
	Poll             *string         `json:"poll"`
	WorkerID         *string         `json:"worker_id"`
	Insecure         *bool           `json:"insecure"`
}

// cmdStackStatus executes the stack status command workflow.
// It exists to keep command orchestration isolated from shared helpers.
func cmdStackStatus(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("stack status", flag.ContinueOnError)
	httpBase := fs.String("http-base", "http://127.0.0.1"+defaultHTTPAddr, "Master HTTP base to query (preferred)")
	gocache := fs.String("gocache", defaultGOCache, "Managed local daemon runtime dir to inspect first")
	jsonOut := fs.Bool("json", false, "Print JSON")
	verbose := fs.Bool("v", false, "Verbose output")
	if handled, code := parseCommandFlags(fs, args, renderStackStatusHelp); handled {
		return code
	}

	base := strings.TrimSuffix(strings.TrimSpace(*httpBase), "/")
	if normalizeHTTPBase(base) == localHTTPBase(defaultHTTPAddr) {
		supervisor := newLocalSupervisor(*gocache, nil)
		managedProcs, _, err := supervisor.listManagedProcesses()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return exitOperational
		}
		if len(managedProcs) > 0 {
			if *jsonOut {
				if err := printStackStatusJSON(os.Stdout, buildStackStatusJSONReportFromProcs(base, stackStatusJSONSourceManaged, managedProcs)); err != nil {
					fmt.Fprintln(os.Stderr, "failed to render JSON:", err)
					return exitOperational
				}
				return exitSuccess
			}
			printProcInfoList(managedProcs, *verbose)
			return exitSuccess
		}
	}
	if base != "" && checkHealth(ctx, base) {
		var status lsMasterStatus
		if err := httpJSON(ctx, http.MethodGet, base+"/status", nil, &status); err == nil {
			grpcHealthy := checkGRPCTCPHealth(ctx, status.GRPCAddr)

			var workers []lsWorkerStatus
			_ = httpJSON(ctx, http.MethodGet, base+"/workers", nil, &workers)

			if *jsonOut {
				if err := printStackStatusJSON(os.Stdout, buildStackStatusJSONReportFromAPI(base, status, grpcHealthy, workers)); err != nil {
					fmt.Fprintln(os.Stderr, "failed to render JSON:", err)
					return exitOperational
				}
				return exitSuccess
			}

			fmt.Printf("master pid=%d http=%s grpc=%s grpc_ready=%v db=%s\n", status.PID, status.HTTPAddr, status.GRPCAddr, grpcHealthy, status.DBPath)
			if len(workers) == 0 {
				fmt.Println("workers: none")
				return exitSuccess
			}
			fmt.Printf("workers: %d\n", len(workers))
			for _, w := range workers {
				fmt.Printf("worker id=%s status=%s last_heartbeat=%s addr=%s\n", w.ID, w.Status, w.LastHeartbeat, w.Addr)
				if *verbose && len(w.Capabilities) != 0 {
					fmt.Printf("  capabilities=%s\n", strings.TrimSpace(string(w.Capabilities)))
				}
			}
			return exitSuccess
		}
	}

	// Fallback: best-effort process scan (useful when master isn't reachable).
	procs, err := listMasterWorkerProcs()
	if err != nil {
		fmt.Fprintln(os.Stderr, "process listing unavailable:", err)
		return exitOperational
	}
	if *jsonOut {
		if err := printStackStatusJSON(os.Stdout, buildStackStatusJSONReportFromProcs(base, stackStatusJSONSourceProcess, procs)); err != nil {
			fmt.Fprintln(os.Stderr, "failed to render JSON:", err)
			return exitOperational
		}
		return exitSuccess
	}
	printProcInfoList(procs, *verbose)
	return exitSuccess
}

func buildStackStatusJSONReportFromAPI(base string, status lsMasterStatus, grpcHealthy bool, workers []lsWorkerStatus) stackStatusJSONReport {
	items := make([]stackStatusJSONItem, 0, 1+len(workers))
	items = append(items, stackStatusJSONItem{
		Kind:             "master",
		PID:              intPtr(status.PID),
		Command:          nil,
		HTTPAddr:         stringPtrOrNil(status.HTTPAddr),
		GRPCAddr:         stringPtrOrNil(status.GRPCAddr),
		GRPCReady:        boolPtr(grpcHealthy),
		DBPath:           stringPtrOrNil(status.DBPath),
		Addr:             nil,
		Status:           nil,
		LastHeartbeat:    nil,
		CapabilitiesJSON: nil,
		MasterAddr:       nil,
		Poll:             nil,
		WorkerID:         nil,
		Insecure:         nil,
	})
	for _, worker := range workers {
		items = append(items, stackStatusJSONItem{
			Kind:             "worker",
			PID:              nil,
			Command:          nil,
			HTTPAddr:         nil,
			GRPCAddr:         nil,
			GRPCReady:        nil,
			DBPath:           nil,
			Addr:             stringPtrOrNil(worker.Addr),
			Status:           stringPtrOrNil(worker.Status),
			LastHeartbeat:    stringPtrOrNil(worker.LastHeartbeat),
			CapabilitiesJSON: cloneRawJSON(worker.Capabilities),
			MasterAddr:       nil,
			Poll:             nil,
			WorkerID:         stringPtrOrNil(worker.ID),
			Insecure:         nil,
		})
	}
	return stackStatusJSONReport{
		SchemaVersion: stackStatusJSONSchemaVersion,
		Source:        stackStatusJSONSourceMaster,
		HTTPBase:      base,
		Items:         items,
	}
}

func buildStackStatusJSONReportFromProcs(base string, source string, procs []procInfo) stackStatusJSONReport {
	items := make([]stackStatusJSONItem, 0, len(procs))
	for _, proc := range procs {
		items = append(items, stackStatusJSONItem{
			Kind:             proc.Kind,
			PID:              intPtr(proc.PID),
			Command:          stringPtrOrNil(proc.Command),
			HTTPAddr:         stringPtrOrNil(proc.HTTPAddr),
			GRPCAddr:         stringPtrOrNil(proc.GRPCAddr),
			GRPCReady:        nil,
			DBPath:           stringPtrOrNil(proc.DBPath),
			Addr:             nil,
			Status:           nil,
			LastHeartbeat:    nil,
			CapabilitiesJSON: nil,
			MasterAddr:       stringPtrOrNil(proc.MasterAddr),
			Poll:             stringPtrOrNil(proc.Poll),
			WorkerID:         stringPtrOrNil(proc.WorkerID),
			Insecure:         boolPtr(proc.Insecure),
		})
	}
	return stackStatusJSONReport{
		SchemaVersion: stackStatusJSONSchemaVersion,
		Source:        strings.TrimSpace(source),
		HTTPBase:      base,
		Items:         items,
	}
}

func printStackStatusJSON(w io.Writer, report stackStatusJSONReport) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func cloneRawJSON(v json.RawMessage) json.RawMessage {
	if len(v) == 0 {
		return nil
	}
	copied := make(json.RawMessage, len(v))
	copy(copied, v)
	return copied
}

func intPtr(v int) *int {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func stringPtrOrNil(v string) *string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return &v
}

func printProcInfoList(procs []procInfo, verbose bool) {
	if len(procs) == 0 {
		fmt.Println("no master/worker processes found")
		return
	}
	for _, p := range procs {
		switch p.Kind {
		case "master":
			fmt.Printf("master pid=%d http=%s grpc=%s db=%s insecure=%v\n", p.PID, p.HTTPAddr, p.GRPCAddr, p.DBPath, p.Insecure)
		case "worker":
			fmt.Printf("worker pid=%d master=%s insecure=%v poll=%s worker_id=%s\n", p.PID, p.MasterAddr, p.Insecure, p.Poll, p.WorkerID)
		default:
			fmt.Printf("%s pid=%d\n", p.Kind, p.PID)
		}
		if verbose {
			fmt.Printf("  cmd=%s\n", p.Command)
		}
	}
}

func listMasterWorkerProcs() ([]procInfo, error) {
	out, err := exec.Command("ps", "-Ao", "pid=,command=").Output()
	if err != nil {
		return nil, fmt.Errorf("ps process scan unavailable (container/restricted env?): %w", err)
	}
	lines := strings.Split(string(out), "\n")
	res := make([]procInfo, 0, len(lines))
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		fields := strings.Fields(ln)
		if len(fields) < 2 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		if pid == os.Getpid() {
			continue
		}
		cmd := strings.Join(fields[1:], " ")

		if looksLikeWorker(cmd) {
			pi := procInfo{PID: pid, Kind: "worker", Command: cmd}
			pi.MasterAddr = extractFlagValue(cmd, "-master")
			pi.WorkerID = extractFlagValue(cmd, "-worker-id")
			pi.Poll = extractFlagValue(cmd, "-poll")
			pi.Insecure = parseBoolFlag(cmd, "-insecure", true)
			res = append(res, pi)
			continue
		}
		if looksLikeMaster(cmd) {
			pi := procInfo{PID: pid, Kind: "master", Command: cmd}
			pi.HTTPAddr = extractFlagValue(cmd, "-http-addr")
			pi.GRPCAddr = extractFlagValue(cmd, "-grpc-addr")
			pi.DBPath = extractFlagValue(cmd, "-db")
			pi.Insecure = parseBoolFlag(cmd, "-insecure", true)
			res = append(res, pi)
			continue
		}
	}
	sort.Slice(res, func(i, j int) bool { return res[i].PID < res[j].PID })
	return res, nil
}

func looksLikeWorker(cmd string) bool {
	if hasArgBase(cmd, "worker") {
		return true
	}
	// Fallback: if a process has a -master flag, it is very likely our worker.
	return strings.Contains(cmd, "-master ") || strings.Contains(cmd, "-master=") ||
		strings.Contains(cmd, "--master ") || strings.Contains(cmd, "--master=")
}

func looksLikeMaster(cmd string) bool {
	if hasArgBase(cmd, "master") {
		return true
	}
	// Fallback: master always listens, so it has at least one of these flags.
	return strings.Contains(cmd, "-grpc-addr") || strings.Contains(cmd, "--grpc-addr") ||
		strings.Contains(cmd, "-http-addr") || strings.Contains(cmd, "--http-addr")
}

func hasArgBase(cmd, want string) bool {
	for _, tok := range strings.Fields(cmd) {
		if isSourceCommandPath(tok, want) {
			continue
		}
		base := filepath.Base(tok)
		if base == want || base == want+".exe" || base == "orabbit-"+want || base == "orabbit-"+want+".exe" {
			return true
		}
	}
	return false
}

func isSourceCommandPath(tok, want string) bool {
	clean := filepath.Clean(strings.TrimSpace(tok))
	if clean == "" || clean == "." {
		return false
	}
	return strings.HasSuffix(clean, filepath.Join("cmd", want)) ||
		strings.HasSuffix(clean, filepath.Join("cmd", "orabbit-"+want))
}

var flagValueRe = regexp.MustCompile(`(?:^|\s)(--?[A-Za-z0-9_-]+)(?:=|\s+)(\S+)`)

func extractFlagValue(cmd, flag string) string {
	if flag == "" {
		return ""
	}
	for _, m := range flagValueRe.FindAllStringSubmatch(cmd, -1) {
		if len(m) != 3 {
			continue
		}
		if m[1] == flag || m[1] == "--"+strings.TrimPrefix(flag, "-") {
			return strings.TrimSpace(m[2])
		}
	}
	return ""
}

func parseBoolFlag(cmd, flag string, def bool) bool {
	v := extractFlagValue(cmd, flag)
	if v == "" {
		return def
	}
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "true" || v == "1" || v == "y" || v == "yes" {
		return true
	}
	if v == "false" || v == "0" || v == "n" || v == "no" {
		return false
	}
	return def
}

// cmdStop executes the stop command workflow.
// It exists to keep command orchestration isolated from shared helpers.
func cmdStop(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("stack stop", flag.ContinueOnError)
	gocache := fs.String("gocache", defaultGOCache, fmt.Sprintf("Managed runtime dir used by %s", CLIName))
	force := fs.Bool("force", false, "Send SIGKILL (immediate)")
	all := fs.Bool("all", false, "Also stop master/worker processes not started with the provided -gocache (Unix only)")
	grpcAddr := fs.String("grpc-addr", "127.0.0.1"+defaultGRPCAddr, "Master gRPC address to match when -all is enabled (workers use -master <addr>)")
	httpAddr := fs.String("http-addr", defaultHTTPAddr, "Master HTTP listen addr to match when -all is enabled (masters use -http-addr <addr>)")
	dryRun := fs.Bool("dry-run", false, "Preview the exact matched targets; don't signal")
	if handled, code := parseCommandFlags(fs, args, renderStackStopHelp); handled {
		return code
	}

	targets, err := findStopTargets(strings.TrimSpace(*gocache), defaultGOCacheMarkers(*gocache), *all, strings.TrimSpace(*grpcAddr), strings.TrimSpace(*httpAddr))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitOperational
	}

	if len(targets) == 0 {
		if *all {
			fmt.Println("no orabbit master/worker processes matched the explicit broad scan")
			return exitSuccess
		}
		fmt.Printf("no CLI-managed orabbit master/worker processes were found in managed state or legacy managed-runtime markers under %q\n", strings.TrimSpace(*gocache))
		return exitSuccess
	}

	if *dryRun {
		action := "stop"
		if *force {
			action = "kill"
		}
		fmt.Printf("dry run: would %s %d process(es):\n", action, len(targets))
		printStopTargets(os.Stdout, targets)
		return exitSuccess
	}

	supervisor := newLocalSupervisor(*gocache, nil)
	action, err := supervisor.stopTargets(ctx, targets, *force, 2*time.Second)
	if code := exitCode(err); code == exitInterrupted {
		return code
	}
	fmt.Printf("%s %d processes\n", action, len(targets))
	return exitSuccess
}

func containsFlagValue(cmd, flag, value string) bool {
	if flag == "" || value == "" {
		return false
	}
	// Accept both -flag value and -flag=value. Go flags also accept --flag.
	if strings.Contains(cmd, flag+" "+value) || strings.Contains(cmd, flag+"="+value) {
		return true
	}
	if strings.HasPrefix(flag, "-") {
		ff := "-" + strings.TrimPrefix(flag, "-")
		long := "--" + strings.TrimPrefix(flag, "-")
		if strings.Contains(cmd, long+" "+value) || strings.Contains(cmd, long+"="+value) {
			return true
		}
		if strings.Contains(cmd, ff+" "+value) || strings.Contains(cmd, ff+"="+value) {
			return true
		}
	}
	return false
}

func defaultGOCacheMarkers(gocache string) []string {
	marker := strings.TrimRight(strings.TrimSpace(gocache), "/")
	if marker == "" {
		return nil
	}
	return []string{marker + "/"}
}

func commandContainsAny(cmd string, substrs []string) bool {
	for _, substr := range substrs {
		if strings.TrimSpace(substr) != "" && strings.Contains(cmd, substr) {
			return true
		}
	}
	return false
}

func findStopTargets(runtimeDir string, gocacheMarkers []string, all bool, grpcAddr, httpAddr string) ([]stopTarget, error) {
	// For safety, default behavior only matches processes started with the provided GOCACHE.
	// With -all, also match workers by "-master <grpcAddr>" and masters by "-grpc-addr"/"-http-addr".
	//
	// Process-list scanning relies on `ps`; containerized/restricted deployments may hide
	// other users' processes or disallow command visibility. Treat matches as best-effort.
	if runtime.GOOS == "windows" && all {
		all = false
	}

	stateTargets, err := findManagedStateStopTargets(runtimeDir)
	if err != nil {
		return nil, err
	}
	if !all && len(stateTargets) > 0 {
		return stateTargets, nil
	}

	procs, err := listMasterWorkerProcs()
	if err != nil {
		if len(stateTargets) > 0 {
			return stateTargets, nil
		}
		return nil, err
	}
	heuristicTargets := matchStopTargets(procs, gocacheMarkers, all, grpcAddr, httpAddr)
	if len(stateTargets) == 0 {
		return heuristicTargets, nil
	}
	return mergeStopTargets(stateTargets, heuristicTargets), nil
}

func findManagedStateStopTargets(runtimeDir string) ([]stopTarget, error) {
	supervisor := newLocalSupervisor(runtimeDir, nil)
	procs, _, err := supervisor.listManagedProcesses()
	if err != nil {
		return nil, err
	}
	targets := make([]stopTarget, 0, len(procs))
	for _, proc := range procs {
		targets = append(targets, stopTarget{Proc: proc, MatchReason: "state"})
	}
	return targets, nil
}

func mergeStopTargets(primary []stopTarget, extra []stopTarget) []stopTarget {
	merged := make([]stopTarget, 0, len(primary)+len(extra))
	seen := make(map[int]struct{}, len(primary)+len(extra))
	for _, target := range append(append([]stopTarget(nil), primary...), extra...) {
		if _, ok := seen[target.Proc.PID]; ok {
			continue
		}
		seen[target.Proc.PID] = struct{}{}
		merged = append(merged, target)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Proc.PID < merged[j].Proc.PID })
	return merged
}

func matchStopTargets(procs []procInfo, gocacheMarkers []string, all bool, grpcAddr, httpAddr string) []stopTarget {
	seen := map[int]struct{}{}
	targets := make([]stopTarget, 0, len(procs))
	for _, proc := range procs {
		if _, ok := seen[proc.PID]; ok {
			continue
		}

		if commandContainsAny(proc.Command, gocacheMarkers) {
			seen[proc.PID] = struct{}{}
			targets = append(targets, stopTarget{Proc: proc, MatchReason: "gocache"})
			continue
		}
		if !all {
			continue
		}

		if grpcAddr != "" && proc.Kind == "worker" && containsFlagValue(proc.Command, "-master", grpcAddr) {
			seen[proc.PID] = struct{}{}
			targets = append(targets, stopTarget{Proc: proc, MatchReason: "all:master-addr"})
			continue
		}

		grpcListen := normalizeListenAddr(grpcAddr)
		httpListen := normalizeListenAddr(httpAddr)
		if proc.Kind == "master" && hasArgBase(proc.Command, "master") && ((grpcListen != "" && (containsFlagValue(proc.Command, "-grpc-addr", grpcListen) || containsFlagValue(proc.Command, "-grpc-addr", grpcAddr))) || (httpListen != "" && (containsFlagValue(proc.Command, "-http-addr", httpListen) || containsFlagValue(proc.Command, "-http-addr", httpAddr)))) {
			seen[proc.PID] = struct{}{}
			targets = append(targets, stopTarget{Proc: proc, MatchReason: "all:listen-addr"})
			continue
		}
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].Proc.PID < targets[j].Proc.PID })
	return targets
}

func stopTargetPIDs(targets []stopTarget) []int {
	pids := make([]int, 0, len(targets))
	for _, target := range targets {
		pids = append(pids, target.Proc.PID)
	}
	return pids
}

func printStopTargets(w io.Writer, targets []stopTarget) {
	for _, target := range targets {
		switch target.Proc.Kind {
		case "master":
			fmt.Fprintf(w, "master pid=%d match=%s http=%s grpc=%s db=%s insecure=%v\n", target.Proc.PID, target.MatchReason, target.Proc.HTTPAddr, target.Proc.GRPCAddr, target.Proc.DBPath, target.Proc.Insecure)
		case "worker":
			fmt.Fprintf(w, "worker pid=%d match=%s master=%s poll=%s worker_id=%s insecure=%v\n", target.Proc.PID, target.MatchReason, target.Proc.MasterAddr, target.Proc.Poll, target.Proc.WorkerID, target.Proc.Insecure)
		default:
			fmt.Fprintf(w, "%s pid=%d match=%s\n", target.Proc.Kind, target.Proc.PID, target.MatchReason)
		}
		fmt.Fprintf(w, "  cmd=%s\n", target.Proc.Command)
	}
}

// signalPIDs handles signal pi ds behavior.
// It exists to keep this logic isolated and reusable.
func signalPIDs(pids []int, sig os.Signal) error {
	for _, pid := range pids {
		p, err := os.FindProcess(pid)
		if err != nil {
			continue
		}
		_ = p.Signal(sig)
	}
	return nil
}

// waitPIDsExit blocks until pi ds exit or timeout.
// It exists to make startup and coordination timing deterministic.
func waitPIDsExit(ctx context.Context, pids []int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		alive := 0
		for _, pid := range pids {
			if processAlive(pid) {
				alive++
			}
		}
		if alive == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for exit")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// processAlive handles process alive behavior.
// It exists to keep this logic isolated and reusable.
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil
}
