package orabbitcli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	managedProcessStateVersion = "orabbit.local-state.v1"
	managedProcessStateFile    = "orabbit-managed-processes.json"
)

var managedProcessStateMu sync.Mutex

type managedProcessState struct {
	Version    string                 `json:"version"`
	RuntimeDir string                 `json:"runtime_dir"`
	UpdatedAt  string                 `json:"updated_at"`
	Processes  []managedProcessRecord `json:"processes"`
}

type managedProcessRecord struct {
	PID        int    `json:"pid"`
	Kind       string `json:"kind"`
	Command    string `json:"command"`
	BinaryPath string `json:"binary_path"`
	HTTPAddr   string `json:"http_addr"`
	GRPCAddr   string `json:"grpc_addr"`
	DBPath     string `json:"db_path"`
	MasterAddr string `json:"master_addr"`
	Poll       string `json:"poll"`
	WorkerID   string `json:"worker_id"`
	Insecure   bool   `json:"insecure"`
	StartedAt  string `json:"started_at"`
}

func managedProcessStatePath(runtimeDir string) string {
	return filepath.Join(strings.TrimSpace(runtimeDir), managedProcessStateFile)
}

func listManagedProcesses(runtimeDir string) ([]procInfo, int, error) {
	managedProcessStateMu.Lock()
	defer managedProcessStateMu.Unlock()

	state, removed, err := pruneManagedProcessStateLocked(runtimeDir)
	if err != nil {
		return nil, 0, err
	}

	procs := make([]procInfo, 0, len(state.Processes))
	for _, record := range state.Processes {
		procs = append(procs, record.procInfo())
	}
	sort.Slice(procs, func(i, j int) bool { return procs[i].PID < procs[j].PID })
	return procs, removed, nil
}

func registerManagedProcess(runtimeDir string, proc procInfo, binaryPath string) error {
	managedProcessStateMu.Lock()
	defer managedProcessStateMu.Unlock()

	if strings.TrimSpace(runtimeDir) == "" {
		return fmt.Errorf("missing managed runtime dir")
	}
	if proc.PID <= 0 {
		return fmt.Errorf("invalid managed pid %d", proc.PID)
	}
	if strings.TrimSpace(proc.Kind) == "" {
		return fmt.Errorf("missing managed process kind")
	}

	state, _, err := pruneManagedProcessStateLocked(runtimeDir)
	if err != nil {
		return err
	}

	state.Processes = filterManagedProcesses(state.Processes, func(record managedProcessRecord) bool {
		return record.PID != proc.PID
	})
	state.Processes = append(state.Processes, managedProcessRecord{
		PID:        proc.PID,
		Kind:       strings.TrimSpace(proc.Kind),
		Command:    strings.TrimSpace(proc.Command),
		BinaryPath: strings.TrimSpace(binaryPath),
		HTTPAddr:   strings.TrimSpace(proc.HTTPAddr),
		GRPCAddr:   strings.TrimSpace(proc.GRPCAddr),
		DBPath:     strings.TrimSpace(proc.DBPath),
		MasterAddr: strings.TrimSpace(proc.MasterAddr),
		Poll:       strings.TrimSpace(proc.Poll),
		WorkerID:   strings.TrimSpace(proc.WorkerID),
		Insecure:   proc.Insecure,
		StartedAt:  time.Now().UTC().Format(time.RFC3339Nano),
	})
	sort.Slice(state.Processes, func(i, j int) bool { return state.Processes[i].PID < state.Processes[j].PID })
	return saveManagedProcessStateLocked(runtimeDir, state)
}

func unregisterManagedProcess(runtimeDir string, pid int) error {
	return unregisterManagedProcesses(runtimeDir, []int{pid})
}

func unregisterManagedProcesses(runtimeDir string, pids []int) error {
	managedProcessStateMu.Lock()
	defer managedProcessStateMu.Unlock()

	if strings.TrimSpace(runtimeDir) == "" || len(pids) == 0 {
		return nil
	}

	state, _, err := pruneManagedProcessStateLocked(runtimeDir)
	if err != nil {
		return err
	}

	remove := make(map[int]struct{}, len(pids))
	for _, pid := range pids {
		if pid > 0 {
			remove[pid] = struct{}{}
		}
	}
	if len(remove) == 0 {
		return nil
	}

	before := len(state.Processes)
	state.Processes = filterManagedProcesses(state.Processes, func(record managedProcessRecord) bool {
		_, drop := remove[record.PID]
		return !drop
	})
	if len(state.Processes) == before {
		return nil
	}
	return saveManagedProcessStateLocked(runtimeDir, state)
}

func pruneManagedProcessState(runtimeDir string) (int, error) {
	managedProcessStateMu.Lock()
	defer managedProcessStateMu.Unlock()

	_, removed, err := pruneManagedProcessStateLocked(runtimeDir)
	return removed, err
}

func pruneManagedProcessStateLocked(runtimeDir string) (managedProcessState, int, error) {
	state, err := loadManagedProcessStateLocked(runtimeDir)
	if err != nil {
		return managedProcessState{}, 0, err
	}

	live := make([]managedProcessRecord, 0, len(state.Processes))
	seen := make(map[int]struct{}, len(state.Processes))
	removed := 0
	for _, record := range state.Processes {
		if record.PID <= 0 || strings.TrimSpace(record.Kind) == "" {
			removed++
			continue
		}
		if _, ok := seen[record.PID]; ok {
			removed++
			continue
		}
		if !processAlive(record.PID) {
			removed++
			continue
		}
		seen[record.PID] = struct{}{}
		live = append(live, record)
	}
	if removed == 0 {
		return state, 0, nil
	}
	state.Processes = live
	if err := saveManagedProcessStateLocked(runtimeDir, state); err != nil {
		return managedProcessState{}, 0, err
	}
	return state, removed, nil
}

func loadManagedProcessStateLocked(runtimeDir string) (managedProcessState, error) {
	runtimeDir = strings.TrimSpace(runtimeDir)
	if runtimeDir == "" {
		return managedProcessState{}, fmt.Errorf("missing managed runtime dir")
	}

	state := managedProcessState{
		Version:    managedProcessStateVersion,
		RuntimeDir: runtimeDir,
		Processes:  []managedProcessRecord{},
	}
	raw, err := os.ReadFile(managedProcessStatePath(runtimeDir))
	if err != nil {
		if os.IsNotExist(err) {
			return state, nil
		}
		return managedProcessState{}, fmt.Errorf("read managed process state: %w", err)
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return state, nil
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return managedProcessState{}, fmt.Errorf("parse managed process state %q: %w", managedProcessStatePath(runtimeDir), err)
	}
	if strings.TrimSpace(state.Version) == "" {
		state.Version = managedProcessStateVersion
	}
	if strings.TrimSpace(state.RuntimeDir) == "" {
		state.RuntimeDir = runtimeDir
	}
	if state.Processes == nil {
		state.Processes = []managedProcessRecord{}
	}
	return state, nil
}

func saveManagedProcessStateLocked(runtimeDir string, state managedProcessState) error {
	runtimeDir = strings.TrimSpace(runtimeDir)
	if runtimeDir == "" {
		return fmt.Errorf("missing managed runtime dir")
	}
	if err := os.MkdirAll(runtimeDir, 0o755); err != nil {
		return fmt.Errorf("prepare managed runtime dir %q: %w", runtimeDir, err)
	}

	state.Version = managedProcessStateVersion
	state.RuntimeDir = runtimeDir
	state.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	if state.Processes == nil {
		state.Processes = []managedProcessRecord{}
	}

	path := managedProcessStatePath(runtimeDir)
	tmp, err := os.CreateTemp(runtimeDir, managedProcessStateFile+".tmp-*")
	if err != nil {
		return fmt.Errorf("create managed process state temp file: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		_ = tmp.Close()
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(state); err != nil {
		return fmt.Errorf("write managed process state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close managed process state temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("finalize managed process state %q: %w", path, err)
	}
	cleanup = false
	return nil
}

func filterManagedProcesses(records []managedProcessRecord, keep func(managedProcessRecord) bool) []managedProcessRecord {
	filtered := make([]managedProcessRecord, 0, len(records))
	for _, record := range records {
		if keep(record) {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

func (record managedProcessRecord) procInfo() procInfo {
	return procInfo{
		PID:        record.PID,
		Kind:       record.Kind,
		Command:    record.Command,
		HTTPAddr:   record.HTTPAddr,
		GRPCAddr:   record.GRPCAddr,
		DBPath:     record.DBPath,
		MasterAddr: record.MasterAddr,
		Poll:       record.Poll,
		WorkerID:   record.WorkerID,
		Insecure:   record.Insecure,
	}
}
