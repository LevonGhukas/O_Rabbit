package orabbitcli

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"time"
)

type localWorkerConfig struct {
	GOCache         string
	HTTPBase        string
	GRPCAddr        string
	WorkerBin       string
	WorkerLogLevel  string
	WorkerLogFormat string
}

// exeSuffix handles exe suffix behavior.
// It exists to keep this logic isolated and reusable.
func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

// ensureLocalWorkers ensures local workers exists before continuing.
// It exists to fail early on missing runtime prerequisites.
func ensureLocalWorkers(ctx context.Context, cfg localWorkerConfig, desired int, supervisor *localSupervisor, pg *procGroup, managed bool) error {
	if desired < 1 {
		desired = 1
	}
	if supervisor == nil {
		supervisor = newLocalSupervisor(cfg.GOCache, nil)
	}
	workerBin, err := resolveManagedDaemonBinary(workerBinarySpec, cfg.WorkerBin, cfg.GOCache)
	if err != nil {
		return err
	}

	managedProcs, _, err := supervisor.listManagedProcesses()
	if err != nil {
		return err
	}

	// Prefer querying the master for active workers. This avoids accidentally starting
	// duplicate worker processes (same worker_id) if process scanning is incomplete.
	type worker struct {
		ID string `json:"id"`
	}
	var ws []worker
	_ = httpJSON(ctx, http.MethodGet, strings.TrimSuffix(cfg.HTTPBase, "/")+"/workers", nil, &ws)
	existing := map[string]struct{}{}
	for _, proc := range managedProcs {
		if proc.Kind != "worker" {
			continue
		}
		if strings.TrimSpace(proc.MasterAddr) != strings.TrimSpace(cfg.GRPCAddr) {
			continue
		}
		if _, ok := parseLocalWorkerNum(proc.WorkerID); !ok {
			continue
		}
		existing[proc.WorkerID] = struct{}{}
	}
	for _, w := range ws {
		if _, ok := parseLocalWorkerNum(w.ID); !ok {
			continue
		}
		existing[w.ID] = struct{}{}
	}

	for i := 1; i <= desired; i++ {
		id := fmt.Sprintf("local-%02d", i)
		if _, ok := existing[id]; ok {
			if supervisor.out != nil {
				supervisor.out.debugf("worker %s already registered with %s", id, cfg.HTTPBase)
			}
			continue
		}
		if supervisor.out != nil {
			supervisor.out.debugf("launching worker %s with %s", id, workerBin)
		}
		proc, err := supervisor.startWorker(localWorkerLaunchSpec{
			FollowCtx:  ctx,
			BinaryPath: workerBin,
			MasterAddr: cfg.GRPCAddr,
			WorkerID:   id,
			Insecure:   true,
			LogLevel:   cfg.WorkerLogLevel,
			LogFormat:  cfg.WorkerLogFormat,
		})
		if err != nil {
			return err
		}
		if managed && pg != nil {
			pg.add(proc)
		}
	}
	return nil
}

// trimLocalWorkers handles trim local workers behavior.
// It exists to keep this logic isolated and reusable.
func trimLocalWorkers(ctx context.Context, cfg localWorkerConfig, desired int) error {
	if desired < 1 {
		desired = 1
	}
	supervisor := newLocalSupervisor(cfg.GOCache, nil)
	procs, _, err := supervisor.listManagedProcesses()
	if err != nil {
		return err
	}
	if len(procs) == 0 {
		procs, err = listMasterWorkerProcs()
		if err != nil {
			return err
		}
	}
	keep := map[string]struct{}{}
	for i := 1; i <= desired; i++ {
		keep[fmt.Sprintf("local-%02d", i)] = struct{}{}
	}

	var stopPIDs []int
	for _, p := range procs {
		if p.Kind != "worker" {
			continue
		}
		if strings.TrimSpace(p.MasterAddr) != strings.TrimSpace(cfg.GRPCAddr) {
			continue
		}
		if _, ok := parseLocalWorkerNum(p.WorkerID); !ok {
			continue
		}
		if _, ok := keep[p.WorkerID]; ok {
			continue
		}
		stopPIDs = append(stopPIDs, p.PID)
	}
	if len(stopPIDs) == 0 {
		return nil
	}
	_, err = supervisor.stopPIDs(ctx, stopPIDs, false, 2*time.Second)
	if code := exitCode(err); code == exitInterrupted {
		return err
	}
	return nil
}
