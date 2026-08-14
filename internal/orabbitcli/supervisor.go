package orabbitcli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

type procGroup struct {
	mu    sync.Mutex
	procs []*managedProcessHandle
}

type localSupervisor struct {
	runtimeDir string
	out        *cliOutput
}

type managedProcessHandle struct {
	cmd        *exec.Cmd
	logs       *commandLogAttachment
	runtimeDir string
	pid        int

	cleanupOnce sync.Once
}

type localWorkerLaunchSpec struct {
	CommandCtx    context.Context
	FollowCtx     context.Context
	BinaryPath    string
	MasterAddr    string
	WorkerID      string
	Insecure      bool
	LogLevel      string
	LogFormat     string
	TLSCA         string
	TLSServerName string
}

func newLocalSupervisor(runtimeDir string, out *cliOutput) *localSupervisor {
	return &localSupervisor{
		runtimeDir: strings.TrimSpace(runtimeDir),
		out:        out,
	}
}

func (g *procGroup) add(proc *managedProcessHandle) {
	if proc == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.procs = append(g.procs, proc)
}

func (g *procGroup) signal(sig os.Signal) {
	g.mu.Lock()
	procs := append([]*managedProcessHandle(nil), g.procs...)
	g.mu.Unlock()
	for _, proc := range procs {
		proc.signal(sig)
	}
}

func (g *procGroup) wait() {
	g.mu.Lock()
	procs := append([]*managedProcessHandle(nil), g.procs...)
	g.mu.Unlock()
	for _, proc := range procs {
		if proc != nil {
			_ = proc.wait()
		}
	}
}

func (h *managedProcessHandle) signal(sig os.Signal) {
	if h == nil || h.cmd == nil || h.cmd.Process == nil {
		return
	}
	_ = h.cmd.Process.Signal(sig)
}

func (h *managedProcessHandle) wait() error {
	if h == nil || h.cmd == nil {
		return nil
	}
	err := h.cmd.Wait()
	if h.logs != nil {
		h.logs.stopFollowing()
	}
	h.unregister()
	return err
}

func (h *managedProcessHandle) unregister() {
	if h == nil {
		return
	}
	h.cleanupOnce.Do(func() {
		if h.pid > 0 && strings.TrimSpace(h.runtimeDir) != "" {
			_ = unregisterManagedProcess(h.runtimeDir, h.pid)
		}
	})
}

func (s *localSupervisor) listManagedProcesses() ([]procInfo, int, error) {
	return listManagedProcesses(s.runtimeDir)
}

func (s *localSupervisor) reconcileManagedState() (int, error) {
	return pruneManagedProcessState(s.runtimeDir)
}

func (s *localSupervisor) startMaster(commandCtx, followCtx context.Context, binaryPath, dbPath, httpAddr, grpcAddr string, insecure bool, tlsCert, tlsKey string) (*managedProcessHandle, error) {
	args := []string{
		"-db", dbPath,
		"-http-addr", httpAddr,
		"-grpc-addr", grpcAddr,
		"-insecure=" + fmt.Sprint(insecure),
	}
	if !insecure {
		args = append(args, "-tls-cert", strings.TrimSpace(tlsCert), "-tls-key", strings.TrimSpace(tlsKey))
	}
	return s.launchManagedProcess(commandCtx, followCtx, "master", binaryPath, args, procInfo{
		Kind:     "master",
		HTTPAddr: strings.TrimSpace(httpAddr),
		GRPCAddr: strings.TrimSpace(grpcAddr),
		DBPath:   strings.TrimSpace(dbPath),
		Insecure: insecure,
	})
}

func (s *localSupervisor) startWorker(spec localWorkerLaunchSpec) (*managedProcessHandle, error) {
	args, err := appendWorkerLogArgs([]string{
		"-master", strings.TrimSpace(spec.MasterAddr),
		"-worker-id", strings.TrimSpace(spec.WorkerID),
		"-insecure=" + fmt.Sprint(spec.Insecure),
	}, spec.LogLevel, spec.LogFormat)
	if err != nil {
		return nil, err
	}
	if !spec.Insecure {
		if v := strings.TrimSpace(spec.TLSCA); v != "" {
			args = append(args, "-tls-ca", v)
		}
		if v := strings.TrimSpace(spec.TLSServerName); v != "" {
			args = append(args, "-tls-server-name", v)
		}
	}
	return s.launchManagedProcess(spec.CommandCtx, spec.FollowCtx, "worker:"+strings.TrimSpace(spec.WorkerID), spec.BinaryPath, args, procInfo{
		Kind:       "worker",
		MasterAddr: strings.TrimSpace(spec.MasterAddr),
		WorkerID:   strings.TrimSpace(spec.WorkerID),
		Insecure:   spec.Insecure,
	})
}

func (s *localSupervisor) launchManagedProcess(commandCtx, followCtx context.Context, label, binaryPath string, args []string, proc procInfo) (*managedProcessHandle, error) {
	if strings.TrimSpace(binaryPath) == "" {
		return nil, fmt.Errorf("missing managed binary path for %s", label)
	}

	var cmd *exec.Cmd
	if commandCtx != nil {
		cmd = exec.CommandContext(commandCtx, binaryPath, args...)
	} else {
		cmd = exec.Command(binaryPath, args...)
	}

	logs, err := newCommandLogAttachment(s.runtimeDir, label)
	if err != nil {
		return nil, err
	}
	logs.apply(cmd)
	if err := cmd.Start(); err != nil {
		logs.closeParentWriter()
		logs.stopFollowing()
		return nil, err
	}

	proc.PID = cmd.Process.Pid
	proc.Command = strings.Join(cmd.Args, " ")
	if err := registerManagedProcess(s.runtimeDir, proc, binaryPath); err != nil {
		logs.closeParentWriter()
		logs.stopFollowing()
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
		return nil, err
	}

	logs.closeParentWriter()
	if followCtx == nil {
		followCtx = context.Background()
	}
	if s.out != nil {
		<-s.out.followCommandLog(followCtx, label, logs)
	}

	return &managedProcessHandle{
		cmd:        cmd,
		logs:       logs,
		runtimeDir: s.runtimeDir,
		pid:        cmd.Process.Pid,
	}, nil
}

func (s *localSupervisor) waitMasterReady(ctx context.Context, httpBase string, timeout time.Duration) error {
	if s.out != nil {
		s.out.debugf("waiting for master readiness at %s/ready", strings.TrimSpace(httpBase))
	}
	return waitHTTP(ctx, strings.TrimRight(strings.TrimSpace(httpBase), "/")+"/ready", timeout)
}

func (s *localSupervisor) waitGRPCReady(ctx context.Context, addr string, timeout time.Duration) error {
	if s.out != nil {
		s.out.debugf("waiting for gRPC readiness at %s", strings.TrimSpace(addr))
	}
	return waitTCP(ctx, addr, timeout)
}

func (s *localSupervisor) stopTargets(ctx context.Context, targets []stopTarget, force bool, timeout time.Duration) (string, error) {
	return s.stopPIDs(ctx, stopTargetPIDs(targets), force, timeout)
}

func (s *localSupervisor) stopPIDs(ctx context.Context, pids []int, force bool, timeout time.Duration) (string, error) {
	if len(pids) == 0 {
		return "stopped", nil
	}
	if force {
		_ = signalPIDs(pids, syscall.SIGKILL)
		_ = unregisterManagedProcesses(s.runtimeDir, pids)
		_, _ = s.reconcileManagedState()
		return "killed", nil
	}

	_ = signalPIDs(pids, syscall.SIGTERM)
	if err := waitPIDsExit(ctx, pids, timeout); err == nil {
		_ = unregisterManagedProcesses(s.runtimeDir, pids)
		_, _ = s.reconcileManagedState()
		return "stopped", nil
	} else if code := exitCode(err); code == exitInterrupted {
		return "", err
	}

	_ = signalPIDs(pids, syscall.SIGKILL)
	_ = unregisterManagedProcesses(s.runtimeDir, pids)
	_, _ = s.reconcileManagedState()
	return "killed", nil
}
