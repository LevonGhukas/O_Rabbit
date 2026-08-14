// cmd/worker/main.go
// this file contains the worker process which executes tasks assigned by the master.

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/arrowio"
	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	grpcapi "github.com/LevonGhukas/O_Rabbit/internal/grpc"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	"github.com/LevonGhukas/O_Rabbit/internal/sysinfo"
	"github.com/LevonGhukas/O_Rabbit/internal/workerworkspace"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type partitionSpec struct {
	Type           string `json:"type"`
	SourceMode     string `json:"source_mode"`
	QueryHash      string `json:"query_hash"`
	Table          string `json:"table"`
	CursorColumn   string `json:"cursor_column"`
	CursorDomain   string `json:"cursor_domain"`
	Lower          string `json:"lower"`
	Upper          string `json:"upper"`
	LowerExclusive bool   `json:"lower_exclusive"`
	UpperInclusive bool   `json:"upper_inclusive"`
	OutputPart     int64  `json:"output_part"` 
	WhereClause    string `json:"where_clause,omitempty"`
	IDColumn       string `json:"id_column"` // legacy alias
	From           int64  `json:"from"`      // legacy alias
	To             int64  `json:"to"`        // legacy alias
}

type sourceExtract struct {
	DBConnectMS    int64
	QueryMS        int64
	ConvertMS      int64
	ParquetCloseMS int64
	Rows           int64
	MaxCursor      string
	CursorDomain   string
	ParquetPath    string
	ParquetFiles   []parquetOutputFile
	ParquetBytes   int64
	LogicalBytes   int64
	PartitionLower string
	PartitionUpper string
	OutputPart     int64
}

type taskCanceledError struct {
	reason string
}

func (e *taskCanceledError) Error() string {
	if e == nil || strings.TrimSpace(e.reason) == "" {
		return "task canceled"
	}
	return e.reason
}

func asTaskCanceledError(err error) (*taskCanceledError, bool) {
	var out *taskCanceledError
	if errors.As(err, &out) {
		return out, true
	}
	return nil, false
}

func taskCanceledErrorFromRPC(err error) error {
	if status.Code(err) != codes.Canceled {
		return err
	}
	reason := strings.TrimSpace(status.Convert(err).Message())
	if reason == "" {
		reason = "task canceled"
	}
	return &taskCanceledError{reason: reason}
}

func main() {
	cfg := loadWorkerConfigFromEnv()
	fs := newWorkerFlagSet(&cfg)
	fs.Parse(os.Args[1:])

	cfg.Poll = normalizePollInterval(cfg.Poll)

	log, normalizedLevel, normalizedFormat, err := newWorkerLogger(cfg.LogLevel, cfg.LogFormat, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	cfg.LogLevel = normalizedLevel
	cfg.LogFormat = normalizedFormat
	slog.SetDefault(log)

	var transportCreds credentials.TransportCredentials
	if cfg.InsecureGRPC {
		transportCreds = insecure.NewCredentials()
	} else if strings.TrimSpace(cfg.TLSCAFile) != "" {
		creds, err := credentials.NewClientTLSFromFile(strings.TrimSpace(cfg.TLSCAFile), strings.TrimSpace(cfg.TLSServerName))
		if err != nil {
			log.Error("load worker TLS CA", slog.String("err", err.Error()))
			os.Exit(2)
		}
		transportCreds = creds
	} else {
		tlsCfg := &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: strings.TrimSpace(cfg.TLSServerName),
		}
		transportCreds = credentials.NewTLS(tlsCfg)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	repositoryRoot, _ := os.Getwd()
	workspaceManager, err := workerworkspace.Open(workerworkspace.Config{
		Root:                cfg.TempRoot,
		RepositoryRoot:      repositoryRoot,
		UnlockedGrace:       cfg.TempUnlockedGrace,
		MaxOfflineRetention: cfg.TempOfflineRetention,
		MaxEntries:          cfg.TempMaxEntries,
		MaxBytes:            cfg.TempMaxBytesPerScan,
		MinFreeBytes:        cfg.TempMinFreeBytes,
		MaxManagedBytes:     cfg.TempMaxManagedBytes,
		DryRun:              cfg.TempDryRun,
	})
	if err != nil {
		log.Error("initialize managed worker temp root", slog.String("err", err.Error()))
		os.Exit(2)
	}
	defer workspaceManager.Close()
	workerInstanceID, err := workerworkspace.RandomInstanceID()
	if err != nil {
		log.Error("create worker instance identity", slog.String("err", err.Error()))
		os.Exit(2)
	}
	if status, err := workspaceManager.Scan(ctx); err != nil {
		log.Error("startup workspace scavenging failed", slog.String("err", err.Error()))
		os.Exit(2)
	} else {
		log.Info("startup workspace scavenging complete", slog.String("managed_temp_root", status.ManagedTempRoot), slog.Int64("managed_bytes", status.ManagedBytes), slog.Int64("bytes_reclaimed", status.BytesReclaimed), slog.Bool("capacity_ready", status.CapacityReady))
	}
	scanTicker := time.NewTicker(cfg.TempScanInterval)
	defer scanTicker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-scanTicker.C:
				status, err := workspaceManager.Scan(ctx)
				if err != nil {
					log.Warn("periodic workspace scavenging failed", slog.String("err", err.Error()))
					continue
				}
				log.Info("periodic workspace scavenging complete", slog.Int64("managed_bytes", status.ManagedBytes), slog.Int64("bytes_reclaimed", status.BytesReclaimed), slog.Int("cleanup_failures", status.CleanupFailures), slog.Bool("capacity_ready", status.CapacityReady))
			}
		}
	}()

	clients := &clientCache{}
	defer clients.Close()

	conn, err := grpc.NewClient(
		cfg.MasterAddr,
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithUnaryInterceptor(grpcapi.WorkerAuthUnaryClientInterceptor(cfg.WorkerAuthToken)),
	)
	if err != nil {
		log.Error("dial master", slog.String("err", err.Error()))
		os.Exit(1)
	}
	defer conn.Close()

	cp := grpcpb.NewControlPlaneClient(conn)

	cap := map[string]any{
		"go":                 runtime.Version(),
		"os":                 runtime.GOOS,
		"arch":               runtime.GOARCH,
		"num_cpu":            runtime.NumCPU(),
		"pid":                os.Getpid(),
		"timestamp":          time.Now().UTC().Format(time.RFC3339Nano),
		"managed_temp_root":  filepath.Base(workspaceManager.Root()),
		"worker_instance_id": workerInstanceID,
	}
	initialCapJSON, _ := json.Marshal(cap)

	reg, err := registerWithRetry(ctx, log, cp, &grpcpb.RegisterWorkerRequest{WorkerId: cfg.WorkerID, Addr: cfg.WorkerAddr, CapabilitiesJson: string(initialCapJSON)})
	if err != nil {
		if ctx.Err() != nil {
			// Shutdown requested.
			return
		}
		log.Error("register worker", slog.String("err", err.Error()))
		os.Exit(1)
	}
	cfg.WorkerID = reg.WorkerId
	hbInterval := time.Duration(reg.HeartbeatIntervalMs) * time.Millisecond
	if hbInterval <= 0 {
		hbInterval = 5 * time.Second
	}
	log.Info("worker registered",
		slog.String("worker_id", cfg.WorkerID),
		slog.String("master", cfg.MasterAddr),
		slog.Duration("heartbeat", hbInterval),
		slog.String("log_level", cfg.LogLevel),
		slog.String("log_format", cfg.LogFormat),
	)

	hb := time.NewTicker(hbInterval)
	defer hb.Stop()

	errBackoff := cfg.Poll
	// Back off quickly when the master is down, but don't spam it.
	maxBackoff := 5 * time.Second
	nextErrLog := time.Time{}
	errLogEvery := 5 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-hb.C:
			hctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			_, _ = cp.Heartbeat(hctx, &grpcpb.HeartbeatRequest{WorkerId: cfg.WorkerID, NowUnixMs: time.Now().UnixMilli()})
			cancel()
		default:
		}

		if capacity, err := workspaceManager.CapacityReady(); err != nil {
			log.Warn("temporary workspace capacity low; task polling paused", slog.Uint64("disk_free_bytes", capacity.DiskFreeBytes), slog.Int64("managed_bytes", capacity.ManagedBytes), slog.String("err", err.Error()))
			_, _ = workspaceManager.Scan(ctx)
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.Poll):
			}
			continue
		}

		if avail, ok := sysinfo.AvailableMemoryBytes(); ok {
			if total, tok := sysinfo.TotalMemoryBytes(); tok && total > 0 {
				ratio := float64(avail) / float64(total)
				if ratio < 0.10 {
					log.Warn("system memory low; task polling paused", slog.Uint64("available_bytes", avail), slog.Uint64("total_bytes", total), slog.Float64("free_ratio", ratio))
					select {
					case <-ctx.Done():
						return
					case <-time.After(cfg.Poll):
					}
					continue
				}
			}
		}

		if usedFDs, limitFDs, ok := sysinfo.FileDescriptors(); ok && limitFDs > 0 {
			ratio := float64(usedFDs) / float64(limitFDs)
			if ratio > 0.85 {
				log.Warn("file descriptors nearing limit; task polling paused", slog.Uint64("used_fds", usedFDs), slog.Uint64("limit_fds", limitFDs), slog.Float64("used_ratio", ratio))
				select {
				case <-ctx.Done():
					return
				case <-time.After(cfg.Poll):
				}
				continue
			}
		}

		if avail, ok := sysinfo.AvailableMemoryBytes(); ok {
			cap["memory_available_bytes"] = avail
		}
		if total, ok := sysinfo.TotalMemoryBytes(); ok {
			cap["memory_total_bytes"] = total
		}
		if usedFDs, limitFDs, ok := sysinfo.FileDescriptors(); ok {
			cap["fd_used"] = usedFDs
			cap["fd_limit"] = limitFDs
		}
		cap["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
		capJSON, _ := json.Marshal(cap)

		rtctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		tres, err := cp.RequestTask(rtctx, &grpcpb.RequestTaskRequest{WorkerId: cfg.WorkerID, CapabilitiesJson: string(capJSON), ProtocolVersion: 5})
		cancel()
		if err != nil {
			// When master is down, RequestTask will error quickly; back off and avoid log spam.
			if ctx.Err() != nil {
				return
			}
			if status.Code(err) == codes.Canceled {
				return
			}
			if isPermanentTaskPollingError(err) {
				log.Error("worker task protocol rejected; worker cannot continue", slog.String("err", err.Error()))
				os.Exit(1)
			}
			now := time.Now()
			if nextErrLog.IsZero() || now.After(nextErrLog) {
				log.Error("request task", slog.String("err", err.Error()))
				nextErrLog = now.Add(errLogEvery)
			}
			if !waitForContext(ctx, errBackoff) {
				return
			}
			if errBackoff < maxBackoff {
				errBackoff *= 2
				if errBackoff > maxBackoff {
					errBackoff = maxBackoff
				}
			}
			continue
		}
		errBackoff = cfg.Poll
		t := tres.Task
		if t == nil || strings.TrimSpace(t.TaskId) == "" {
			if !waitForContext(ctx, cfg.Poll) {
				return
			}
			continue
		}

		log.Info("task assigned", slog.String("task_id", t.TaskId), slog.String("run_id", t.RunId), slog.Int("task_index", int(t.TaskIndex)))

		err = executeTaskManaged(ctx, log, cp, cfg.WorkerID, workerInstanceID, t, clients, workspaceManager)
		if err != nil {
			var ownershipLost *taskOwnershipLostError
			if errors.As(err, &ownershipLost) {
				log.Warn("task ownership lost; result suppressed", slog.String("task_id", t.TaskId), slog.String("err", err.Error()))
				continue
			}
			var transientSuccess *transientSuccessReportError
			if errors.As(err, &transientSuccess) {
				log.Warn("successful task result remains unreported after transient outage; failure suppressed", slog.String("task_id", t.TaskId), slog.String("err", err.Error()))
				continue
			}
			if cancelErr, ok := asTaskCanceledError(err); ok {
				_, _ = cp.ReportTaskResult(ctx, &grpcpb.ReportTaskResultRequest{
					WorkerId:     cfg.WorkerID,
					TaskId:       t.TaskId,
					RunId:        t.RunId,
					AttemptId:    t.AttemptId,
					FencingToken: t.FencingToken,
					Status:       "CANCELED",
					ErrorMessage: cancelErr.Error(),
					FailureClass: string(failure.FailureCanceled),
				})
				log.Info("task canceled", slog.String("task_id", t.TaskId), slog.String("reason", cancelErr.Error()))
				continue
			}
			if failure, ok := artifact.AsFailure(err); ok {
				reportArtifactFailureBestEffort(ctx, log, cp, cfg.WorkerID, t, failure)
			}
			failureClass := ""
			var fErr *failure.Failure
			if errors.As(err, &fErr) {
				failureClass = string(fErr.Class)
			}

			// Best-effort report failure.
			_, _ = cp.ReportTaskResult(ctx, &grpcpb.ReportTaskResultRequest{
				WorkerId:     cfg.WorkerID,
				TaskId:       t.TaskId,
				RunId:        t.RunId,
				AttemptId:    t.AttemptId,
				FencingToken: t.FencingToken,
				Status:       "FAILED",
				ErrorMessage: err.Error(),
				FailureClass: failureClass,
			})
			log.Error("task failed", slog.String("task_id", t.TaskId), slog.String("err", err.Error()))
			continue
		}
	}
}

func isPermanentTaskPollingError(err error) bool {
	return status.Code(err) == codes.FailedPrecondition
}

func waitForContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func registerWithRetry(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, req *grpcpb.RegisterWorkerRequest) (*grpcpb.RegisterWorkerResponse, error) {
	backoff := 200 * time.Millisecond
	// The master may still be starting; retry for a short grace period.
	deadline := time.Now().Add(30 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		resp, err := cp.RegisterWorker(callCtx, req)
		cancel()
		if err == nil {
			return resp, nil
		}
		last = err
		log.Warn("register failed, retrying", slog.String("err", err.Error()), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 2*time.Second {
			backoff *= 2
			if backoff > 2*time.Second {
				backoff = 2 * time.Second
			}
		}
	}
	return nil, last
}

func reportProgressBestEffort(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, req *grpcpb.ReportTaskProgressRequest) error {
	// Progress is optional; avoid failing the task for transient master/DB contention.
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	_, err := cp.ReportTaskProgress(callCtx, req)
	cancel()
	if err != nil {
		if cancelErr, ok := asTaskCanceledError(taskCanceledErrorFromRPC(err)); ok {
			return cancelErr
		}
		log.Debug("progress report dropped", slog.String("err", err.Error()))
	}
	return nil
}

func reportArtifactFailureBestEffort(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, failure *artifact.Failure) {
	if failure == nil || t == nil {
		return
	}
	fields, _ := json.Marshal(map[string]any{"artifact_failure": map[string]any{
		"classification": failure.Classification, "attempt_id": t.AttemptId, "attempt_number": t.AttemptNumber,
		"worker_id": workerID, "file_index": failure.FileIndex, "object_key": failure.ObjectKey,
		"verification_method": failure.VerificationMethod, "retryable": failure.Retryable,
		"ambiguous": failure.Ambiguous, "reconciliation_allowed": failure.ReconciliationOK,
	}})
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := cp.ReportTaskProgress(callCtx, &grpcpb.ReportTaskProgressRequest{WorkerId: workerID, TaskId: t.TaskId, RunId: t.RunId, AttemptId: t.AttemptId, FencingToken: t.FencingToken, FieldsJson: string(fields)})
	if err != nil {
		log.Warn("artifact failure event not persisted", slog.String("task_id", t.TaskId), slog.String("failure_class", string(failure.Classification)), slog.String("err", err.Error()))
	}
}

func normalizePollInterval(poll time.Duration) time.Duration {
	if poll <= 0 {
		return 2 * time.Second
	}
	return poll
}

func normalizePartitionSpec(ps partitionSpec) partitionSpec {
	ps.SourceMode = strings.ToLower(strings.TrimSpace(ps.SourceMode))
	if ps.SourceMode == "" {
		ps.SourceMode = "table"
	}
	if strings.TrimSpace(ps.CursorColumn) == "" {
		ps.CursorColumn = strings.TrimSpace(ps.IDColumn)
	}
	if strings.TrimSpace(ps.CursorDomain) == "" {
		switch ps.Type {
		case "sql_int_range", "mssql_int_range":
			ps.CursorDomain = string(connectors.CursorDomainInt64)
		}
	}
	if strings.TrimSpace(ps.Lower) == "" && (ps.Type == "sql_int_range" || ps.Type == "mssql_int_range") {
		ps.Lower = fmt.Sprintf("%d", ps.From)
		ps.LowerExclusive = true
	}
	if strings.TrimSpace(ps.Upper) == "" && (ps.Type == "sql_int_range" || ps.Type == "mssql_int_range") {
		ps.Upper = fmt.Sprintf("%d", ps.To)
		ps.UpperInclusive = true
	}
	return ps
}

func reportProgressWithRetry(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, req *grpcpb.ReportTaskProgressRequest) error {
	backoff := 100 * time.Millisecond
	deadline := time.Now().Add(15 * time.Second)
	for {
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := cp.ReportTaskProgress(callCtx, req)
		cancel()
		if err == nil {
			return nil
		}
		if cancelErr, ok := asTaskCanceledError(taskCanceledErrorFromRPC(err)); ok {
			return cancelErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		code := status.Code(err)
		retryable := isBusyRPC(err) || code == codes.Unavailable || code == codes.DeadlineExceeded
		if !retryable {
			return err
		}
		if time.Now().After(deadline) {
			return err
		}
		log.Debug("progress report retry", slog.String("err", err.Error()), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
			if backoff > time.Second {
				backoff = time.Second
			}
		}
	}
}

func checkTaskCancellation(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, rowsRead, bytesRead, bytesWritten int64) error {
	return reportProgressWithRetry(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
		WorkerId:          workerID,
		TaskId:            t.TaskId,
		RunId:             t.RunId,
		AttemptId:         t.AttemptId,
		FencingToken:      t.FencingToken,
		RowsRead:          rowsRead,
		BytesRead:         bytesRead,
		BytesWritten:      bytesWritten,
		UncompressedBytes: bytesWritten, // Fallback if logical bytes not explicitly passed
	})
}

type taskOwnershipLostError struct{ err error }

func (e *taskOwnershipLostError) Error() string { return "task ownership lost: " + e.err.Error() }

type leaseClock interface {
	Now() time.Time
	NewTimer(time.Duration) leaseTimer
}

type leaseTimer interface {
	C() <-chan time.Time
	Stop()
}

type realLeaseClock struct{}

type realLeaseTimer struct{ timer *time.Timer }

func (realLeaseClock) Now() time.Time { return time.Now() }
func (realLeaseClock) NewTimer(d time.Duration) leaseTimer {
	return realLeaseTimer{timer: time.NewTimer(d)}
}
func (t realLeaseTimer) C() <-chan time.Time { return t.timer.C }
func (t realLeaseTimer) Stop()               { t.timer.Stop() }

func executeTask(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, clients *clientCache) error {
	return executeTaskWithBody(ctx, cp, workerID, t, realLeaseClock{}, func(taskCtx context.Context) error {
		return executeTaskBody(taskCtx, log, cp, workerID, t, clients)
	})
}

func executeTaskManaged(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID, workerInstanceID string, t *grpcpb.TaskAssignment, clients *clientCache, manager *workerworkspace.Manager) error {
	workspace, err := manager.Create(t.RunId, t.TaskId, t.AttemptId, t.AttemptNumber, workerID, workerInstanceID)
	if err != nil {
		return err
	}
	log.Info("workspace created", slog.String("task_id", t.TaskId), slog.String("attempt_id", t.AttemptId))
	err = executeTaskWithBody(ctx, cp, workerID, t, realLeaseClock{}, func(taskCtx context.Context) error {
		taskCtx = withWorkspaceDir(taskCtx, workspace.Path)
		return executeTaskBody(taskCtx, log, cp, workerID, t, clients)
	})
	state := "COMPLETED"
	if err != nil {
		state = "FAILED"
		var ownershipLost *taskOwnershipLostError
		if errors.As(err, &ownershipLost) {
			state = "OWNERSHIP_LOST"
		} else if _, ok := asTaskCanceledError(err); ok {
			state = "CANCELED"
		}
	}
	if cleanupErr := workspace.Cleanup(state); cleanupErr != nil {
		log.Warn("workspace cleanup deferred to scavenger", slog.String("task_id", t.TaskId), slog.String("attempt_id", t.AttemptId), slog.String("err", cleanupErr.Error()))
		if err == nil {
			return cleanupErr
		}
	}
	return err
}

func executeTaskWithBody(ctx context.Context, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, clock leaseClock, body func(context.Context) error) error {
	if t.AttemptId == "" || t.FencingToken == "" {
		return &taskOwnershipLostError{err: errors.New("missing fenced attempt credentials")}
	}
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	lost := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		maintainTaskLeaseWithClock(taskCtx, cp, workerID, t, cancel, lost, clock)
	}()
	err := body(taskCtx)
	cancel()
	<-done
	select {
	case leaseErr := <-lost:
		return &taskOwnershipLostError{err: leaseErr}
	default:
		return err
	}
}

func maintainTaskLease(ctx context.Context, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, cancel context.CancelFunc, lost chan<- error) {
	maintainTaskLeaseWithClock(ctx, cp, workerID, t, cancel, lost, realLeaseClock{})
}

func renewalDelay(now, deadline time.Time) time.Duration {
	remaining := deadline.Sub(now)
	interval := remaining / 3
	if interval > 10*time.Second {
		interval = 10 * time.Second
	}
	if interval < time.Second {
		interval = time.Second
	}
	if interval > remaining {
		interval = remaining
	}
	return interval
}

func retryableRenewalError(err error) bool {
	return isTransientRPCError(err)
}

func maintainTaskLeaseWithClock(ctx context.Context, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, cancel context.CancelFunc, lost chan<- error, clock leaseClock) {
	deadline := time.UnixMilli(t.LeaseDeadlineUnixMs)
	for {
		now := clock.Now()
		if !now.Before(deadline) {
			reportOwnershipLost(cancel, lost, context.DeadlineExceeded)
			return
		}
		delay := renewalDelay(now, deadline)
		timer := clock.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
			timer.Stop()
			if !clock.Now().Before(deadline) {
				reportOwnershipLost(cancel, lost, context.DeadlineExceeded)
				return
			}
			callCtx, done := context.WithTimeout(ctx, 3*time.Second)
			resp, err := cp.RenewTaskLease(callCtx, &grpcpb.RenewTaskLeaseRequest{WorkerId: workerID, TaskId: t.TaskId, AttemptId: t.AttemptId, FencingToken: t.FencingToken})
			done()
			if err == nil {
				deadline = time.UnixMilli(resp.LeaseDeadlineUnixMs)
				continue
			}
			if !retryableRenewalError(err) || !clock.Now().Before(deadline) {
				reportOwnershipLost(cancel, lost, err)
				return
			}
		}
	}
}

func reportOwnershipLost(cancel context.CancelFunc, lost chan<- error, err error) {
	select {
	case lost <- err:
	default:
	}
	cancel()
}

func executeTaskBody(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, clients *clientCache) error {
	taskStart := time.Now()

	// Parse partition spec.
	var ps partitionSpec
	if strings.TrimSpace(t.PartitionSpecJson) != "" {
		if err := json.Unmarshal([]byte(t.PartitionSpecJson), &ps); err != nil {
			return fmt.Errorf("parse partition_spec_json: %w", err)
		}
	}
	ps = normalizePartitionSpec(ps)
	if err := checkTaskCancellation(ctx, log, cp, workerID, t, 0, 0, 0); err != nil {
		return err
	}

	engine := connectors.NormalizeSourceEngine(t.SourceEngine)
	var extracted sourceExtract
	var err error
	switch {
	case connectors.SupportsDocumentReader(engine):
		extracted, err = extractDocumentTask(ctx, log, cp, workerID, t, ps, clients, engine)
	case connectors.SupportsOrderedCursor(engine):
		extracted, err = extractSQLCursorTask(ctx, log, cp, workerID, t, ps, clients, engine)
	case engine == "flightsql":
		extracted, err = extractFlightSQLTask(ctx, log, cp, workerID, t, ps, clients)
	default:
		return fmt.Errorf("unsupported source_engine %q", t.SourceEngine)
	}
	if err != nil {
		return err
	}

	if len(extracted.ParquetFiles) == 0 {
		// No rows in this partition. Emit a bench event so CLI can still print totals.
		{
			fields := map[string]any{
				"bench": map[string]any{
					"db_connect_ms":     extracted.DBConnectMS,
					"query_ms":          extracted.QueryMS,
					"s3_init_ms":        int64(0),
					"convert_ms":        extracted.ConvertMS,
					"parquet_close_ms":  extracted.ParquetCloseMS,
					"parquet_meta_ms":   int64(0),
					"minio_upload_ms":   int64(0),
					"task_total_ms":     time.Since(taskStart).Milliseconds(),
					"rows":              extracted.Rows,
					"parquet_bytes":     int64(0),
					"parquet_files":     int64(0),
					"target_file_bytes": t.TargetFileBytes,
					"cursor_domain":     extracted.CursorDomain,
					"partition_lower":   extracted.PartitionLower,
					"partition_upper":   extracted.PartitionUpper,
					"output_part":       extracted.OutputPart,
					"query_hash":        ps.QueryHash,
					"upload_skipped":    true,
				},
			}
			b, _ := json.Marshal(fields)
			if err := reportProgressWithRetry(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
				WorkerId:     workerID,
				TaskId:       t.TaskId,
				RunId:        t.RunId,
				AttemptId:    t.AttemptId,
				FencingToken: t.FencingToken,
				RowsRead:     extracted.Rows,
				FieldsJson:   string(b),
			}); err != nil {
				if cancelErr, ok := asTaskCanceledError(err); ok {
					return cancelErr
				}
				log.Warn("task benchmark progress not persisted", slog.String("task_id", t.TaskId), slog.String("err", err.Error()))
			}
		}
		if err := checkTaskCancellation(ctx, log, cp, workerID, t, extracted.Rows, 0, 0); err != nil {
			return err
		}
		return reportResultWithRetry(ctx, log, cp, &grpcpb.ReportTaskResultRequest{
			WorkerId:          workerID,
			TaskId:            t.TaskId,
			RunId:             t.RunId,
			AttemptId:         t.AttemptId,
			FencingToken:      t.FencingToken,
			Status:            "SUCCEEDED",
			RowsRead:          extracted.Rows,
			BytesRead:         0,
			BytesWritten:      0,
			UncompressedBytes: 0,
		})
	}
	defer func() {
		for _, f := range extracted.ParquetFiles {
			if strings.TrimSpace(f.Path) != "" {
				_ = os.Remove(f.Path)
			}
		}
	}()

	s3Cfg := s3io.Config{
		Endpoint:        t.S3Endpoint,
		Region:          t.S3Region,
		Bucket:          t.S3Bucket,
		ForcePathStyle:  t.S3ForcePathStyle,
		AccessKeyID:     t.S3AccessKeyId,
		SecretAccessKey: t.S3SecretAccessKey,
		SessionToken:    t.S3SessionToken,
	}
	if err := checkTaskCancellation(ctx, log, cp, workerID, t, extracted.Rows, 0, extracted.ParquetBytes); err != nil {
		return err
	}
	u, s3InitMS, err := clients.S3(ctx, s3Cfg)
	if err != nil {
		return fmt.Errorf("init s3: %w", err)
	}

	datasetPrefix := strings.TrimSuffix(strings.TrimSpace(t.S3Prefix), "/")
	partNo := extracted.OutputPart
	if partNo <= 0 {
		partNo = int64(t.TaskIndex)
	}

	// Upload objects under a run-scoped prefix. This avoids costly promote/copy on commit and prevents collisions.
	runPrefix := buildAttemptRunPrefix(datasetPrefix, t.RunId, t.TaskId, t.AttemptId)
	objectKeys := buildTaskParquetObjectKeys(runPrefix, partNo, len(extracted.ParquetFiles))
	if len(objectKeys) != len(extracted.ParquetFiles) {
		return fmt.Errorf("build object keys: got %d keys for %d parquet files", len(objectKeys), len(extracted.ParquetFiles))
	}
	artifactRecords := make([]*grpcpb.ArtifactIntegrity, len(extracted.ParquetFiles))
	for i, pf := range extracted.ParquetFiles {
		artifactRecords[i] = &grpcpb.ArtifactIntegrity{ObjectKey: objectKeys[i], ByteSize: pf.Bytes, Sha256: pf.SHA256, RowCount: pf.Rows, SchemaFingerprint: pf.SchemaFingerprint, RunId: t.RunId, TaskId: t.TaskId, AttemptId: t.AttemptId, AttemptNumber: t.AttemptNumber, FileIndex: int32(i), FormatVersion: artifact.FormatVersion, VerificationMethod: artifact.VerificationPortable, VerificationStatus: artifact.VerificationVerified, MaxHwm: extracted.MaxCursor}
	}

	var (
		uploadMS      int64
		uploadSkipped = true
	)

	uploadCtx, releaseUploadCapacity, err := holdUploadCapacity(ctx, cp, workerID, t)
	if err != nil {
		return err
	}
	uploadCapacityReleased := false
	defer func() {
		if !uploadCapacityReleased {
			_ = releaseUploadCapacity()
		}
	}()
	uploadStart := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, len(extracted.ParquetFiles))
	skipCh := make(chan bool, len(extracted.ParquetFiles))

	for i, pf := range extracted.ParquetFiles {
		if err := uploadCtx.Err(); err != nil {
			guardErr := releaseUploadCapacity()
			uploadCapacityReleased = true
			if guardErr != nil {
				return guardErr
			}
			return err
		}
		wg.Add(1)
		go func(idx int, path string) {
			defer wg.Done()
			record := artifactRecords[idx]
			meta := map[string]string{
				"run_id":              t.RunId,
				"task_id":             t.TaskId,
				"attempt_id":          t.AttemptId,
				"attempt_number":      fmt.Sprintf("%d", t.AttemptNumber),
				"worker_id":           workerID,
				"part":                fmt.Sprintf("%06d", partNo),
				"file_index":          fmt.Sprintf("%03d", idx),
				"byte_size":           fmt.Sprintf("%d", record.ByteSize),
				"sha256":              record.Sha256,
				"row_count":           fmt.Sprintf("%d", record.RowCount),
				"schema_fingerprint":  record.SchemaFingerprint,
				"format_version":      fmt.Sprintf("%d", record.FormatVersion),
				"verification_method": record.VerificationMethod,
			}
			multipartObserver := func(eventCtx context.Context, event s3io.MultipartEvent) error {
				fields, _ := json.Marshal(map[string]any{"event": event.Event, "file_index": event.FileIndex, "object_key": event.ObjectKey, "provider_upload_id": event.ProviderUploadID, "sha256": event.SHA256, "size": event.Size, "error_class": event.ErrorClass})
				_, err := cp.ReportTaskProgress(eventCtx, &grpcpb.ReportTaskProgressRequest{WorkerId: workerID, TaskId: t.TaskId, RunId: t.RunId, AttemptId: t.AttemptId, FencingToken: t.FencingToken, Message: "MULTIPART_LIFECYCLE", FieldsJson: string(fields)})
				return taskCanceledErrorFromRPC(err)
			}
			upRes, err := u.UploadFileVerifiedTracked(uploadCtx, objectKeys[idx], path, meta, record.ByteSize, record.Sha256, idx, multipartObserver)
			if err != nil {
				if failure, ok := artifact.AsFailure(err); ok {
					failure.FileIndex = idx
				}
				errCh <- fmt.Errorf("upload parquet file %d: %w", idx, err)
				return
			}
			if upRes.VerificationMethod != "" {
				record.VerificationMethod = upRes.VerificationMethod
			}
			skipCh <- upRes.Skipped
		}(i, pf.Path)
	}

	wg.Wait()
	close(errCh)
	close(skipCh)

	var uploadErr error
	for err := range errCh {
		if err != nil && uploadErr == nil {
			uploadErr = err
		}
	}
	for skipped := range skipCh {
		uploadSkipped = uploadSkipped && skipped
	}
	if err := releaseUploadCapacity(); err != nil {
		return err
	}
	uploadCapacityReleased = true
	if uploadErr != nil {
		return uploadErr
	}
	uploadMS = time.Since(uploadStart).Milliseconds()

	minFileBytes, maxFileBytes, avgFileBytes := parquetFileSizeStats(extracted.ParquetFiles)

	// Emit a single benchmark event for the task (no message so it does not spam SSE output).
	// The CLI aggregates these at the end of `orabbit-client run`.
	{
		fields := map[string]any{
			"bench": map[string]any{
				"db_connect_ms":     extracted.DBConnectMS,
				"query_ms":          extracted.QueryMS,
				"s3_init_ms":        s3InitMS,
				"convert_ms":        extracted.ConvertMS,
				"parquet_close_ms":  extracted.ParquetCloseMS,
				"parquet_meta_ms":   int64(0),
				"minio_upload_ms":   uploadMS,
				"task_total_ms":     time.Since(taskStart).Milliseconds(),
				"rows":              extracted.Rows,
				"parquet_bytes":     extracted.ParquetBytes,
				"parquet_files":     len(extracted.ParquetFiles),
				"target_file_bytes": t.TargetFileBytes,
				"min_file_bytes":    minFileBytes,
				"max_file_bytes":    maxFileBytes,
				"avg_file_bytes":    avgFileBytes,
				"partition_keys":    t.PartitionKeys,
				"cursor_domain":     extracted.CursorDomain,
				"partition_lower":   extracted.PartitionLower,
				"partition_upper":   extracted.PartitionUpper,
				"output_part":       partNo,
				"query_hash":        ps.QueryHash,
				"upload_skipped":    uploadSkipped,
			},
		}
		b, _ := json.Marshal(fields)
		if err := reportProgressWithRetry(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
			WorkerId:          workerID,
			TaskId:            t.TaskId,
			RunId:             t.RunId,
			AttemptId:         t.AttemptId,
			FencingToken:      t.FencingToken,
			RowsRead:          extracted.Rows,
			BytesWritten:      extracted.ParquetBytes,
			UncompressedBytes: extracted.LogicalBytes,
			FieldsJson:        string(b),
		}); err != nil {
			if cancelErr, ok := asTaskCanceledError(err); ok {
				return cancelErr
			}
			log.Warn("task benchmark progress not persisted", slog.String("task_id", t.TaskId), slog.String("err", err.Error()))
		}
	}
	if err := checkTaskCancellation(ctx, log, cp, workerID, t, extracted.Rows, 0, extracted.ParquetBytes); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Report result.
	err = reportResultWithRetry(ctx, log, cp, &grpcpb.ReportTaskResultRequest{
		WorkerId:          workerID,
		TaskId:            t.TaskId,
		RunId:             t.RunId,
		AttemptId:         t.AttemptId,
		FencingToken:      t.FencingToken,
		Status:            "SUCCEEDED",
		RowsRead:          extracted.Rows,
		BytesRead:         0,
		BytesWritten:      extracted.ParquetBytes,
		UncompressedBytes: extracted.LogicalBytes,
		ParquetObjectKeys: objectKeys,
		Artifacts:         artifactRecords,
		MaxHwmValue:       extracted.MaxCursor,
	})
	if err != nil {
		return err
	}

	log.Info("task done",
		slog.String("task_id", t.TaskId),
		slog.Int64("rows", extracted.Rows),
		slog.Int64("bytes", extracted.ParquetBytes),
		slog.Int("parquet_files", len(extracted.ParquetFiles)),
		slog.Bool("upload_skipped", uploadSkipped),
		slog.String("first_object_key", objectKeys[0]),
	)
	return nil
}

func buildAttemptRunPrefix(datasetPrefix, runID, taskID, attemptID string) string {
	return strings.TrimSuffix(datasetPrefix, "/") + "/_runs/run-" + runID
}

// extractSQLCursorTask reads an ordered-cursor partition from a SQL source and writes a local Parquet file.
func extractSQLCursorTask(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, ps partitionSpec, clients *clientCache, sourceEngine string) (sourceExtract, error) {
	res := sourceExtract{}

	if ps.Type != "sql_cursor_range" && ps.Type != "sql_cursor_single" && ps.Type != "mssql_int_range" && ps.Type != "sql_int_range" {
		return res, fmt.Errorf("unsupported partition type %q for %s", ps.Type, sourceEngine)
	}
	sourceMode := strings.ToLower(strings.TrimSpace(ps.SourceMode))
	if sourceMode == "" {
		sourceMode = "table"
	}
	sourceQuery := ""
	if sourceMode == "query" {
		sourceQuery = strings.TrimSpace(t.SourceSql)
		if sourceQuery == "" {
			return res, fmt.Errorf("query mode task is missing source_sql")
		}
		log.Info("query mode task execution",
			slog.String("task_id", t.TaskId),
			slog.String("run_id", t.RunId),
			slog.String("source_engine", sourceEngine),
			slog.String("query_hash", ps.QueryHash),
		)
	}
	res.CursorDomain = ps.CursorDomain
	res.PartitionLower = strings.TrimSpace(ps.Lower)
	res.PartitionUpper = strings.TrimSpace(ps.Upper)
	res.OutputPart = ps.OutputPart
	if res.OutputPart <= 0 {
		res.OutputPart = int64(t.TaskIndex)
	}

	src, ms, err := clients.SQLReader(ctx, sourceEngine, t.SourceDsn)
	res.DBConnectMS = ms
	if err != nil {
		return res, fmt.Errorf("open %s: %w", sourceEngine, err)
	}

	qctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()

	queryStart := time.Now()
	rows, cols, colTypes, cursorIdx, err := src.QueryCursor(qctx, connectors.CursorQuery{
		Table:          ps.Table,
		SourceQuery:    sourceQuery,
		CursorColumn:   ps.CursorColumn,
		CursorDomain:   connectors.NormalizeCursorDomain(ps.CursorDomain),
		LowerBound:     ps.Lower,
		UpperBound:     ps.Upper,
		LowerExclusive: ps.LowerExclusive,
		UpperInclusive: ps.UpperInclusive,
		WhereClause:    ps.WhereClause,
	})
	if err != nil {
		return res, fmt.Errorf("query cursor partition: %w", err)
	}
	defer rows.Close()
	res.QueryMS = time.Since(queryStart).Milliseconds()

	alloc := memory.NewGoAllocator()
	var (
		pw       = newParquetRollingWriterWithContext(ctx, t.TargetFileBytes)
		rowsRead int64
		lastProg time.Time
	)

	convertStart := time.Now()
	total, actualMaxCursor, err := arrowio.RowsToRecordBatches(rows, cols, colTypes, 50_000, alloc, cursorIdx, connectors.NormalizeCursorDomain(ps.CursorDomain), func(schema *arrow.Schema, rec arrow.RecordBatch) error {
		rowsRead += rec.NumRows()
		if time.Since(lastProg) > 5*time.Second {
			lastProg = time.Now()
			if err := reportProgressBestEffort(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
				WorkerId:     workerID,
				TaskId:       t.TaskId,
				RunId:        t.RunId,
				AttemptId:    t.AttemptId,
				FencingToken: t.FencingToken,
				RowsRead:     rowsRead,
			}); err != nil {
				return err
			}
		}
		return pw.Write(schema, rec)
	})
	res.ConvertMS = time.Since(convertStart).Milliseconds()
	res.Rows = total
	res.MaxCursor = actualMaxCursor
	if err != nil {
		pw.Abort()
		return res, fmt.Errorf("read to arrow/parquet: %w", err)
	}
	if err := pw.Close(); err != nil {
		pw.Abort()
		return res, fmt.Errorf("close parquet: %w", err)
	}
	res.ParquetCloseMS = pw.CloseMS()
	res.ParquetFiles = pw.Files()
	res.ParquetBytes = pw.TotalBytes()
	res.LogicalBytes = pw.TotalLogicalBytes()
	if len(res.ParquetFiles) == 0 {
		return res, nil
	}
	return res, nil
}

// extractFlightSQLTask runs a FlightSQL query and writes a local Parquet file.
func extractFlightSQLTask(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, ps partitionSpec, clients *clientCache) (sourceExtract, error) {
	res := sourceExtract{}

	if ps.Type != "single" {
		return res, fmt.Errorf("unsupported partition type %q for flightsql", ps.Type)
	}
	res.OutputPart = int64(t.TaskIndex)

	src, ms, err := clients.FlightSQL(ctx, t.SourceDsn)
	res.DBConnectMS = ms
	if err != nil {
		return res, fmt.Errorf("open flightsql: %w", err)
	}

	qctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()

	pw := newParquetRollingWriterWithContext(ctx, t.TargetFileBytes)
	var (
		rowsRead int64
		lastProg time.Time
	)

	convertStart := time.Now()
	total, err := src.StreamQuery(qctx, t.SourceSql, func(schema *arrow.Schema, rec arrow.RecordBatch) error {
		rowsRead += rec.NumRows()
		if time.Since(lastProg) > 5*time.Second {
			lastProg = time.Now()
			if err := reportProgressBestEffort(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
				WorkerId:     workerID,
				TaskId:       t.TaskId,
				RunId:        t.RunId,
				AttemptId:    t.AttemptId,
				FencingToken: t.FencingToken,
				RowsRead:     rowsRead,
			}); err != nil {
				return err
			}
		}
		return pw.Write(schema, rec)
	})
	res.ConvertMS = time.Since(convertStart).Milliseconds()
	res.Rows = total
	if err != nil {
		pw.Abort()
		return res, fmt.Errorf("read flightsql to parquet: %w", err)
	}
	if err := pw.Close(); err != nil {
		pw.Abort()
		return res, fmt.Errorf("close parquet: %w", err)
	}
	res.ParquetCloseMS = pw.CloseMS()
	res.ParquetFiles = pw.Files()
	res.ParquetBytes = pw.TotalBytes()
	res.LogicalBytes = pw.TotalLogicalBytes()
	if len(res.ParquetFiles) == 0 {
		return res, nil
	}
	return res, nil
}

func extractDocumentTask(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, ps partitionSpec, clients *clientCache, sourceEngine string) (sourceExtract, error) {
	res := sourceExtract{}

	if ps.Type != "single" && ps.Type != "sql_cursor_range" && ps.Type != "sql_cursor_single" {
		return res, fmt.Errorf("unsupported partition type %q for %s", ps.Type, sourceEngine)
	}
	res.OutputPart = int64(t.TaskIndex)

	src, ms, err := clients.DocumentReader(ctx, sourceEngine, t.SourceDsn)
	res.DBConnectMS = ms
	if err != nil {
		return res, fmt.Errorf("open %s: %w", sourceEngine, err)
	}

	qctx, cancel := context.WithTimeout(ctx, 2*time.Hour)
	defer cancel()

	collection := ps.Table
	if collection == "" {
		collection = strings.TrimSpace(t.SourceSql)
	}
	if collection == "" {
		return res, fmt.Errorf("%s requires collection name in partition_spec.table or source_sql", sourceEngine)
	}

	const batchSize = 10000
	var (
		pw             *parquetRollingWriter
		schema         *arrow.Schema
		rowsRead       int64
		lastProg       time.Time
		schemaInferred bool
	)
	pw = newParquetRollingWriterWithContext(ctx, t.TargetFileBytes)

	convertStart := time.Now()

	var filter map[string]any
	if ps.Type == "sql_cursor_range" || ps.Type == "sql_cursor_single" {
		f, err := src.BuildCursorFilter(connectors.CursorQuery{
			CursorColumn:   ps.CursorColumn,
			CursorDomain:   connectors.NormalizeCursorDomain(ps.CursorDomain),
			LowerBound:     ps.Lower,
			UpperBound:     ps.Upper,
			LowerExclusive: ps.LowerExclusive,
			UpperInclusive: ps.UpperInclusive,
		})
		if err != nil {
			return res, fmt.Errorf("build cursor filter: %w", err)
		}
		filter = f
		res.CursorDomain = ps.CursorDomain
		res.PartitionLower = ps.Lower
		res.PartitionUpper = ps.Upper
	}

	it, err := src.StreamDocuments(qctx, collection, filter, batchSize)
	if err != nil {
		return res, fmt.Errorf("%s stream: %w", sourceEngine, err)
	}
	defer it.Close()

	var (
		docBuf []map[string]any
		alloc  = memory.NewGoAllocator()
	)

	for it.Next(qctx) {
		doc, err := it.Decode()
		if err != nil {
			return res, fmt.Errorf("decode %s document: %w", sourceEngine, err)
		}
		docBuf = append(docBuf, doc)

		if !schemaInferred && len(docBuf) >= batchSize {
			schema, err = arrowio.InferMongoSchema(docBuf)
			if err != nil {
				return res, fmt.Errorf("infer schema: %w", err)
			}
			schemaInferred = true
		}

		if schemaInferred && len(docBuf) >= batchSize {
			if err := writeMongoDocBatch(alloc, pw, schema, docBuf); err != nil {
				return res, err
			}
			rowsRead += int64(len(docBuf))
			docBuf = docBuf[:0]

			if time.Since(lastProg) > 5*time.Second {
				lastProg = time.Now()
				if err := reportProgressBestEffort(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
					WorkerId:     workerID,
					TaskId:       t.TaskId,
					RunId:        t.RunId,
					AttemptId:    t.AttemptId,
					FencingToken: t.FencingToken,
					RowsRead:     rowsRead,
				}); err != nil {
					return res, err
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		pw.Abort()
		return res, fmt.Errorf("%s cursor error: %w", sourceEngine, err)
	}

	// Flush remaining docs.
	if len(docBuf) > 0 {
		if !schemaInferred {
			schema, err = arrowio.InferMongoSchema(docBuf)
			if err != nil {
				return res, fmt.Errorf("infer schema: %w", err)
			}
			schemaInferred = true
		}
		if err := writeMongoDocBatch(alloc, pw, schema, docBuf); err != nil {
			return res, err
		}
		rowsRead += int64(len(docBuf))
	}

	res.ConvertMS = time.Since(convertStart).Milliseconds()
	res.Rows = rowsRead

	if err := pw.Close(); err != nil {
		pw.Abort()
		return res, fmt.Errorf("close parquet: %w", err)
	}
	res.ParquetCloseMS = pw.CloseMS()
	res.ParquetFiles = pw.Files()
	res.ParquetBytes = pw.TotalBytes()
	res.LogicalBytes = pw.TotalLogicalBytes()
	if len(res.ParquetFiles) == 0 {
		return res, nil
	}
	return res, nil
}

func writeMongoDocBatch(alloc memory.Allocator, pw *parquetRollingWriter, schema *arrow.Schema, docs []map[string]any) error {
	rec, err := arrowio.MongoDocsToRecord(alloc, schema, docs)
	if err != nil {
		return fmt.Errorf("convert batch to arrow: %w", err)
	}
	if rec == nil {
		return nil
	}
	defer rec.Release()
	return pw.Write(schema, rec)
}
