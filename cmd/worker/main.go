// cmd/worker/main.go
// this file contains the worker process which executes tasks assigned by the master.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	grpcapi "github.com/LevonGhukas/O_Rabbit/internal/grpc"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/sysinfo"
	"github.com/LevonGhukas/O_Rabbit/internal/telemetry"
	"github.com/LevonGhukas/O_Rabbit/internal/workerworkspace"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type partitionSpec struct {
	Type           string            `json:"type"`
	SourceMode     string            `json:"source_mode"`
	QueryHash      string            `json:"query_hash"`
	Table          string            `json:"table"`
	CursorColumn   string            `json:"cursor_column"`
	CursorDomain   string            `json:"cursor_domain"`
	Lower          string            `json:"lower"`
	Upper          string            `json:"upper"`
	LowerExclusive bool              `json:"lower_exclusive"`
	UpperInclusive bool              `json:"upper_inclusive"`
	OutputPart     int64             `json:"output_part"`
	WhereClause    string            `json:"where_clause,omitempty"`
	SelectColumns  []string          `json:"select_columns,omitempty"`
	ColumnTypes    map[string]string `json:"column_types,omitempty"`
	RecordPath     string            `json:"record_path,omitempty"`
	FileFormat     string            `json:"format,omitempty"`
	IDColumn       string            `json:"id_column"` // legacy alias
	From           int64             `json:"from"`      // legacy alias
	To             int64             `json:"to"`        // legacy alias
}

func main() {
	cfg := loadWorkerConfigFromEnv()
	fs := newWorkerFlagSet(&cfg)
	fs.Parse(os.Args[1:])

	cfg.Poll = normalizePollInterval(cfg.Poll)
	sourceQueryTimeout = cfg.SourceQueryTimeout

	log, normalizedLevel, normalizedFormat, err := newWorkerLogger(cfg.LogLevel, cfg.LogFormat, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	cfg.LogLevel = normalizedLevel
	cfg.LogFormat = normalizedFormat
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.InitTracing(ctx, "orabbit-worker")
	if err != nil {
		log.Error("configure tracing", slog.String("err", err.Error()))
		os.Exit(2)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(flushCtx)
	}()

	// With TLS the worker authenticates with its master-issued identity
	// certificate, enrolling on first start; the master derives the worker ID
	// from that certificate. Plaintext (loopback development) keeps the
	// configured worker ID.
	var transportCreds credentials.TransportCredentials
	var identity *workerIdentity
	if cfg.InsecureGRPC {
		transportCreds = insecure.NewCredentials()
	} else {
		identity, err = loadWorkerIdentity(cfg.IdentityDir)
		if err == nil && identity != nil && time.Now().After(identity.leaf().NotAfter) {
			log.Warn("worker identity certificate expired; re-enrolling", slog.String("worker_id", identity.workerID()))
			identity = nil
		}
		if err == nil && identity == nil {
			identity, err = enrollWorker(ctx, log, cfg, cfg.IdentityDir)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Error("worker identity unavailable", slog.String("err", err.Error()))
			os.Exit(2)
		}
		tlsCfg, err := grpcapi.ClientTLSConfig(cfg.TLSCAFile, cfg.TLSServerName, identity.clientCertificate)
		if err != nil {
			log.Error("configure worker gRPC TLS", slog.String("err", err.Error()))
			os.Exit(2)
		}
		transportCreds = credentials.NewTLS(tlsCfg)
		cfg.WorkerID = identity.workerID()
	}

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
		grpc.WithKeepaliveParams(grpcapi.WorkerKeepaliveParams),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Error("dial master", slog.String("err", err.Error()))
		os.Exit(1)
	}
	defer conn.Close()

	cp := grpcpb.NewControlPlaneClient(conn)
	if identity != nil {
		go identity.renewLoop(ctx, log, cp)
	}

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
		tres, err := cp.RequestTask(rtctx, &grpcpb.RequestTaskRequest{WorkerId: cfg.WorkerID, CapabilitiesJson: string(capJSON), ProtocolVersion: grpcapi.WorkerProtocolVersion})
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

		err = executeTaskManaged(
			ctx,
			log,
			cp,
			cfg.WorkerID,
			workerInstanceID,
			t,
			clients,
			workspaceManager,
		)
		if err != nil {
			var ownershipLost *taskOwnershipLostError
			if errors.As(err, &ownershipLost) {
				log.Warn(
					"task ownership lost; result suppressed",
					slog.String("task_id", t.TaskId),
					slog.String("err", err.Error()),
				)
				continue
			}
			var transientSuccess *transientSuccessReportError
			if errors.As(err, &transientSuccess) {
				log.Warn(
					"successful task result remains unreported after transient outage; failure suppressed",
					slog.String("task_id", t.TaskId),
					slog.String("err", err.Error()),
				)
				continue
			}
			if cancelErr, ok := asTaskCanceledError(err); ok {
				reportErr := reportResultWithRetry(
					ctx,
					log,
					cp,
					&grpcpb.ReportTaskResultRequest{
						WorkerId:     cfg.WorkerID,
						BootId:       workerInstanceID,
						TaskId:       t.TaskId,
						RunId:        t.RunId,
						AttemptId:    t.AttemptId,
						FencingToken: t.FencingToken,
						Status:       "CANCELED",
						ErrorMessage: cancelErr.Error(),
						FailureClass: string(failure.FailureCanceled),
					},
				)
				if reportErr != nil {
					log.Warn(
						"failed to report canceled task result",
						slog.String("task_id", t.TaskId),
						slog.String("err", reportErr.Error()),
					)
				}
				log.Info(
					"task canceled",
					slog.String("task_id", t.TaskId),
					slog.String("reason", cancelErr.Error()),
				)
				continue
			}
			if artifactFailure, ok := artifact.AsFailure(err); ok {
				reportArtifactFailureBestEffort(
					ctx,
					log,
					cp,
					cfg.WorkerID,
					t,
					artifactFailure,
				)
			}
			// Always report a class: unrecognized errors classify as
			// UNKNOWN_PERMANENT rather than leaving the failure unclassified.
			failureClass := string(failure.ClassOf(err))
			reportErr := reportResultWithRetry(
				ctx,
				log,
				cp,
				&grpcpb.ReportTaskResultRequest{
					WorkerId:     cfg.WorkerID,
					BootId:       workerInstanceID,
					TaskId:       t.TaskId,
					RunId:        t.RunId,
					AttemptId:    t.AttemptId,
					FencingToken: t.FencingToken,
					Status:       "FAILED",
					ErrorMessage: err.Error(),
					FailureClass: failureClass,
				},
			)
			if reportErr != nil {
				log.Warn(
					"failed to report task failure",
					slog.String("task_id", t.TaskId),
					slog.String("err", reportErr.Error()),
				)
			}
			log.Error(
				"task failed",
				slog.String("task_id", t.TaskId),
				slog.String("err", err.Error()),
			)
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
