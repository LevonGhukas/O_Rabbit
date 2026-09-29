// cmd/master/main.go
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	grpcapi "github.com/LevonGhukas/O_Rabbit/internal/grpc"
	httpapi "github.com/LevonGhukas/O_Rabbit/internal/http"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
)

// Process exit codes. A supervisor restarts the master on any non-zero code.
const (
	exitOK      = 0 // requested shutdown (SIGINT/SIGTERM)
	exitFailure = 1 // runtime or recovery failure, lost leadership, server error
	exitConfig  = 2 // invalid configuration
)

func main() {
	os.Exit(run())
}

// run starts the master and returns its exit code. Returning, rather than
// calling os.Exit, lets deferred cleanup run on every path: the durable
// leadership lease is released (so a restarted master can take over
// immediately), the database is closed, and the singleton lock is released.
func run() int {
	cfg := loadMasterConfigFromEnv()
	bindMasterFlags(&cfg)
	flag.Parse()

	log := newMasterLogger(cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(log)
	if err := cfg.validateLeasePolicy(); err != nil {
		log.Error("invalid task lease configuration", slog.String("err", err.Error()))
		return exitConfig
	}
	if err := cfg.validateAuthentication(); err != nil {
		log.Error("invalid control-plane authentication configuration", slog.String("err", err.Error()))
		return exitConfig
	}

	if cfg.Insecure && cfg.AllowInsecureRemoteGRPC && !isLoopbackListenAddress(cfg.GRPCAddr) {
		log.Warn("plaintext gRPC is enabled on a non-loopback listener; task credentials are sent unencrypted",
			slog.String("grpc", cfg.GRPCAddr))
	}

	k, err := crypto.LoadMasterKeyFromEnv()
	if err != nil {
		log.Error("load master key", slog.String("err", err.Error()))
		return exitConfig
	}
	if k.IsZero() {
		log.Error("ORABBIT_MASTER_KEY is required")
		return exitConfig
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	instanceID, err := db.NewMasterInstanceID()
	if err != nil {
		log.Error("create master instance identity", slog.String("err", err.Error()))
		return exitFailure
	}
	processLock, err := db.AcquireMasterProcessLock(cfg.DBPath, instanceID)
	if err != nil {
		log.Error("acquire local master singleton lock", slog.String("err", err.Error()))
		return exitFailure
	}
	defer processLock.Close()
	cfg.DBPath = processLock.DatabasePath

	st, err := db.Open(ctx, db.Config{Path: cfg.DBPath}, log)
	if err != nil {
		log.Error("open db", slog.String("err", err.Error()))
		return exitFailure
	}
	defer st.Close()

	st.SetMasterKey(k)
	if err := st.MigrateLegacySecrets(ctx, k); err != nil {
		log.Error("migrate legacy secrets", slog.String("err", err.Error()))
		return exitFailure
	}
	st.SetMaxActiveRuns(cfg.MaxActiveRuns)
	lease, err := st.AcquireLeadership(ctx, instanceID, cfg.LeadershipLeaseDuration, map[string]any{"pid": os.Getpid(), "database_identity": processLock.Identity})
	if err != nil {
		log.Error("acquire durable master leadership", slog.String("err", err.Error()))
		return exitFailure
	}
	if err := st.ActivateLeadershipFence(ctx, instanceID, lease.Epoch); err != nil {
		log.Error("activate master mutation fence", slog.String("err", err.Error()))
		return exitFailure
	}
	leadership, err := db.NewLeadershipController(st, lease, cfg.LeadershipLeaseDuration, cfg.LeadershipRenewInterval, processLock.Identity)
	if err != nil {
		log.Error("configure master leadership", slog.String("err", err.Error()))
		return exitFailure
	}
	leaderCtx := leadership.Start(ctx)
	defer leadership.Stop(context.Background())

	bc := httpapi.NewBroadcaster(log)

	icebergMgr := icebergreg.NewManager(log, icebergreg.ManagerConfig{IceBinary: cfg.IceBin})
	grpcSrv := grpcapi.NewServer(log, st, bc, k, 5*time.Second, icebergMgr)
	if !cfg.Insecure {
		workerCA, err := grpcapi.LoadWorkerCA(ctx, st, k, time.Now())
		if err != nil {
			log.Error("load worker identity CA", slog.String("err", err.Error()))
			return exitFailure
		}
		grpcSrv.SetWorkerIdentity(workerCA, cfg.WorkerCertTTL)
	}
	grpcSrv.SetLeasePolicy(db.LeasePolicy{Duration: cfg.TaskLeaseDuration, MaxAttempts: cfg.TaskMaxAttempts, MaxActiveTasks: cfg.MaxActiveTasks, BackoffBase: cfg.TaskRetryBackoff, BackoffMax: cfg.TaskRetryBackoffMax})
	grpcSrv.SetCatalogWorkLimit(cfg.CatalogWorkLimit)
	grpcSrv.SetUploadCapacityPolicy(cfg.UploadCapacityLimit, cfg.UploadCapacityLeaseTTL)
	grpcSrv.SetMultipartCleanupPolicy(cfg.MultipartAbandonmentGrace, time.Minute, cfg.MultipartCleanupMaxAttempts)
	st.SetCanceledObjectRetention(cfg.CanceledObjectRetention)
	grpcSrv.SetCanceledObjectCleanupPolicy(time.Minute, cfg.CanceledObjectCleanupMaxAttempts, cfg.CanceledObjectCleanupDryRun)
	st.RecordLeadershipEvent(leaderCtx, instanceID, lease.Epoch, "MASTER_RECOVERY_STARTED", nil)
	reconcileCtx, reconcileCancel := context.WithTimeout(leaderCtx, 30*time.Minute)
	if err := grpcSrv.ReconcileCommittingRuns(reconcileCtx); err != nil {
		log.Error("reconcile committing runs", slog.String("err", err.Error()))
		st.RecordLeadershipEvent(context.Background(), instanceID, lease.Epoch, "MASTER_RECOVERY_FAILED", map[string]any{"phase": "committing_runs"})
		reconcileCancel()
		return recoveryExitCode(ctx)
	}
	if n, err := grpcSrv.ExpireLeases(reconcileCtx); err != nil {
		log.Error("reconcile expired task leases", slog.String("err", err.Error()))
		st.RecordLeadershipEvent(context.Background(), instanceID, lease.Epoch, "MASTER_RECOVERY_FAILED", map[string]any{"phase": "task_leases"})
		reconcileCancel()
		return recoveryExitCode(ctx)
	} else if n > 0 {
		log.Info("reconciled expired task leases", slog.Int("count", n))
	}
	reconcileCancel()
	if abandoned, err := st.FailAbandonedPlanningRuns(leaderCtx, time.Now()); err != nil {
		log.Error("fail abandoned planning runs", slog.String("err", err.Error()))
		st.RecordLeadershipEvent(context.Background(), instanceID, lease.Epoch, "MASTER_RECOVERY_FAILED", map[string]any{"phase": "abandoned_planning_runs"})
		return recoveryExitCode(ctx)
	} else if len(abandoned) > 0 {
		log.Warn("failed runs abandoned during planning", slog.Int("count", len(abandoned)), slog.Any("run_ids", abandoned))
	}
	registrationPolicy := db.RegistrationPolicy{LeaseDuration: 30 * time.Second, MaxAttempts: 5, BackoffBase: time.Second, BackoffMax: time.Minute}
	if classified, err := st.ReconcileHistoricalRegistrations(leaderCtx, time.Now()); err != nil {
		log.Error("classify historical registrations", slog.String("err", err.Error()))
		st.RecordLeadershipEvent(context.Background(), instanceID, lease.Epoch, "MASTER_RECOVERY_FAILED", map[string]any{"phase": "historical_registrations"})
		return recoveryExitCode(ctx)
	} else if len(classified) > 0 {
		log.Info("classified historical registrations", slog.Int("count", len(classified)))
	}
	if n, err := st.ExpireRegistrationAttempts(leaderCtx, time.Now(), registrationPolicy); err != nil {
		log.Error("reconcile expired registration leases", slog.String("err", err.Error()))
		st.RecordLeadershipEvent(context.Background(), instanceID, lease.Epoch, "MASTER_RECOVERY_FAILED", map[string]any{"phase": "registration_leases"})
		return recoveryExitCode(ctx)
	} else if n > 0 {
		log.Info("reconciled expired registration leases", slog.Int("count", n))
	}
	if n, err := st.ExpireReconciliationAttempts(leaderCtx, time.Now(), time.Second, 5); err != nil {
		log.Error("reconcile expired catalog-observation leases", slog.String("err", err.Error()))
		st.RecordLeadershipEvent(context.Background(), instanceID, lease.Epoch, "MASTER_RECOVERY_FAILED", map[string]any{"phase": "catalog_reconciliation_leases"})
		return recoveryExitCode(ctx)
	} else if n > 0 {
		log.Info("reconciled expired catalog-observation leases", slog.Int("count", n))
	}
	st.RecordLeadershipEvent(leaderCtx, instanceID, lease.Epoch, "MASTER_RECOVERY_COMPLETED", nil)
	leadership.SetReady(true)
	grpcSrv.SetLeadershipGuard(leadership)
	go runCommittingReconciliationLoop(leaderCtx, 2*time.Second, 30*time.Minute, grpcSrv, log)
	go runPeriodic(leaderCtx, log, "catalog registration and reconciliation", 2*time.Second, func() {
		for i := 0; i < 2; i++ {
			processed, err := grpcSrv.ProcessReconciliationOnce(leaderCtx)
			if err != nil {
				log.Warn("catalog reconciliation failed", slog.String("err", err.Error()))
				break
			}
			if !processed {
				break
			}
		}
		for i := 0; i < 4; i++ {
			processed, err := grpcSrv.ProcessRegistrationOnce(leaderCtx)
			if err != nil {
				log.Warn("durable iceberg registration FAILED", slog.String("err", err.Error()))
				break
			}
			if !processed {
				break
			}
		}
		if _, err := st.ExpireRegistrationAttempts(leaderCtx, time.Now(), registrationPolicy); err != nil && leaderCtx.Err() == nil {
			log.Warn("registration lease expiration scan failed", slog.String("err", err.Error()))
		}
		if _, err := st.ExpireReconciliationAttempts(leaderCtx, time.Now(), time.Second, 5); err != nil && leaderCtx.Err() == nil {
			log.Warn("reconciliation lease expiration scan failed", slog.String("err", err.Error()))
		}
	})
	go runPeriodic(leaderCtx, log, "task lease expiration", cfg.TaskLeaseScanInterval, func() {
		if _, err := grpcSrv.ExpireLeases(leaderCtx); err != nil && leaderCtx.Err() == nil {
			log.Warn("task lease expiration scan failed", slog.String("err", err.Error()))
		}
	})
	go runPeriodic(leaderCtx, log, "multipart cleanup", cfg.MultipartCleanupScanInterval, func() {
		for i := 0; i < 4; i++ {
			processed, err := grpcSrv.ProcessMultipartCleanupOnce(leaderCtx)
			if err != nil {
				if leaderCtx.Err() == nil {
					log.Warn("multipart cleanup failed", slog.String("err", err.Error()))
				}
				break
			}
			if !processed {
				break
			}
		}
	})
	go runPeriodic(leaderCtx, log, "canceled-object cleanup", cfg.CanceledObjectCleanupScanInterval, func() {
		for i := 0; i < 4; i++ {
			processed, err := grpcSrv.ProcessCanceledObjectCleanupOnce(leaderCtx)
			if err != nil {
				if leaderCtx.Err() == nil {
					log.Warn("canceled-object cleanup failed", slog.String("err", err.Error()))
				}
				break
			}
			if !processed {
				break
			}
		}
	})

	httpErr := make(chan error, 1)
	httpSrv := httpapi.NewServer(log, st, bc, k, httpapi.StatusInfo{PID: os.Getpid(), HTTPAddr: cfg.HTTPAddr, GRPCAddr: cfg.GRPCAddr, DBPath: processLock.Identity}, cfg.HTTPAuthToken)
	httpSrv.SetLeadershipGuard(leadership)
	httpSrv.SetRemoteOpsToken(cfg.RemoteOpsAuthToken)
	httpSrv.SetOperability(cfg.TaskMaxAttempts, grpcSrv)
	// The servers stop when serveCtx ends: on shutdown, on lost leadership,
	// or when the other server fails.
	serveCtx, cancelServe := context.WithCancel(leaderCtx)
	defer cancelServe()
	go func() {
		httpErr <- httpSrv.Serve(serveCtx, cfg.HTTPAddr)
	}()

	gcfg := grpcapi.Config{Addr: cfg.GRPCAddr, Insecure: cfg.Insecure, TLSCertFile: cfg.TLSCert, TLSKeyFile: cfg.TLSKey, WorkerAuthToken: cfg.WorkerAuthToken, HeartbeatInterval: 5 * time.Second}
	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- grpcapi.ListenAndServe(serveCtx, gcfg, grpcSrv)
	}()

	log.Info("master started",
		slog.String("http", cfg.HTTPAddr),
		slog.String("grpc", cfg.GRPCAddr),
		slog.Bool("http_auth", strings.TrimSpace(cfg.HTTPAuthToken) != ""),
		slog.Bool("worker_auth", strings.TrimSpace(cfg.WorkerAuthToken) != ""),
		slog.Bool("insecure", cfg.Insecure),
		slog.Bool("worker_identity", !cfg.Insecure),
		slog.Bool("remote_ops", cfg.RemoteOpsAuthToken != ""),
		slog.String("iceberg_registration", "persisted-run-snapshot"),
		slog.String("ice_binary", cfg.IceBin),
		slog.String("log_level", cfg.LogLevel),
		slog.String("log_format", cfg.LogFormat),
		slog.String("instance_id", instanceID),
		slog.Int64("leadership_epoch", lease.Epoch),
	)

	return awaitShutdown(ctx, leaderCtx, cancelServe, httpErr, grpcErr, log)
}

// recoveryExitCode is the exit code after a startup recovery step fails: a
// failure caused by a requested shutdown is not an error.
func recoveryExitCode(signalCtx context.Context) int {
	if signalCtx.Err() != nil {
		return exitOK
	}
	return exitFailure
}

// awaitShutdown blocks until the master must stop, then stops both servers
// and waits for them to drain before returning the exit code. signalCtx ends
// on SIGINT/SIGTERM; leaderCtx also ends when leadership is lost.
func awaitShutdown(signalCtx, leaderCtx context.Context, stopServers context.CancelFunc, httpErr, grpcErr <-chan error, log *slog.Logger) int {
	code := exitOK
	httpDone, grpcDone := false, false
	select {
	case <-leaderCtx.Done():
		if signalCtx.Err() != nil {
			log.Info("master shutting down", slog.String("reason", "signal"))
		} else {
			log.Error("master leadership lost; shutting down")
			code = exitFailure
		}
	case err := <-httpErr:
		httpDone = true
		code = serverStopExitCode(log, "http", err, leaderCtx)
	case err := <-grpcErr:
		grpcDone = true
		code = serverStopExitCode(log, "grpc", err, leaderCtx)
	}
	stopServers()
	for !httpDone || !grpcDone {
		select {
		case err := <-httpErr:
			httpDone = true
			if err != nil {
				log.Warn("http server shutdown error", slog.String("err", err.Error()))
			}
		case err := <-grpcErr:
			grpcDone = true
			if err != nil {
				log.Warn("grpc server shutdown error", slog.String("err", err.Error()))
			}
		}
	}
	log.Info("master stopped", slog.Int("exit_code", code))
	return code
}

// serverStopExitCode classifies a server that stopped on its own. Servers
// only return nil once their context ends, so a nil result while the master
// should still be serving is also a failure.
func serverStopExitCode(log *slog.Logger, name string, err error, leaderCtx context.Context) int {
	if err != nil {
		log.Error(name+" server stopped", slog.String("err", err.Error()))
		return exitFailure
	}
	if leaderCtx.Err() == nil {
		log.Error(name + " server stopped unexpectedly")
		return exitFailure
	}
	return exitOK
}

type committingRunReconciler interface {
	ReconcileCommittingRuns(context.Context) error
}

func runCommittingReconciliationLoop(ctx context.Context, interval, timeout time.Duration, reconciler committingRunReconciler, log *slog.Logger) {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	if log == nil {
		log = slog.Default()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer grpcapi.RecoverPanic(log, "committing-run reconciliation")
				reconcileCtx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				if err := reconciler.ReconcileCommittingRuns(reconcileCtx); err != nil && ctx.Err() == nil {
					log.Warn("live committing-run reconciliation failed", slog.String("err", err.Error()))
				}
			}()
		}
	}
}

// runPeriodic calls tick every interval until ctx ends. A panic in one tick is
// logged and the loop continues, so a background job cannot crash the master.
func runPeriodic(ctx context.Context, log *slog.Logger, name string, interval time.Duration, tick func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			func() {
				defer grpcapi.RecoverPanic(log, name)
				tick()
			}()
		}
	}
}
