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

func main() {
	cfg := loadMasterConfigFromEnv()
	bindMasterFlags(&cfg)
	flag.Parse()

	log := newMasterLogger(cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(log)
	if err := cfg.validateLeasePolicy(); err != nil {
		log.Error("invalid task lease configuration", slog.String("err", err.Error()))
		os.Exit(2)
	}
	if err := cfg.validateAuthentication(); err != nil {
		log.Error("invalid control-plane authentication configuration", slog.String("err", err.Error()))
		os.Exit(2)
	}

	k, err := crypto.LoadMasterKeyFromEnv()
	if err != nil {
		log.Error("load master key", slog.String("err", err.Error()))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	instanceID, err := db.NewMasterInstanceID()
	if err != nil {
		log.Error("create master instance identity", slog.String("err", err.Error()))
		os.Exit(1)
	}
	processLock, err := db.AcquireMasterProcessLock(cfg.DBPath, instanceID)
	if err != nil {
		log.Error("acquire local master singleton lock", slog.String("err", err.Error()))
		os.Exit(1)
	}
	defer processLock.Close()
	cfg.DBPath = processLock.DatabasePath

	st, err := db.Open(ctx, db.Config{Path: cfg.DBPath}, log)
	if err != nil {
		log.Error("open db", slog.String("err", err.Error()))
		os.Exit(1)
	}
	defer st.Close()
	st.SetMaxActiveRuns(cfg.MaxActiveRuns)
	lease, err := st.AcquireLeadership(ctx, instanceID, cfg.LeadershipLeaseDuration, map[string]any{"pid": os.Getpid(), "database_identity": processLock.Identity})
	if err != nil {
		log.Error("acquire durable master leadership", slog.String("err", err.Error()))
		os.Exit(1)
	}
	if err := st.ActivateLeadershipFence(ctx, instanceID, lease.Epoch); err != nil {
		log.Error("activate master mutation fence", slog.String("err", err.Error()))
		os.Exit(1)
	}
	leadership, err := db.NewLeadershipController(st, lease, cfg.LeadershipLeaseDuration, cfg.LeadershipRenewInterval, processLock.Identity)
	if err != nil {
		log.Error("configure master leadership", slog.String("err", err.Error()))
		os.Exit(1)
	}
	leaderCtx := leadership.Start(ctx)
	defer leadership.Stop(context.Background())

	bc := httpapi.NewBroadcaster(log)

	icebergMgr := icebergreg.NewManager(log, icebergreg.ManagerConfig{IceBinary: cfg.IceBin})
	grpcSrv := grpcapi.NewServer(log, st, bc, k, 5*time.Second, icebergMgr)
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
		return
	}
	if n, err := grpcSrv.ExpireLeases(reconcileCtx); err != nil {
		log.Error("reconcile expired task leases", slog.String("err", err.Error()))
		st.RecordLeadershipEvent(context.Background(), instanceID, lease.Epoch, "MASTER_RECOVERY_FAILED", map[string]any{"phase": "task_leases"})
		reconcileCancel()
		return
	} else if n > 0 {
		log.Info("reconciled expired task leases", slog.Int("count", n))
	}
	reconcileCancel()
	registrationPolicy := db.RegistrationPolicy{LeaseDuration: 30 * time.Second, MaxAttempts: 5, BackoffBase: time.Second, BackoffMax: time.Minute}
	if classified, err := st.ReconcileHistoricalRegistrations(leaderCtx, time.Now()); err != nil {
		log.Error("classify historical registrations", slog.String("err", err.Error()))
		st.RecordLeadershipEvent(context.Background(), instanceID, lease.Epoch, "MASTER_RECOVERY_FAILED", map[string]any{"phase": "historical_registrations"})
		return
	} else if len(classified) > 0 {
		log.Info("classified historical registrations", slog.Int("count", len(classified)))
	}
	if n, err := st.ExpireRegistrationAttempts(leaderCtx, time.Now(), registrationPolicy); err != nil {
		log.Error("reconcile expired registration leases", slog.String("err", err.Error()))
		st.RecordLeadershipEvent(context.Background(), instanceID, lease.Epoch, "MASTER_RECOVERY_FAILED", map[string]any{"phase": "registration_leases"})
		return
	} else if n > 0 {
		log.Info("reconciled expired registration leases", slog.Int("count", n))
	}
	if n, err := st.ExpireReconciliationAttempts(leaderCtx, time.Now(), time.Second, 5); err != nil {
		log.Error("reconcile expired catalog-observation leases", slog.String("err", err.Error()))
		st.RecordLeadershipEvent(context.Background(), instanceID, lease.Epoch, "MASTER_RECOVERY_FAILED", map[string]any{"phase": "catalog_reconciliation_leases"})
		return
	} else if n > 0 {
		log.Info("reconciled expired catalog-observation leases", slog.Int("count", n))
	}
	st.RecordLeadershipEvent(leaderCtx, instanceID, lease.Epoch, "MASTER_RECOVERY_COMPLETED", nil)
	leadership.SetReady(true)
	grpcSrv.SetLeadershipGuard(leadership)
	go runCommittingReconciliationLoop(leaderCtx, 2*time.Second, 30*time.Minute, grpcSrv, log)
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-leaderCtx.Done():
				return
			case <-ticker.C:
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
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(cfg.TaskLeaseScanInterval)
		defer ticker.Stop()
		for {
			select {
			case <-leaderCtx.Done():
				return
			case <-ticker.C:
				if _, err := grpcSrv.ExpireLeases(leaderCtx); err != nil && leaderCtx.Err() == nil {
					log.Warn("task lease expiration scan failed", slog.String("err", err.Error()))
				}
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(cfg.MultipartCleanupScanInterval)
		defer ticker.Stop()
		for {
			select {
			case <-leaderCtx.Done():
				return
			case <-ticker.C:
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
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(cfg.CanceledObjectCleanupScanInterval)
		defer ticker.Stop()
		for {
			select {
			case <-leaderCtx.Done():
				return
			case <-ticker.C:
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
			}
		}
	}()

	httpErr := make(chan error, 1)
	httpSrv := httpapi.NewServer(log, st, bc, k, httpapi.StatusInfo{PID: os.Getpid(), HTTPAddr: cfg.HTTPAddr, GRPCAddr: cfg.GRPCAddr, DBPath: processLock.Identity}, cfg.HTTPAuthToken)
	httpSrv.SetLeadershipGuard(leadership)
	httpSrv.SetOperability(cfg.TaskMaxAttempts, grpcSrv)
	go func() {
		httpErr <- httpSrv.Serve(leaderCtx, cfg.HTTPAddr)
	}()

	gcfg := grpcapi.Config{Addr: cfg.GRPCAddr, Insecure: cfg.Insecure, TLSCertFile: cfg.TLSCert, TLSKeyFile: cfg.TLSKey, WorkerAuthToken: cfg.WorkerAuthToken, HeartbeatInterval: 5 * time.Second}
	grpcErr := make(chan error, 1)
	go func() {
		grpcErr <- grpcapi.ListenAndServe(leaderCtx, gcfg, grpcSrv)
	}()

	log.Info("master started",
		slog.String("http", cfg.HTTPAddr),
		slog.String("grpc", cfg.GRPCAddr),
		slog.Bool("http_auth", strings.TrimSpace(cfg.HTTPAuthToken) != ""),
		slog.Bool("worker_auth", strings.TrimSpace(cfg.WorkerAuthToken) != ""),
		slog.Bool("insecure", cfg.Insecure),
		slog.String("iceberg_registration", "persisted-run-snapshot"),
		slog.String("ice_binary", cfg.IceBin),
		slog.String("log_level", cfg.LogLevel),
		slog.String("log_format", cfg.LogFormat),
		slog.String("instance_id", instanceID),
		slog.Int64("leadership_epoch", lease.Epoch),
	)

	select {
	case <-leaderCtx.Done():
		log.Info("master leadership context stopped", slog.String("state", leadership.Status().State))
		return
	case err := <-httpErr:
		if err != nil {
			log.Error("http server stopped", slog.String("err", err.Error()))
			os.Exit(1)
		}
	case err := <-grpcErr:
		if err != nil {
			log.Error("grpc server stopped", slog.String("err", err.Error()))
			os.Exit(1)
		}
	}
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
			reconcileCtx, cancel := context.WithTimeout(ctx, timeout)
			err := reconciler.ReconcileCommittingRuns(reconcileCtx)
			cancel()
			if err != nil && ctx.Err() == nil {
				log.Warn("live committing-run reconciliation failed", slog.String("err", err.Error()))
			}
		}
	}
}
