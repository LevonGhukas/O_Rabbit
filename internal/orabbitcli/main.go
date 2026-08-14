// Package orabbitcli contains the shared implementation behind the
// orabbit-client entrypoint.
package orabbitcli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
)

const (
	CLIName = "orabbit-client"

	defaultHTTPAddr = ":9100"
	defaultGRPCAddr = ":9102"

	defaultDBPath = "./master.sqlite"

	defaultGOCache = "/tmp/orabbit-client-gocache"

	defaultMasterHelperBinary = "orabbit-master"
	defaultWorkerHelperBinary = "orabbit-worker"
)

// Main is the CLI process entrypoint.
// It exists to dispatch subcommands and return process exit codes.
func Main(args []string) int {
	if len(args) < 1 {
		usage(os.Stderr)
		return exitUsage
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return cliRootCommand().invoke(ctx, args)
}

// cmdStart executes the start command workflow.
// It exists to keep command orchestration isolated from shared helpers.
func cmdStart(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("stack start", flag.ContinueOnError)

	// Positional: components: master, worker
	components, rest := splitPositionals(args)

	startMaster := has(components, "master")
	startWorker := has(components, "worker")

	dbPath := fs.String("db", defaultDBPath, "SQLite DB path")
	httpAddr := fs.String("http-addr", defaultHTTPAddr, "HTTP listen address")
	grpcAddr := fs.String("grpc-addr", defaultGRPCAddr, "gRPC listen address")
	masterAddr := fs.String("master-addr", "", "Master gRPC address for workers (defaults to local -grpc-addr)")
	gocache := fs.String("gocache", defaultGOCache, "Managed local daemon runtime dir")
	masterBin := fs.String("master-bin", "", "Path to the orabbit-master binary (defaults to a sibling next to orabbit-client)")
	workerBin := fs.String("worker-bin", "", "Path to the orabbit-worker binary (defaults to a sibling next to orabbit-client)")
	workerLogLevel := fs.String("worker-log-level", "", "Log level for local workers: DEBUG, INFO, WARN, ERROR (defaults to worker env/default)")
	workerLogFormat := fs.String("worker-log-format", "", "Log format for local workers: json or text (defaults to worker env/default)")
	quiet := fs.Bool("quiet", false, "Suppress non-essential client output and helper daemon logs")
	verbose := fs.Bool("verbose", false, "Show additional client progress details")
	tlsCert := fs.String("tls-cert", "", "Master gRPC TLS cert file (required when -insecure=false and starting master)")
	tlsKey := fs.String("tls-key", "", "Master gRPC TLS key file (required when -insecure=false and starting master)")
	tlsCA := fs.String("tls-ca", "", "Worker CA cert for master gRPC TLS (optional)")
	tlsServerName := fs.String("tls-server-name", "", "Expected server name for worker gRPC TLS (optional)")

	count := fs.Int("count", 1, "Worker process count")
	insecure := fs.Bool("insecure", true, "Disable gRPC TLS (dev)")

	if handled, code := parseCommandFlags(fs, rest, renderStackStartHelp); handled {
		return code
	}
	if err := validateOutputMode(*quiet, *verbose); err != nil {
		fmt.Fprintln(os.Stderr, err)
		renderStackStartHelp(os.Stderr, fs)
		return exitUsage
	}
	if err := validateWorkerLogSettings(*workerLogLevel, *workerLogFormat); err != nil {
		fmt.Fprintln(os.Stderr, err)
		renderStackStartHelp(os.Stderr, fs)
		return exitUsage
	}
	out := newCLIOutput(*quiet, *verbose)
	defer out.wait()

	if !startMaster && !startWorker {
		fmt.Fprintln(os.Stderr, "stack start: need at least one component: master and/or worker")
		return exitUsage
	}
	if startWorker && *count <= 0 {
		fmt.Fprintln(os.Stderr, "--count must be >= 1")
		return exitUsage
	}
	if startMaster && !*insecure && (strings.TrimSpace(*tlsCert) == "" || strings.TrimSpace(*tlsKey) == "") {
		fmt.Fprintln(os.Stderr, "starting master with TLS requires both --tls-cert and --tls-key")
		return exitUsage
	}

	parentCtx := ctx
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	var pg procGroup
	supervisor := newLocalSupervisor(*gocache, out)

	var masterProc *managedProcessHandle
	if startMaster {
		httpURL := "http://127.0.0.1" + normalizeListenAddr(*httpAddr)
		grpcTarget := localGRPCTarget(*grpcAddr)
		if checkHealth(ctx, httpURL) && checkGRPCTCPHealth(ctx, grpcTarget) {
			out.infof("reusing already-healthy master at %s", httpURL)
			startMaster = false
		} else {
			managedMasterBin, err := resolveManagedDaemonBinary(masterBinarySpec, *masterBin, *gocache)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return exitOperational
			}
			out.infof("starting master at http=%s grpc=%s", httpURL, grpcTarget)
			out.debugf("using master binary %s", managedMasterBin)
			masterProc, err = supervisor.startMaster(ctx, ctx, managedMasterBin, *dbPath, *httpAddr, *grpcAddr, *insecure, *tlsCert, *tlsKey)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return exitCode(err)
			}
			pg.add(masterProc)

			if err := supervisor.waitMasterReady(ctx, httpURL, 10*time.Second); err != nil {
				fmt.Fprintln(os.Stderr, "master did not become ready:", err)
				pg.signal(syscall.SIGTERM)
				pg.wait()
				_, _ = supervisor.reconcileManagedState()
				return exitCode(err)
			}
		}
	}

	if startWorker {
		masterGRPC := strings.TrimSpace(*masterAddr)
		if masterGRPC == "" {
			if startMaster {
				masterGRPC = localGRPCTarget(*grpcAddr)
			} else {
				masterGRPC = normalizeGRPCTarget(*grpcAddr)
			}
		}
		if masterGRPC == "" {
			fmt.Fprintln(os.Stderr, "stack start: worker needs a master gRPC address")
			pg.signal(syscall.SIGTERM)
			pg.wait()
			return exitUsage
		}
		out.infof("starting %d worker(s) against %s", *count, masterGRPC)

		var wg sync.WaitGroup
		errCh := make(chan error, 1)
		managedWorkerBin, err := resolveManagedDaemonBinary(workerBinarySpec, *workerBin, *gocache)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			pg.signal(syscall.SIGTERM)
			pg.wait()
			return exitOperational
		}
		out.debugf("using worker binary %s", managedWorkerBin)
		for i := 0; i < *count; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				workerID := fmt.Sprintf("local-%02d", i+1)
				out.debugf("launching worker %s", workerID)
				proc, err := supervisor.startWorker(localWorkerLaunchSpec{
					CommandCtx:    ctx,
					FollowCtx:     ctx,
					BinaryPath:    managedWorkerBin,
					MasterAddr:    masterGRPC,
					WorkerID:      workerID,
					Insecure:      *insecure,
					LogLevel:      *workerLogLevel,
					LogFormat:     *workerLogFormat,
					TLSCA:         *tlsCA,
					TLSServerName: *tlsServerName,
				})
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
				pg.add(proc)
				if err := proc.wait(); err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
				select {
				case errCh <- nil:
				default:
				}
				cancel()
			}(i)
		}

		select {
		case err := <-errCh:
			if err != nil {
				fmt.Fprintln(os.Stderr, "worker exited:", err)
			}
			cancel()
			pg.signal(syscall.SIGTERM)
			wg.Wait()
			pg.wait()
			_, _ = supervisor.reconcileManagedState()
			return exitCode(err)
		case <-parentCtx.Done():
			cancel()
			pg.signal(syscall.SIGTERM)
			wg.Wait()
			pg.wait()
			_, _ = supervisor.reconcileManagedState()
			return exitCode(parentCtx.Err())
		}
	}

	// Master only: wait until interrupted or master exits.
	if masterProc == nil && !startWorker {
		return exitSuccess
	}
	if masterProc != nil {
		errCh := make(chan error, 1)
		go func() { errCh <- masterProc.wait() }()
		select {
		case <-parentCtx.Done():
			cancel()
			pg.signal(syscall.SIGTERM)
			pg.wait()
			_, _ = supervisor.reconcileManagedState()
			return exitCode(parentCtx.Err())
		case err := <-errCh:
			if err != nil {
				fmt.Fprintln(os.Stderr, "master exited:", err)
				return exitCode(err)
			}
			return exitSuccess
		}
	}
	return exitSuccess
}

// cmdRun executes the run command group workflow.
// It exists to route run subcommands and keep group help behavior in one place.
func cmdRun(ctx context.Context, args []string) int {
	if len(args) == 0 {
		renderRunGroupHelp(os.Stdout)
		return exitSuccess
	}

	switch strings.TrimSpace(args[0]) {
	case "interactive":
		return cmdRunInteractive(ctx, args[1:])
	case "submit":
		return cmdRunSubmit(ctx, args[1:])
	case "watch":
		return cmdRunWatch(ctx, args[1:])
	case "cancel":
		return cmdRunCancel(ctx, args[1:])
	case "diagnose":
		return cmdRunDiagnose(ctx, args[1:])
	case "recover":
		return cmdRunRecover(ctx, args[1:])
	case "help", "-h", "--help":
		renderRunGroupHelp(os.Stdout)
		return exitSuccess
	}

	if strings.HasPrefix(args[0], "-") {
		fmt.Fprintln(os.Stderr, "run requires a subcommand")
	} else {
		fmt.Fprintf(os.Stderr, "unknown run subcommand %q\n", strings.TrimSpace(args[0]))
	}
	renderRunGroupHelp(os.Stderr)
	return exitUsage
}

// cmdRunInteractive executes the interactive guided run workflow.
// It exists to keep interactive prompting and local orchestration isolated from run subcommand routing.
func cmdRunInteractive(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("run interactive", flag.ContinueOnError)

	httpAddr := fs.String("http-addr", defaultHTTPAddr, "Master HTTP listen addr (when starting stack)")
	grpcAddr := fs.String("grpc-addr", defaultGRPCAddr, "Master gRPC listen addr (when starting stack)")
	masterHTTP := fs.String("master-http", "", "Existing master HTTP base URL (for remote/distributed runs)")
	masterGRPC := fs.String("master-grpc", "", "Master gRPC address for local workers when using an existing master")
	quiet := fs.Bool("quiet", false, "Suppress non-essential client output, daemon logs, and streamed run chatter")
	verbose := fs.Bool("verbose", false, "Show additional client progress details")
	localWorkers := fs.Bool("local-workers", true, "Start local worker processes on this machine")
	advanced := fs.Bool("advanced", false, "Show advanced interactive options")
	workerLogLevel := fs.String("worker-log-level", "", "Log level for local workers: DEBUG, INFO, WARN, ERROR (defaults to worker env/default)")
	workerLogFormat := fs.String("worker-log-format", "", "Log format for local workers: json or text (defaults to worker env/default)")
	workers := fs.Int("count", 10, "Worker count (when starting stack)")
	autoMaxInFlight := fs.Int("auto-max-in-flight", envPositiveInt(envAutoMaxInFlight), "Override planner auto max_in_flight_tasks (0 = heuristic)")
	dbPath := fs.String("db", defaultDBPath, "Master SQLite path (when starting stack)")
	gocache := fs.String("gocache", defaultGOCache, "Managed local daemon runtime dir")
	masterBin := fs.String("master-bin", "", "Path to the orabbit-master binary (defaults to a sibling next to orabbit-client)")
	workerBin := fs.String("worker-bin", "", "Path to the orabbit-worker binary (defaults to a sibling next to orabbit-client)")
	autoIceberg := fs.Bool("auto-iceberg", true, "After a successful run, register Parquet parts into an Iceberg REST catalog")
	icebergEngine := fs.String("iceberg-engine", "rest-go", "Iceberg registration engine: rest-go (default) or ice")
	iceBin := fs.String("ice-bin", "ice", "Deprecated: ignored by master-owned registration; master uses its in-container ice binary")
	iceConfig := fs.String("ice-config", ".ice.yaml", "Iceberg config file path to snapshot into the run registration config")
	iceTable := fs.String("ice-table", "", "Iceberg table name (namespace.table) (default: derived)")

	if handled, code := parseCommandFlags(fs, args, renderRunInteractiveHelp); handled {
		return code
	}
	if err := validateOutputMode(*quiet, *verbose); err != nil {
		fmt.Fprintln(os.Stderr, err)
		renderRunInteractiveHelp(os.Stderr, fs)
		return exitUsage
	}
	if err := validateWorkerLogSettings(*workerLogLevel, *workerLogFormat); err != nil {
		fmt.Fprintln(os.Stderr, err)
		renderRunInteractiveHelp(os.Stderr, fs)
		return exitUsage
	}
	out := newCLIOutput(*quiet, *verbose)
	defer out.wait()
	visitedFlags := map[string]bool{}
	fs.Visit(func(f *flag.Flag) {
		visitedFlags[f.Name] = true
	})

	if err := requireInteractiveTTY(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(err)
	}

	prompts, err := newTTYPromptSession()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitOperational
	}
	defer func() {
		if cerr := prompts.Close(); cerr != nil {
			fmt.Fprintln(os.Stderr, "restore terminal:", cerr)
		}
	}()

	cfg := ranConfig{
		HTTPBase:          localHTTPBase(*httpAddr),
		GRPCAddr:          localGRPCTarget(*grpcAddr),
		StartStack:        true,
		StartLocalWorkers: *localWorkers,
		Workers:           *workers,
		DBPath:            *dbPath,
		GOCache:           *gocache,
		MasterBin:         *masterBin,
		WorkerBin:         *workerBin,
		WorkerLogLevel:    *workerLogLevel,
		WorkerLogFormat:   *workerLogFormat,

		AutoIceberg:   *autoIceberg,
		IcebergEngine: *icebergEngine,
		IceBin:        *iceBin,
		IceConfig:     *iceConfig,
		IceTable:      *iceTable,

		SourceName:   "mssql",
		SourceEngine: "mssql",
		SourceDSN:    "sqlserver://sa:YourStrong(!)Password@localhost:1433?database=master&encrypt=disable&trustServerCertificate=true",
		SourceSQL:    "",

		TargetName:        "s3",
		S3Endpoint:        "http://localhost:9000",
		S3Region:          "us-east-1",
		S3Bucket:          "bucket1",
		S3Prefix:          "",
		S3ForcePathStyle:  true,
		S3AccessKeyID:     "minioadmin",
		S3SecretAccessKey: "minioadmin",

		JobName:         "export",
		TargetNamespace: "orders",
		TargetTable:     "Orders",
		WriteMode:       "append",
		Incremental:     false,
		Table:           "SalesDB.dbo.BigTable4",
		IDColumn:        "RowId",
		PlannedTasks:    2,
		ChunkSize:       105000,
		FetchLimit:      50000,

		AutoTune:          true,
		MaxInFlightTasks:  0,
		TargetRowsPerTask: 200000,
	}
	if v := strings.TrimSpace(*masterHTTP); v != "" {
		cfg.HTTPBase = normalizeHTTPBase(v)
		cfg.StartStack = false
	}
	if v := strings.TrimSpace(*masterGRPC); v != "" {
		cfg.GRPCAddr = normalizeGRPCTarget(v)
	} else if !cfg.StartStack {
		cfg.GRPCAddr = guessGRPCTargetFromHTTPBase(cfg.HTTPBase)
	}

	masterAlreadyRunning := checkHealth(ctx, cfg.HTTPBase)
	if masterAlreadyRunning && strings.TrimSpace(cfg.GRPCAddr) != "" {
		masterAlreadyRunning = checkGRPCTCPHealth(ctx, cfg.GRPCAddr)
	}
	if masterAlreadyRunning {
		cfg.StartStack = false
	}
	if !flagWasProvided("local-workers", visitedFlags) {
		cfg.StartLocalWorkers = defaultStartLocalWorkers(cfg)
	}

	if err := promptRunConfig(ctx, prompts, &cfg, *autoMaxInFlight, masterAlreadyRunning, *advanced); err != nil {
		if errors.Is(err, ErrPromptAborted) {
			return exitSuccess
		}
		if !errors.Is(err, ErrInterrupted) {
			fmt.Fprintln(os.Stderr, err)
		}
		return exitCode(err)
	}
	if err := prompts.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "restore terminal:", err)
		return exitOperational
	}
	prompts = nil

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var pg procGroup
	supervisor := newLocalSupervisor(cfg.GOCache, out)
	if cfg.StartStack {
		// Start master only; workers are auto-scaled per run after planning.
		if !masterAlreadyRunning {
			managedMasterBin, err := resolveManagedDaemonBinary(masterBinarySpec, cfg.MasterBin, cfg.GOCache)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return exitOperational
			}
			out.infof("starting local master at %s", cfg.HTTPBase)
			out.debugf("using master binary %s", managedMasterBin)
			masterProc, err := supervisor.startMaster(context.TODO(), runCtx, managedMasterBin, cfg.DBPath, *httpAddr, *grpcAddr, true, "", "")
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return exitCode(err)
			}
			pg.add(masterProc)
			if err := supervisor.waitMasterReady(runCtx, cfg.HTTPBase, 10*time.Second); err != nil {
				fmt.Fprintln(os.Stderr, "master did not become ready:", err)
				// If master/workers are long-lived, stop excess local workers after the run completes.
				if masterAlreadyRunning {
					_ = trimLocalWorkers(ctx, localWorkerConfig{
						GOCache:  cfg.GOCache,
						HTTPBase: cfg.HTTPBase,
						GRPCAddr: cfg.GRPCAddr,
					}, cfg.Workers)
				}

				pg.signal(syscall.SIGTERM)
				pg.wait()
				_, _ = supervisor.reconcileManagedState()
				return exitCode(err)
			}
		}
	}
	if cfg.StartLocalWorkers && strings.TrimSpace(cfg.GRPCAddr) == "" {
		fmt.Fprintln(os.Stderr, "gRPC target is required when starting local workers")
		pg.signal(syscall.SIGTERM)
		pg.wait()
		_, _ = supervisor.reconcileManagedState()
		return exitUsage
	}
	if cfg.StartStack || cfg.StartLocalWorkers {
		if err := supervisor.waitGRPCReady(runCtx, cfg.GRPCAddr, 5*time.Second); err != nil {
			fmt.Fprintln(os.Stderr, "gRPC did not become ready:", err)
			pg.signal(syscall.SIGTERM)
			pg.wait()
			_, _ = supervisor.reconcileManagedState()
			return exitCode(err)
		}
	}

	jobID, err := prepareRunPlan(runCtx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		pg.signal(syscall.SIGTERM)
		pg.wait()
		_, _ = supervisor.reconcileManagedState()
		return exitCode(err)
	}

	// Start a baseline set of local workers up-front so task assignment can begin immediately.
	baselineWorkers := autoDefaultMaxInFlight()
	if cfg.AutoTune && cfg.MaxInFlightTasks > 0 {
		baselineWorkers = cfg.MaxInFlightTasks
	}
	if !cfg.AutoTune {
		baselineWorkers = cfg.Workers
		if baselineWorkers < 1 {
			baselineWorkers = 1
		}
	}
	if cfg.StartLocalWorkers {
		out.debugf("ensuring %d local worker(s) before starting the run", baselineWorkers)
		if err := ensureLocalWorkers(runCtx, localWorkerConfig{
			GOCache:         cfg.GOCache,
			HTTPBase:        cfg.HTTPBase,
			GRPCAddr:        cfg.GRPCAddr,
			WorkerBin:       cfg.WorkerBin,
			WorkerLogLevel:  cfg.WorkerLogLevel,
			WorkerLogFormat: cfg.WorkerLogFormat,
		}, baselineWorkers, supervisor, &pg, true); err != nil {
			fmt.Fprintln(os.Stderr, "worker autoscale:", err)
		}
	} else {
		baselineWorkers = 0
	}

	out.infof("submitting run")
	registrationConfig, err := buildIcebergRegistrationSnapshot(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		pg.signal(syscall.SIGTERM)
		pg.wait()
		_, _ = supervisor.reconcileManagedState()
		return exitCode(err)
	}
	workersAvailable := 0
	if workers, err := listActiveWorkers(runCtx, cfg.HTTPBase); err == nil {
		workersAvailable = len(workers)
	} else {
		out.debugf("failed to list active workers for benchmark planning summary: %v", err)
	}

	observedStart := time.Now()
	runID, taskCount, err := startRun(runCtx, cfg.HTTPBase, jobID, registrationConfig)
	planningMS := time.Since(observedStart).Milliseconds()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		pg.signal(syscall.SIGTERM)
		pg.wait()
		_, _ = supervisor.reconcileManagedState()
		return exitCode(err)
	}

	fmt.Println("RUN_ID=", runID)

	// Auto-scale local workers for this run based on tuned job options.
	if cfg.AutoTune {
		opts, err := getJobOptions(runCtx, cfg.HTTPBase, jobID)
		if err != nil {
			fmt.Fprintln(os.Stderr, "auto-tune: failed to fetch job options; falling back to local default:", err)
		}
		cfg.MaxInFlightTasks = opts.MaxInFlightTasks
		if cfg.StartLocalWorkers {
			desired := desiredWorkers(taskCount, opts.MaxInFlightTasks)
			cfg.Workers = desired
			if desired != baselineWorkers {
				out.debugf("auto-tune selected %d local worker(s) for %d task(s)", desired, taskCount)
				if err := ensureLocalWorkers(runCtx, localWorkerConfig{
					GOCache:   cfg.GOCache,
					HTTPBase:  cfg.HTTPBase,
					GRPCAddr:  cfg.GRPCAddr,
					WorkerBin: cfg.WorkerBin,
				}, desired, supervisor, &pg, true); err != nil {
					fmt.Fprintln(os.Stderr, "worker autoscale:", err)
				}
			}
		} else {
			cfg.Workers = 0
		}
	}

	// Stream SSE until terminal event.
	out.infof("streaming run events")
	runStatus, sseBench, streamErr := streamSSE(runCtx, cfg.HTTPBase+"/sse?run_id="+runID, out, cfg.AutoIceberg)
	if streamErr != nil {
		if code := exitCode(streamErr); code == exitInterrupted {
			pg.signal(syscall.SIGTERM)
			pg.wait()
			_, _ = supervisor.reconcileManagedState()
			return code
		}
		fmt.Fprintln(os.Stderr, "sse stream:", streamErr)
		if fallbackStatus, serr := getRunStatus(runCtx, cfg.HTTPBase, runID); serr == nil {
			runStatus = strings.ToUpper(strings.TrimSpace(fallbackStatus))
		} else {
			fmt.Fprintln(os.Stderr, "failed to fetch run status:", serr)
			pg.signal(syscall.SIGTERM)
			pg.wait()
			_, _ = supervisor.reconcileManagedState()
			return exitCode(serr)
		}
	}
	finalDetails, err := getRunDetails(runCtx, cfg.HTTPBase, runID)
	if err != nil {
		out.debugf("failed to fetch final run details for benchmark report: %v", err)
	}
	if err == nil && !out.quiet {
		printBenchReport(out, benchReportConfig{
			WorkersAvailable:   workersAvailable,
			MaxInFlightTasks:   cfg.MaxInFlightTasks,
			AutoTune:           cfg.AutoTune,
			PlannedTasks:       cfg.PlannedTasks,
			FetchLimit:         cfg.FetchLimit,
			PlanningMS:         planningMS,
			Source:             benchSourceLabel(cfg),
			Target:             benchTargetLabel(cfg),
			RegistrationEnable: cfg.AutoIceberg,
			RegistrationEngine: registrationEngine(cfg),
		}, runID, runStatus, finalDetails, sseBench, observedStart)
	}

	// Scale local workers down after the run completes (best-effort).
	// The master stays running; use `orabbit-client stack stop --all` to stop everything.
	if cfg.StartLocalWorkers {
		_ = trimLocalWorkers(ctx, localWorkerConfig{
			GOCache:  cfg.GOCache,
			HTTPBase: cfg.HTTPBase,
			GRPCAddr: cfg.GRPCAddr,
		}, cfg.Workers)
	}
	switch strings.ToUpper(strings.TrimSpace(runStatus)) {
	case "SUCCEEDED":
		return exitSuccess
	case "FAILED", "CANCELED":
		return exitOperational
	default:
		return exitOperational
	}
}

func benchSourceLabel(cfg ranConfig) string {
	sourceEngine := normalizeSourceEngine(cfg.SourceEngine)
	sourceName := strings.TrimSpace(cfg.Table)
	if sourceEngine == "flightsql" && strings.TrimSpace(sourceName) == "" {
		sourceName = strings.TrimSpace(cfg.SourceSQL)
	}
	if sourceName == "" {
		sourceName = strings.TrimSpace(cfg.TargetTable)
	}
	return strings.TrimSpace(sourceEngine + " " + sourceName)
}

func benchTargetLabel(cfg ranConfig) string {
	prefix := dataset.Prefix(cfg.S3Prefix, normalizeSourceEngine(cfg.SourceEngine), cfg.Table)
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return "s3://" + strings.TrimSpace(cfg.S3Bucket)
	}
	return "s3://" + strings.TrimSpace(cfg.S3Bucket) + "/" + prefix
}

func cmdRunWatch(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("run watch", flag.ContinueOnError)
	masterHTTP := fs.String("master-http", localHTTPBase(defaultHTTPAddr), "Master HTTP base URL")
	runID, parseArgs := takeLeadingPositionalArg(args)
	if handled, code := parseCommandFlags(fs, parseArgs, renderRunWatchHelp); handled {
		return code
	}
	if runID == "" && len(fs.Args()) == 1 {
		runID = strings.TrimSpace(fs.Args()[0])
	}
	if runID == "" || len(fs.Args()) > 1 {
		fmt.Fprintln(os.Stderr, "run watch: expected exactly one run ID")
		renderRunWatchHelp(os.Stderr, fs)
		return exitUsage
	}
	base := normalizeHTTPBase(*masterHTTP)
	out := newCLIOutput(false, false)
	defer out.wait()

	details, err := getRunDetails(ctx, base, runID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(err)
	}

	initialStatus := strings.ToUpper(strings.TrimSpace(details.Run.Status))
	if initialStatus == "SUCCEEDED" || initialStatus == "FAILED" || initialStatus == "CANCELED" {
		out.eventln("run " + initialStatus)
		return exitStatusForRun(initialStatus)
	}

	out.infof("watching run %s on %s", runID, base)
	runStatus, _, streamErr := streamSSE(ctx, base+"/sse?run_id="+runID, out, false)
	if streamErr != nil {
		if code := exitCode(streamErr); code == exitInterrupted {
			return code
		}
		fmt.Fprintln(os.Stderr, "sse stream:", streamErr)
		fallbackStatus, serr := getRunStatus(ctx, base, runID)
		if serr != nil {
			fmt.Fprintln(os.Stderr, "failed to fetch run status:", serr)
			return exitCode(serr)
		}
		runStatus = strings.ToUpper(strings.TrimSpace(fallbackStatus))
	}
	return exitStatusForRun(runStatus)
}

func cmdRunSubmit(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("run submit", flag.ContinueOnError)
	filePath := fs.String("file", "", "Path to a YAML/JSON run spec file")
	masterHTTP := fs.String("master-http", "", "Master HTTP base URL override (defaults to spec.master.http or http://127.0.0.1:9100)")
	quiet := fs.Bool("quiet", false, "Suppress non-essential client progress output")
	verbose := fs.Bool("verbose", false, "Show additional client progress details")

	if handled, code := parseCommandFlags(fs, args, renderRunSubmitHelp); handled {
		return code
	}
	if err := validateOutputMode(*quiet, *verbose); err != nil {
		fmt.Fprintln(os.Stderr, err)
		renderRunSubmitHelp(os.Stderr, fs)
		return exitUsage
	}
	if len(fs.Args()) != 0 {
		fmt.Fprintln(os.Stderr, "run submit: unexpected positional arguments")
		renderRunSubmitHelp(os.Stderr, fs)
		return exitUsage
	}

	spec, err := loadRunSubmitFile(*filePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(err)
	}
	cfg := spec.toRanConfig(*masterHTTP)
	out := newCLIOutput(*quiet, *verbose)
	defer out.wait()

	out.infof("submitting run via %s", cfg.HTTPBase)
	_, runID, _, err := submitRunPlan(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(err)
	}

	fmt.Printf("submitted run %s\n", runID)
	fmt.Printf("watch with: %s run watch %s\n", CLIName, runID)
	return exitSuccess
}

func cmdRunCancel(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("run cancel", flag.ContinueOnError)
	masterHTTP := fs.String("master-http", localHTTPBase(defaultHTTPAddr), "Master HTTP base URL")
	runID, parseArgs := takeLeadingPositionalArg(args)
	if handled, code := parseCommandFlags(fs, parseArgs, renderRunCancelHelp); handled {
		return code
	}
	if runID == "" && len(fs.Args()) == 1 {
		runID = strings.TrimSpace(fs.Args()[0])
	}
	if runID == "" || len(fs.Args()) > 1 {
		fmt.Fprintln(os.Stderr, "run cancel: expected exactly one run ID")
		renderRunCancelHelp(os.Stderr, fs)
		return exitUsage
	}
	base := normalizeHTTPBase(*masterHTTP)
	resp, err := cancelRun(ctx, base, runID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(err)
	}
	if resp.Canceled {
		fmt.Printf("canceled run %s\n", runID)
		if resp.PendingTasksCanceled > 0 {
			fmt.Printf("pending tasks canceled: %d\n", resp.PendingTasksCanceled)
		}
		return exitSuccess
	}
	fmt.Printf("run %s is already canceled\n", runID)
	return exitSuccess
}

func cmdRunDiagnose(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("run diagnose", flag.ContinueOnError)
	masterHTTP := fs.String("master-http", localHTTPBase(defaultHTTPAddr), "Master HTTP base URL")
	runID, parseArgs := takeLeadingPositionalArg(args)
	if handled, code := parseCommandFlags(fs, parseArgs, renderRunDiagnoseHelp); handled {
		return code
	}
	if runID == "" && len(fs.Args()) == 1 {
		runID = strings.TrimSpace(fs.Args()[0])
	}
	if runID == "" || len(fs.Args()) > 1 {
		fmt.Fprintln(os.Stderr, "run diagnose: expected exactly one run ID")
		renderRunDiagnoseHelp(os.Stderr, fs)
		return exitUsage
	}
	diagnosis, err := diagnoseRun(ctx, normalizeHTTPBase(*masterHTTP), runID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(err)
	}
	body, err := json.MarshalIndent(diagnosis, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitOperational
	}
	fmt.Println(string(body))
	return exitSuccess
}

func cmdRunRecover(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("run recover", flag.ContinueOnError)
	masterHTTP := fs.String("master-http", localHTTPBase(defaultHTTPAddr), "Master HTTP base URL")
	action := fs.String("action", "", "Recovery action: reconcile_commit, registration_retry, reconciliation_retry, acknowledge_quarantine")
	reason := fs.String("reason", "", "Required operator reason recorded in the audit log")
	runID, parseArgs := takeLeadingPositionalArg(args)
	if handled, code := parseCommandFlags(fs, parseArgs, renderRunRecoverHelp); handled {
		return code
	}
	if runID == "" && len(fs.Args()) == 1 {
		runID = strings.TrimSpace(fs.Args()[0])
	}
	if runID == "" || len(fs.Args()) > 1 || strings.TrimSpace(*action) == "" || strings.TrimSpace(*reason) == "" {
		fmt.Fprintln(os.Stderr, "run recover: run ID, --action, and --reason are required")
		renderRunRecoverHelp(os.Stderr, fs)
		return exitUsage
	}
	result, err := recoverRun(ctx, normalizeHTTPBase(*masterHTTP), runID, *action, *reason)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitCode(err)
	}
	fmt.Printf("%s: status=%s changed=%t message=%s\n", result.Action, result.Status, result.Changed, result.Message)
	return exitSuccess
}

func exitStatusForRun(status string) int {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "SUCCEEDED":
		return exitSuccess
	case "FAILED", "CANCELED":
		return exitOperational
	default:
		return exitOperational
	}
}

func takeLeadingPositionalArg(args []string) (string, []string) {
	if len(args) == 0 {
		return "", nil
	}
	first := strings.TrimSpace(args[0])
	if first == "" || strings.HasPrefix(first, "-") {
		return "", args
	}
	return first, args[1:]
}

func promptRunConfig(ctx context.Context, prompts *promptSession, cfg *ranConfig, autoMaxInFlight int, masterAlreadyRunning bool, advanced bool) error {
	if err := prompts.writeWrappedPrompt("Interactive setup", "", ""); err != nil {
		return err
	}
	if err := prompts.writeWrappedPrompt("Press Enter to accept the current default.", "  ", "  "); err != nil {
		return err
	}
	if masterAlreadyRunning {
		if err := prompts.writeWrappedPrompt("Master already running at "+cfg.HTTPBase, "  ", "  "); err != nil {
			return err
		}
	}
	if !cfg.StartStack && !masterAlreadyRunning {
		if err := prompts.writeWrappedPrompt("Using existing master at "+cfg.HTTPBase, "  ", "  "); err != nil {
			return err
		}
	}

	var err error

	if err := prompts.writePromptSection("Source Database"); err != nil {
		return err
	}
	cfg.SourceEngine, err = promptStringField(ctx, prompts, promptFieldSpec{
		Label: sourceEnginePromptLabel(),
		Note:  sourceEnginePromptNote(),
	}, cfg.SourceEngine)
	if err != nil {
		return err
	}
	cfg.SourceEngine = normalizeSourceEngine(cfg.SourceEngine)
	cfg.SourceName = cfg.SourceEngine
	if !connectors.IsKnownSourceEngine(cfg.SourceEngine) {
		return fmt.Errorf("source engine %q is not supported in this build (known: %s)", cfg.SourceEngine, strings.Join(connectors.KnownSourceEngines(), ", "))
	}
	cfg.SourceDSN = defaultSourceDSN(cfg.SourceEngine, cfg.SourceDSN)
	cfg.SourceDSN, err = promptStringField(ctx, prompts, promptFieldSpec{
		Label: sourceDSNPromptLabel(cfg.SourceEngine),
		Note:  sourceDSNPromptNote(cfg.SourceEngine),
	}, cfg.SourceDSN)
	if err != nil {
		return err
	}

	if connectors.SupportsOrderedCursor(cfg.SourceEngine) {
		cfg.SourceSQL = ""
		cfg.Table, err = promptStringField(ctx, prompts, promptFieldSpec{Label: "Source table"}, cfg.Table)
		if err != nil {
			return err
		}
		cfg.IDColumn, err = promptStringField(ctx, prompts, promptFieldSpec{Label: "Cursor / ordering column"}, cfg.IDColumn)
		if err != nil {
			return err
		}
		cfg.Incremental, err = promptBoolField(ctx, prompts, promptFieldSpec{
			Label: "Export only new rows after previous run?",
			Note:  "Uses the cursor / ordering column as the high-water mark.",
		}, cfg.Incremental)
		if err != nil {
			return err
		}
	}

	if err := prompts.writePromptSection("Target Storage"); err != nil {
		return err
	}
	cfg.S3Endpoint, err = promptStringField(ctx, prompts, promptFieldSpec{
		Label: "S3 / MinIO endpoint URL",
		Note:  "Must be reachable from all workers.",
	}, cfg.S3Endpoint)
	if err != nil {
		return err
	}
	cfg.S3Bucket, err = promptStringField(ctx, prompts, promptFieldSpec{Label: "Target bucket"}, cfg.S3Bucket)
	if err != nil {
		return err
	}
	cfg.S3AccessKeyID, err = promptStringField(ctx, prompts, promptFieldSpec{Label: "Access key ID"}, cfg.S3AccessKeyID)
	if err != nil {
		return err
	}
	cfg.S3SecretAccessKey, err = promptSecretStringField(ctx, prompts, promptFieldSpec{Label: "Secret access key"}, cfg.S3SecretAccessKey)
	if err != nil {
		return err
	}

	if advanced {
		if err := prompts.writePromptSection("Advanced Storage"); err != nil {
			return err
		}
		cfg.S3Region, err = promptStringField(ctx, prompts, promptFieldSpec{Label: "S3 region"}, cfg.S3Region)
		if err != nil {
			return err
		}
		cfg.S3Prefix, err = promptStringField(ctx, prompts, promptFieldSpec{
			Label: "S3 prefix override",
			Note:  "Leave empty to derive the prefix automatically.",
		}, cfg.S3Prefix)
		if err != nil {
			return err
		}
		cfg.S3ForcePathStyle, err = promptBoolField(ctx, prompts, promptFieldSpec{Label: "S3 force path style"}, cfg.S3ForcePathStyle)
		if err != nil {
			return err
		}
	}
	if advanced {
		if err := prompts.writePromptSection("Advanced Execution"); err != nil {
			return err
		}
		cfg.StartLocalWorkers, err = promptBoolField(ctx, prompts, promptFieldSpec{
			Label: "Start local worker processes on this machine?",
		}, cfg.StartLocalWorkers)
		if err != nil {
			return err
		}
		if cfg.StartLocalWorkers && !cfg.StartStack {
			cfg.GRPCAddr, err = promptStringField(ctx, prompts, promptFieldSpec{
				Label: "Master gRPC address",
				Note:  "Used by local workers on this machine.",
			}, cfg.GRPCAddr)
			if err != nil {
				return err
			}
			cfg.GRPCAddr = normalizeGRPCTarget(cfg.GRPCAddr)
		}
	}

	switch {
	case cfg.SourceEngine == "flightsql":
		cfg.Incremental = false
		cfg.Table, err = promptStringField(ctx, prompts, promptFieldSpec{
			Label: "Dataset label",
			Note:  "Used for S3 prefix and Iceberg naming.",
		}, cfg.Table)
		if err != nil {
			return err
		}
		if strings.TrimSpace(cfg.SourceSQL) == "" {
			cfg.SourceSQL = fmt.Sprintf("SELECT * FROM %s", cfg.Table)
		}
		cfg.SourceSQL, err = promptStringField(ctx, prompts, promptFieldSpec{
			Label: "Source SQL query",
			Note:  "FlightSQL query text.",
		}, cfg.SourceSQL)
		if err != nil {
			return err
		}
		cfg.IDColumn = ""
		cfg.AutoTune = false
		cfg.MaxInFlightTasks = 1
		cfg.TargetRowsPerTask = 0
		cfg.PlannedTasks = 0
		cfg.ChunkSize = 0
		cfg.FetchLimit = 0
		cfg.StartLocalWorkers = false
		cfg.Workers = 0
	case connectors.SupportsOrderedCursor(cfg.SourceEngine):
		cfg.AutoTune = true
		if advanced {
			cfg.AutoTune, err = promptBoolField(ctx, prompts, promptFieldSpec{
				Label: "Use automatic performance tuning?",
				Note:  "Lets the planner choose chunking, fetch limit, and concurrency.",
			}, true)
			if err != nil {
				return err
			}
		}
		if cfg.AutoTune {
			// Leave knobs at 0 so the master fills them in, unless operator override is provided.
			cfg.MaxInFlightTasks = 0
			if autoMaxInFlight > 0 {
				cfg.MaxInFlightTasks = autoMaxInFlight
			}
			cfg.TargetRowsPerTask = 0
			cfg.PlannedTasks = 0
			cfg.ChunkSize = 0
			cfg.FetchLimit = 0
		} else {
			// Manual mode: user must provide the knobs.
			if advanced && cfg.StartLocalWorkers {
				cfg.Workers, err = promptIntField(ctx, prompts, promptFieldSpec{Label: "Local worker processes"}, cfg.Workers)
				if err != nil {
					return err
				}
				if cfg.Workers < 1 {
					cfg.Workers = 1
				}
			} else {
				cfg.Workers = 0
			}
			maxInFlightDefault := cfg.Workers
			if maxInFlightDefault < 1 {
				maxInFlightDefault = 1
			}
			cfg.MaxInFlightTasks, err = promptIntField(ctx, prompts, promptFieldSpec{Label: "max_in_flight_tasks"}, maxInFlightDefault)
			if err != nil {
				return err
			}
			if cfg.MaxInFlightTasks < 1 {
				cfg.MaxInFlightTasks = 1
			}
			cfg.PlannedTasks, err = promptIntField(ctx, prompts, promptFieldSpec{Label: "planned_tasks"}, cfg.PlannedTasks)
			if err != nil {
				return err
			}
			if cfg.PlannedTasks < 1 {
				cfg.PlannedTasks = 1
			}
			cfg.FetchLimit, err = promptIntField(ctx, prompts, promptFieldSpec{Label: "fetch_limit_rows"}, cfg.FetchLimit)
			if err != nil {
				return err
			}
			if cfg.FetchLimit < 1 {
				cfg.FetchLimit = 1
			}
			cfg.ChunkSize = 0
		}
	default:
		return fmt.Errorf("source engine %q is registered but not yet wired for interactive run flow", cfg.SourceEngine)
	}
	cfg.JobName = defaultJobName(*cfg)
	if cfg.SourceEngine == "flightsql" && strings.TrimSpace(cfg.SourceSQL) == "" {
		return fmt.Errorf("FlightSQL mode requires a non-empty source SQL query")
	}

	if err := prompts.writePromptSection("Iceberg"); err != nil {
		return err
	}
	cfg.AutoIceberg, err = promptBoolField(ctx, prompts, promptFieldSpec{
		Label: "Register output as an Iceberg table?",
	}, cfg.AutoIceberg)
	if err != nil {
		return err
	}
	if cfg.AutoIceberg {
		if strings.TrimSpace(cfg.IcebergEngine) == "" {
			cfg.IcebergEngine = "rest-go"
		}
		if advanced {
			cfg.IcebergEngine, err = promptStringField(ctx, prompts, promptFieldSpec{
				Label: "Iceberg engine",
				Note:  "Choices: rest-go or ice.",
			}, cfg.IcebergEngine)
			if err != nil {
				return err
			}
			if strings.EqualFold(strings.TrimSpace(cfg.IcebergEngine), "rest-go") {
				cfg.IceConfig, err = promptStringField(ctx, prompts, promptFieldSpec{
					Label: "Iceberg REST config file",
				}, cfg.IceConfig)
				if err != nil {
					return err
				}
			} else if strings.EqualFold(strings.TrimSpace(cfg.IcebergEngine), "ice") {
				cfg.IceConfig, err = promptStringField(ctx, prompts, promptFieldSpec{Label: "ice config file"}, cfg.IceConfig)
				if err != nil {
					return err
				}
			}
		}
		if strings.TrimSpace(cfg.IceTable) == "" {
			cfg.IceTable = defaultIceTable(*cfg)
		}
		cfg.IceTable, err = promptStringField(ctx, prompts, promptFieldSpec{
			Label: "Iceberg destination table",
			Note:  "Use namespace.table format.",
		}, cfg.IceTable)
		if err != nil {
			return err
		}
	}

	if connectors.SupportsOrderedCursor(cfg.SourceEngine) {
		if err := prompts.writePromptSection("Performance"); err != nil {
			return err
		}
		if advanced {
			if cfg.AutoTune {
				if err := prompts.writeWrappedPrompt("Automatic performance tuning: enabled", "  ", "  "); err != nil {
					return err
				}
			}
		} else if err := prompts.writeWrappedPrompt("Automatic performance tuning: enabled", "  ", "  "); err != nil {
			return err
		}
	}

	if !advanced && !cfg.StartLocalWorkers {
		cfg.Workers = 0
	}

	if err := prompts.writePromptSection("Review"); err != nil {
		return err
	}
	workersAvailable := 0
	workersKnown := false
	if checkHealth(ctx, cfg.HTTPBase) {
		if workers, err := listActiveWorkers(ctx, cfg.HTTPBase); err == nil {
			workersAvailable = len(workers)
			workersKnown = true
		}
	}
	for _, line := range buildRunReviewSummary(*cfg, workersAvailable, workersKnown, advanced) {
		if err := prompts.writeWrappedPrompt(line, "  ", "  "); err != nil {
			return err
		}
	}
	submit, err := promptBoolField(ctx, prompts, promptFieldSpec{
		Label: "Submit run?",
	}, true)
	if err != nil {
		return err
	}
	if !submit {
		return ErrPromptAborted
	}

	return nil
}

// init performs startup-time process configuration.
// It exists to apply defaults before any command path runs.
func init() {
	_ = runtime.GOMAXPROCS(0)
}
