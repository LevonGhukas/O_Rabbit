// Package telemetry holds O_Rabbit's Prometheus metrics and OpenTelemetry
// tracing setup.
package telemetry

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/common/expfmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// Registry holds every O_Rabbit collector. It is separate from the global
// default registry so only these series are exported.
var Registry = prometheus.NewRegistry()

var (
	grpcHandled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "orabbit_grpc_server_handled_total",
		Help: "Worker control-plane RPCs completed, by method and status code.",
	}, []string{"method", "code"})
	grpcLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "orabbit_grpc_server_handling_seconds",
		Help:    "Worker control-plane RPC latency.",
		Buckets: prometheus.ExponentialBuckets(0.001, 4, 10), // 1ms .. ~4.4m
	}, []string{"method"})

	httpHandled = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "orabbit_http_requests_total",
		Help: "HTTP API requests, by route pattern and status code.",
	}, []string{"route", "code"})
	httpLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "orabbit_http_request_duration_seconds",
		Help:    "HTTP API request latency (SSE streams excluded).",
		Buckets: prometheus.DefBuckets,
	}, []string{"route"})

	taskResults = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "orabbit_task_results_total",
		Help: "Accepted task attempt results, by final task status.",
	}, []string{"status"})
	taskRows = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "orabbit_task_rows_read_total",
		Help: "Source rows read by accepted task attempts.",
	})
	taskBytesRead = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "orabbit_task_bytes_read_total",
		Help: "Source bytes read by accepted task attempts.",
	})
	taskBytesWritten = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "orabbit_task_bytes_written_total",
		Help: "Parquet bytes written by accepted task attempts.",
	})

	commitLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "orabbit_run_commit_duration_seconds",
		Help:    "Duration of one run publication attempt, by outcome.",
		Buckets: prometheus.ExponentialBuckets(0.1, 3, 10), // 100ms .. ~33m
	}, []string{"outcome"})

	catalogSlotsCapacity = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "orabbit_catalog_work_slots_capacity",
		Help: "Catalog work slots: registrations and reconciliations the master runs at once.",
	})
	catalogSlotsInUse = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "orabbit_catalog_work_slots_in_use",
		Help: "Catalog work slots held by running registrations and reconciliations.",
	})
	catalogWorkRejected = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "orabbit_catalog_work_rejected_total",
		Help: "Registration or reconciliation attempts deferred because every catalog work slot was busy.",
	})

	rateLimited = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "orabbit_rate_limited_total",
		Help: "Requests rejected by a rate limit (HTTP 429 or gRPC ResourceExhausted), by surface and reason.",
	}, []string{"surface", "reason"})
	authFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "orabbit_auth_failures_total",
		Help: "Rejected credentials counted against a client's failed-authentication budget, by surface.",
	}, []string{"surface"})
)

func init() {
	Registry.MustRegister(
		grpcHandled, grpcLatency,
		httpHandled, httpLatency,
		taskResults, taskRows, taskBytesRead, taskBytesWritten,
		commitLatency,
		catalogSlotsCapacity, catalogSlotsInUse, catalogWorkRejected,
		rateLimited, authFailures,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
}

// UnaryServerInterceptor records RPC counts and latency. Method names come
// from the service definition, so label cardinality is bounded.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		grpcLatency.WithLabelValues(info.FullMethod).Observe(time.Since(start).Seconds())
		grpcHandled.WithLabelValues(info.FullMethod, status.Code(err).String()).Inc()
		return resp, err
	}
}

// HTTPMiddleware records request counts and latency by ServeMux route
// pattern, never by raw path. Unmatched requests use the "unmatched" route;
// SSE streams are counted but not timed.
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		httpHandled.WithLabelValues(route, strconv.Itoa(rec.status)).Inc()
		if rec.Header().Get("Content-Type") != "text/event-stream" {
			httpLatency.WithLabelValues(route).Observe(time.Since(start).Seconds())
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.wroteHeader {
		r.status, r.wroteHeader = code, true
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	r.wroteHeader = true
	return r.ResponseWriter.Write(b)
}

// Flush keeps SSE streaming working through the recorder.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// ObserveTaskResult records one accepted task attempt result.
func ObserveTaskResult(status string, rowsRead, bytesRead, bytesWritten int64) {
	taskResults.WithLabelValues(status).Inc()
	if rowsRead > 0 {
		taskRows.Add(float64(rowsRead))
	}
	if bytesRead > 0 {
		taskBytesRead.Add(float64(bytesRead))
	}
	if bytesWritten > 0 {
		taskBytesWritten.Add(float64(bytesWritten))
	}
}

// ObserveCommit records one run publication attempt.
func ObserveCommit(outcome string, d time.Duration) {
	commitLatency.WithLabelValues(outcome).Observe(d.Seconds())
}

// SetCatalogWorkCapacity records how many catalog work slots exist.
func SetCatalogWorkCapacity(n int) { catalogSlotsCapacity.Set(float64(n)) }

// ObserveCatalogWork records a catalog slot taken (acquired) or not.
func ObserveCatalogWork(acquired bool) {
	if acquired {
		catalogSlotsInUse.Inc()
		return
	}
	catalogWorkRejected.Inc()
}

// ReleaseCatalogWork records a catalog slot given back.
func ReleaseCatalogWork() { catalogSlotsInUse.Dec() }

// ObserveRateLimited records one request rejected by a rate limit. Callers
// pass fixed surface and reason names, never client data, so cardinality
// stays bounded.
func ObserveRateLimited(surface, reason string) { rateLimited.WithLabelValues(surface, reason).Inc() }

// ObserveAuthFailure records one rejected credential on surface.
func ObserveAuthFailure(surface string) { authFailures.WithLabelValues(surface).Inc() }

// WriteText appends every registered metric in Prometheus text format.
func WriteText(w http.ResponseWriter) error {
	families, err := Registry.Gather()
	if err != nil {
		return err
	}
	enc := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range families {
		if err := enc.Encode(mf); err != nil {
			return err
		}
	}
	return nil
}
