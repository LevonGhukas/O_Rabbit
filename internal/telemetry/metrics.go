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
)

func init() {
	Registry.MustRegister(
		grpcHandled, grpcLatency,
		httpHandled, httpLatency,
		taskResults, taskRows, taskBytesRead, taskBytesWritten,
		commitLatency,
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
