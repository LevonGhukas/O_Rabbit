package httpapi

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
)

type trackingResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

func (w *trackingResponseWriter) WriteHeader(status int) {
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

func (w *trackingResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(p)
}

func (w *trackingResponseWriter) Flush() {
	if fl, ok := w.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

func (s *Server) withRecoverer(next http.Handler) http.Handler {
	log := s.log
	if log == nil {
		log = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tw := &trackingResponseWriter{ResponseWriter: w}
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("http handler panic recovered",
					slog.Any("panic", rec),
					slog.String("method", r.Method),
					slog.String("path", r.URL.Path),
					slog.String("stack", string(debug.Stack())),
				)
				if !tw.wroteHeader {
					writeInternalError(tw, "internal server error")
				}
			}
		}()
		next.ServeHTTP(tw, r)
	})
}

func isKnownAPIPath(path string) bool {
	switch path {
	case "/healthz", "/ready", "/status", "/metrics", "/workers", "/connections", "/jobs", "/runs", "/sse", "/servers", "/deployments",
		"/api/workers", "/api/runs", "/api/runs/submit", "/api/runs/validate", "/api/source-engines":
		return true
	}
	if strings.HasPrefix(path, "/api/runs/") {
		return true
	}
	if strings.HasPrefix(path, "/api/jobs/") {
		_, isRunCreate, ok := parseJobRoute(strings.TrimPrefix(path, "/api"))
		return ok && isRunCreate
	}
	if path == "/executions" || strings.HasPrefix(path, "/executions/") {
		return true
	}
	if path == "/deployments" || strings.HasPrefix(path, "/deployments/") {
		return true
	}
	if path == "/servers" || strings.HasPrefix(path, "/servers/") {
		return true
	}
	if id := strings.Trim(strings.TrimPrefix(path, "/connections/"), "/"); id != "" && strings.HasPrefix(path, "/connections/") {
		return true
	}
	if id := strings.Trim(strings.TrimPrefix(path, "/runs/"), "/"); id != "" && strings.HasPrefix(path, "/runs/") {
		return true
	}
	_, _, ok := parseJobRoute(path)
	return ok
}

func parseJobRoute(path string) (jobID string, isRunCreate bool, ok bool) {
	if !strings.HasPrefix(path, "/jobs/") {
		return "", false, false
	}
	raw := strings.TrimPrefix(path, "/jobs/")
	raw = strings.TrimRight(raw, "/")
	if raw == "" {
		return "", false, false
	}
	parts := strings.Split(raw, "/")
	switch len(parts) {
	case 1:
		if strings.TrimSpace(parts[0]) == "" {
			return "", false, false
		}
		return parts[0], false, true
	case 2:
		if strings.TrimSpace(parts[0]) == "" || parts[1] != "runs" {
			return "", false, false
		}
		return parts[0], true, true
	default:
		return "", false, false
	}
}
