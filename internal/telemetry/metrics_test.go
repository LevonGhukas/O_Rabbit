package telemetry

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPMiddlewareLabelsByRoutePattern(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/runs/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTeapot) })
	h := HTTPMiddleware(mux)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/runs/abc123", nil))

	rec := httptest.NewRecorder()
	if err := WriteText(rec); err != nil {
		t.Fatal(err)
	}
	out := rec.Body.String()
	if !strings.Contains(out, `orabbit_http_requests_total{code="418",route="/runs/"} 1`) {
		t.Fatalf("route-labelled counter missing:\n%s", out)
	}
	if strings.Contains(out, "abc123") {
		t.Fatal("raw path leaked into metric labels")
	}
}
