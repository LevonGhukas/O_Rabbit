package httperr

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCodeForStatus(t *testing.T) {
	tests := []struct {
		name   string
		status int
		want   Code
	}{
		{
			name:   "bad request",
			status: http.StatusBadRequest,
			want:   CodeInvalidInput,
		},
		{
			name:   "unauthorized",
			status: http.StatusUnauthorized,
			want:   CodeUnauthorized,
		},
		{
			name:   "not found",
			status: http.StatusNotFound,
			want:   CodeNotFound,
		},
		{
			name:   "method not allowed",
			status: http.StatusMethodNotAllowed,
			want:   CodeMethodNotAllowed,
		},
		{
			name:   "conflict",
			status: http.StatusConflict,
			want:   CodeConflict,
		},
		{
			name:   "service unavailable",
			status: http.StatusServiceUnavailable,
			want:   CodeDependencyUnavailable,
		},
		{
			name:   "unknown status",
			status: http.StatusTeapot,
			want:   CodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CodeForStatus(tt.status)
			if got != tt.want {
				t.Fatalf("CodeForStatus(%d) = %q, want %q", tt.status, got, tt.want)
			}
		})
	}
}

func TestWriteWithRequestID(t *testing.T) {
	rec := httptest.NewRecorder()

	WriteWithRequestID(
		rec,
		http.StatusNotFound,
		CodeNotFound,
		"dataset not found",
		map[string]string{
			"dataset": "orders",
		},
		"req-123",
	)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d, want %d", rec.Code, http.StatusNotFound)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Fatalf("content-type=%q, want application/json", contentType)
	}

	var response Response
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if response.Error.Code != CodeNotFound {
		t.Fatalf("code=%q, want %q", response.Error.Code, CodeNotFound)
	}

	if response.Error.Message != "dataset not found" {
		t.Fatalf("message=%q, want dataset not found", response.Error.Message)
	}

	if response.Error.RequestID != "req-123" {
		t.Fatalf("request_id=%q, want req-123", response.Error.RequestID)
	}

	if response.Error.Details == nil {
		t.Fatal("details is nil, want details")
	}
}

func TestWriteUsesStatusCodeWhenCodeEmpty(t *testing.T) {
	rec := httptest.NewRecorder()

	Write(
		rec,
		http.StatusBadRequest,
		"",
		"invalid payload",
		nil,
	)

	var response Response
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if response.Error.Code != CodeInvalidInput {
		t.Fatalf(
			"code=%q, want %q",
			response.Error.Code,
			CodeInvalidInput,
		)
	}
}
