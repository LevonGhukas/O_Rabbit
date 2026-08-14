package httperr

import (
	"encoding/json"
	"net/http"
)

type Code string

const (
	CodeInvalidInput          Code = "invalid_input"
	CodeNotFound              Code = "not_found"
	CodeConflict              Code = "conflict"
	CodeUnauthorized          Code = "unauthorized"
	CodeDependencyUnavailable Code = "dependency_unavailable"
	CodeInternalError         Code = "internal_error"
	CodeMethodNotAllowed      Code = "method_not_allowed"
	CodeDatasetBusy           Code = "dataset_busy"
	CodeNotImplemented        Code = "not_implemented"
)

type Response struct {
	Error APIError `json:"error"`
}

type APIError struct {
	Code      Code   `json:"code"`
	Message   string `json:"message"`
	Details   any    `json:"details,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

func Write(w http.ResponseWriter, status int, code Code, message string, details any) {
	WriteWithRequestID(w, status, code, message, details, "")
}

func WriteWithRequestID(w http.ResponseWriter, status int, code Code, message string, details any, requestID string) {
	if code == "" {
		code = CodeForStatus(status)
	}
	resp := Response{
		Error: APIError{
			Code:      code,
			Message:   message,
			Details:   details,
			RequestID: requestID,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(resp)
}

func CodeForStatus(status int) Code {
	switch status {
	case http.StatusBadRequest:
		return CodeInvalidInput
	case http.StatusUnauthorized:
		return CodeUnauthorized
	case http.StatusNotFound:
		return CodeNotFound
	case http.StatusMethodNotAllowed:
		return CodeMethodNotAllowed
	case http.StatusConflict:
		return CodeConflict
	case http.StatusServiceUnavailable:
		return CodeDependencyUnavailable
	default:
		return CodeInternalError
	}
}
