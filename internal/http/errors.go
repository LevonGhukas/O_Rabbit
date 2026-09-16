package httpapi

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/httperr"
	"github.com/LevonGhukas/O_Rabbit/internal/planner"
)

func writeAPIError(w http.ResponseWriter, status int, code httperr.Code, message string, details any) {
	httperr.Write(w, status, code, message, details)
}

func writePayloadTooLarge(w http.ResponseWriter, message string) {
	msg := strings.TrimSpace(message)
	if msg == "" {
		msg = "request payload exceeds maximum allowed size"
	}
	writeAPIError(w, http.StatusRequestEntityTooLarge, httperr.CodePayloadTooLarge, msg, nil)
}

func handleJSONReadError(w http.ResponseWriter, err error) {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		writePayloadTooLarge(w, fmt.Sprintf("request payload exceeds maximum allowed size of %d bytes", maxBytesErr.Limit))
		return
	}
	writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
}

func writeInvalidInput(w http.ResponseWriter, message string, details any) {
	writeAPIError(w, http.StatusBadRequest, httperr.CodeInvalidInput, message, details)
}

func writeUnauthorized(w http.ResponseWriter) {
	writeAPIError(w, http.StatusUnauthorized, httperr.CodeUnauthorized, "unauthorized", nil)
}

func writeNotFound(w http.ResponseWriter, resource string, details any) {
	msg := strings.TrimSpace(resource)
	if msg == "" {
		msg = "resource"
	}
	writeAPIError(w, http.StatusNotFound, httperr.CodeNotFound, fmt.Sprintf("%s not found", msg), details)
}

func writeConflict(w http.ResponseWriter, code httperr.Code, message string, details any) {
	if code == "" {
		code = httperr.CodeConflict
	}
	writeAPIError(w, http.StatusConflict, code, message, details)
}

func writeMethodNotAllowed(w http.ResponseWriter, method string, allowed ...string) {
	if len(allowed) > 0 {
		w.Header().Set("Allow", strings.Join(allowed, ", "))
	}
	details := map[string]any{"method": method}
	if len(allowed) > 0 {
		details["allowed"] = allowed
	}
	writeAPIError(w, http.StatusMethodNotAllowed, httperr.CodeMethodNotAllowed, "method not allowed", details)
}

func writeInternalError(w http.ResponseWriter, message string) {
	msg := strings.TrimSpace(message)
	if msg == "" {
		msg = "internal server error"
	}
	writeAPIError(w, http.StatusInternalServerError, httperr.CodeInternalError, msg, nil)
}

func writeDependencyUnavailable(w http.ResponseWriter, message string) {
	msg := strings.TrimSpace(message)
	if msg == "" {
		msg = "dependency unavailable"
	}
	writeAPIError(w, http.StatusServiceUnavailable, httperr.CodeDependencyUnavailable, msg, nil)
}

func writeNotImplemented(w http.ResponseWriter, message string, details any) {
	msg := strings.TrimSpace(message)
	if msg == "" {
		msg = "not implemented"
	}
	writeAPIError(w, http.StatusNotImplemented, httperr.CodeNotImplemented, msg, details)
}

func invalidJSONDetails(err error) map[string]any {
	if err == nil {
		return nil
	}
	return map[string]any{"cause": err.Error()}
}

func handleLookupError(w http.ResponseWriter, err error, resource string) bool {
	if errors.Is(err, sql.ErrNoRows) {
		writeNotFound(w, resource, nil)
		return true
	}
	return false
}

func writeUnknownRoute(w http.ResponseWriter, path string) {
	writeAPIError(w, http.StatusNotFound, httperr.CodeNotFound, "route not found", map[string]any{"path": path})
}

func writePlannerFailure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, planner.ErrDatasetBusy):
		writeConflict(w, httperr.CodeDatasetBusy, "dataset is busy", nil)
	case err != nil:
		details := err.Error()
		if cause := errors.Unwrap(err); cause != nil {
			details = cause.Error()
		}
		writeAPIError(w, http.StatusInternalServerError, httperr.CodeInternalError, "run planning failed", details)
	default:
		writeInternalError(w, "internal server error")
	}
}
