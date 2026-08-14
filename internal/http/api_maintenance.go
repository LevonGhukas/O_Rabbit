package httpapi

import (
	"encoding/json"
	"net/http"
)

type maintenanceSubmitRequest struct {
	Operation string `json:"operation"` // "compact" or "vacuum"
	Iceberg   struct {
		Enabled    bool   `json:"enabled"`
		Engine     string `json:"engine"`
		Table      string `json:"table"`
		ConfigYAML string `json:"config_yaml"`
	} `json:"iceberg"`
}

func (s *Server) handleMaintenanceSubmit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}

	var req maintenanceSubmitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeInvalidInput(w, "Invalid JSON payload", nil)
		return
	}

	if req.Operation != "compact" && req.Operation != "vacuum" {
		writeInvalidInput(w, "operation must be 'compact' or 'vacuum'", nil)
		return
	}

	s.log.Info("Received maintenance request",
		"operation", req.Operation,
		"iceberg_table", req.Iceberg.Table,
	)

	// Here we simulate the successful parsing and acceptance of the command.
	// In the future, this will dispatch to a background worker or an Iceberg-Go routine.
	res := map[string]string{
		"status":    "submitted",
		"operation": req.Operation,
		"table":     req.Iceberg.Table,
	}
	writeJSON(w, http.StatusOK, res)
}
