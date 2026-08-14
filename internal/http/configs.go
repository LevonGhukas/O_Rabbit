package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	opsconfigs "github.com/LevonGhukas/O_Rabbit/internal/ops/configs"
)

type configContentRequest struct {
	Content string `json:"content"`
}

func (s *Server) handleServerConfigs(w http.ResponseWriter, r *http.Request, serverID string, parts []string) {
	if len(parts) == 0 {
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, r.Method, http.MethodGet)
			return
		}
		server, target, err := s.loadServerTarget(r.Context(), serverID)
		if err != nil {
			s.writeServerTargetError(w, err)
			return
		}
		items, err := s.configs.ListEditableConfigs(r.Context(), target, server.ProjectDir)
		if err != nil {
			writeDependencyUnavailable(w, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
		return
	}

	configID := strings.TrimSpace(parts[0])
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			s.handleReadConfig(w, r, serverID, configID)
		case http.MethodPut:
			s.handleUpdateConfig(w, r, serverID, configID)
		default:
			writeMethodNotAllowed(w, r.Method, http.MethodGet, http.MethodPut)
		}
		return
	}
	if len(parts) == 2 && parts[1] == "validate" {
		s.handleValidateConfig(w, r, serverID, configID)
		return
	}
	writeUnknownRoute(w, r.URL.Path)
}

func (s *Server) handleReadConfig(w http.ResponseWriter, r *http.Request, serverID string, configID string) {
	server, target, err := s.loadServerTarget(r.Context(), serverID)
	if err != nil {
		s.writeServerTargetError(w, err)
		return
	}
	result, err := s.configs.ReadConfig(r.Context(), target, server.ProjectDir, configID)
	if err != nil {
		if errors.Is(err, db.ErrMasterKeyRequired) {
			writeMasterKeyRequired(w, "config versions")
			return
		}
		writeDependencyUnavailable(w, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleValidateConfig(w http.ResponseWriter, r *http.Request, serverID string, configID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}
	if _, err := s.st.GetServer(r.Context(), serverID); err != nil {
		if handleLookupError(w, err, "server") {
			return
		}
		writeInternalError(w, "failed to fetch server")
		return
	}
	var req configContentRequest
	if err := readJSON(r, &req); err != nil {
		writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
		return
	}
	validation := opsconfigs.ValidateConfig(configID, req.Content)
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         validation.OK,
		"config_id":  configID,
		"validation": validation,
	})
}

func (s *Server) handleUpdateConfig(w http.ResponseWriter, r *http.Request, serverID string, configID string) {
	server, target, err := s.loadServerTarget(r.Context(), serverID)
	if err != nil {
		s.writeServerTargetError(w, err)
		return
	}
	if s.k.IsZero() {
		writeMasterKeyRequired(w, "config versions")
		return
	}

	var req configContentRequest
	if err := readJSON(r, &req); err != nil {
		writeInvalidInput(w, "invalid JSON body", invalidJSONDetails(err))
		return
	}
	validation := opsconfigs.ValidateConfig(configID, req.Content)
	if !validation.OK {
		writeInvalidInput(w, "config validation failed", map[string]any{
			"config_id":  configID,
			"validation": validation,
		})
		return
	}
	validation, err = s.configs.UpdateConfig(r.Context(), target, server.ProjectDir, configID, req.Content)
	if err != nil {
		writeDependencyUnavailable(w, err.Error())
		return
	}
	blob, err := db.EncryptConfigVersionContent(s.k, "server:"+server.ID+":config:"+configID, []byte(req.Content))
	if err != nil {
		if errors.Is(err, db.ErrMasterKeyRequired) {
			writeMasterKeyRequired(w, "config versions")
			return
		}
		writeInternalError(w, "failed to save config version")
		return
	}
	cfg, err := s.st.SaveConfigVersion(r.Context(), db.ConfigVersion{
		ID:                   newID(),
		ServerID:             server.ID,
		ConfigID:             configID,
		ContentEnc:           blob,
		ValidationStatus:     validationStatusString(validation),
		ValidationErrorsJSON: json.RawMessage(toRawJSON(validation.Errors, `[]`)),
	})
	if err != nil {
		writeInternalError(w, "failed to save config version")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":         true,
		"config_id":  configID,
		"version":    cfg.Version,
		"validation": validation,
	})
}
