package httpapi

import (
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

type serverCreateRequest struct {
	Name       string                   `json:"name"`
	Host       string                   `json:"host"`
	SSHPort    int                      `json:"ssh_port"`
	SSHUser    string                   `json:"ssh_user"`
	ProjectDir string                   `json:"project_dir"`
	RoleHints  []string                 `json:"role_hints"`
	Labels     map[string]string        `json:"labels"`
	Credential *serverCredentialRequest `json:"credential,omitempty"`
}

type serverPatchRequest struct {
	Name       *string                  `json:"name,omitempty"`
	Host       *string                  `json:"host,omitempty"`
	SSHPort    *int                     `json:"ssh_port,omitempty"`
	SSHUser    *string                  `json:"ssh_user,omitempty"`
	ProjectDir *string                  `json:"project_dir,omitempty"`
	RoleHints  *[]string                `json:"role_hints,omitempty"`
	Labels     *map[string]string       `json:"labels,omitempty"`
	Credential *serverCredentialRequest `json:"credential,omitempty"`
}

func (s *Server) handleServers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		items, err := s.st.ListServers(r.Context())
		if err != nil {
			writeInternalError(w, "failed to list servers")
			return
		}
		out := make([]serverResponse, 0, len(items))
		for _, item := range items {
			out = append(out, s.serverDTO(item))
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": out})
	case http.MethodPost:
		var req serverCreateRequest
		if err := readJSON(r, &req); err != nil {
			handleJSONReadError(w, err)
			return
		}
		server, err := s.st.CreateServer(r.Context(), db.Server{
			ID:            newID(),
			Name:          strings.TrimSpace(req.Name),
			Host:          strings.TrimSpace(req.Host),
			SSHPort:       req.SSHPort,
			SSHUser:       strings.TrimSpace(req.SSHUser),
			ProjectDir:    strings.TrimSpace(req.ProjectDir),
			RoleHintsJSON: toRawJSON(req.RoleHints, `[]`),
			LabelsJSON:    toRawJSON(req.Labels, `{}`),
		})
		if err != nil {
			writeInvalidInput(w, "invalid server configuration", map[string]any{"cause": err.Error()})
			return
		}
		if req.Credential != nil {
			if s.k.IsZero() {
				_ = s.st.DeleteServer(r.Context(), server.ID)
				writeMasterKeyRequired(w, "SSH credentials")
				return
			}
			if err := s.saveCredentialInput(r.Context(), server, req.Credential); err != nil {
				_ = s.st.DeleteServer(r.Context(), server.ID)
				writeInvalidInput(w, "invalid server credential", map[string]any{"cause": err.Error()})
				return
			}
		}
		writeJSON(w, http.StatusCreated, s.serverDTO(server))
	default:
		writeMethodNotAllowed(w, r.Method, http.MethodGet, http.MethodPost)
	}
}

func (s *Server) handleServerByID(w http.ResponseWriter, r *http.Request) {
	serverID, parts, ok := parseServerRoute(r.URL.Path)
	if !ok {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	if len(parts) == 0 {
		s.handleServerResource(w, r, serverID)
		return
	}
	switch parts[0] {
	case "ssh":
		if len(parts) == 2 && parts[1] == "test" {
			s.handleServerSSHTest(w, r, serverID)
			return
		}
	case "project":
		if len(parts) == 2 && parts[1] == "validate" {
			s.handleServerProjectValidate(w, r, serverID)
			return
		}
	case "system":
		if len(parts) == 1 {
			s.handleServerSystem(w, r, serverID)
			return
		}
	case "docker":
		if len(parts) == 1 {
			s.handleServerDocker(w, r, serverID)
			return
		}
	case "containers":
		s.handleServerContainers(w, r, serverID, parts[1:])
		return
	case "configs":
		s.handleServerConfigs(w, r, serverID, parts[1:])
		return
	}
	writeUnknownRoute(w, r.URL.Path)
}

func (s *Server) handleServerResource(w http.ResponseWriter, r *http.Request, serverID string) {
	switch r.Method {
	case http.MethodGet:
		server, err := s.st.GetServer(r.Context(), serverID)
		if err != nil {
			if handleLookupError(w, err, "server") {
				return
			}
			writeInternalError(w, "failed to fetch server")
			return
		}
		writeJSON(w, http.StatusOK, s.serverDTO(server))
	case http.MethodPatch:
		current, err := s.st.GetServer(r.Context(), serverID)
		if err != nil {
			if handleLookupError(w, err, "server") {
				return
			}
			writeInternalError(w, "failed to fetch server")
			return
		}
		var req serverPatchRequest
		if err := readJSON(r, &req); err != nil {
			handleJSONReadError(w, err)
			return
		}
		updated := current
		if req.Name != nil {
			updated.Name = strings.TrimSpace(*req.Name)
		}
		if req.Host != nil {
			updated.Host = strings.TrimSpace(*req.Host)
		}
		if req.SSHPort != nil {
			updated.SSHPort = *req.SSHPort
		}
		if req.SSHUser != nil {
			updated.SSHUser = strings.TrimSpace(*req.SSHUser)
		}
		if req.ProjectDir != nil {
			updated.ProjectDir = strings.TrimSpace(*req.ProjectDir)
		}
		if req.RoleHints != nil {
			updated.RoleHintsJSON = toRawJSON(*req.RoleHints, `[]`)
		}
		if req.Labels != nil {
			updated.LabelsJSON = toRawJSON(*req.Labels, `{}`)
		}
		updated, err = s.st.UpdateServer(r.Context(), current, updated)
		if err != nil {
			writeInvalidInput(w, "invalid server configuration", map[string]any{"cause": err.Error()})
			return
		}
		if req.Credential != nil {
			if s.k.IsZero() {
				writeMasterKeyRequired(w, "SSH credentials")
				return
			}
			if err := s.saveCredentialInput(r.Context(), updated, req.Credential); err != nil {
				writeInvalidInput(w, "invalid server credential", map[string]any{"cause": err.Error()})
				return
			}
		}
		writeJSON(w, http.StatusOK, s.serverDTO(updated))
	case http.MethodDelete:
		if err := s.st.DeleteServer(r.Context(), serverID); err != nil {
			if handleLookupError(w, err, "server") {
				return
			}
			writeInternalError(w, "failed to delete server")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeMethodNotAllowed(w, r.Method, http.MethodGet, http.MethodPatch, http.MethodDelete)
	}
}

func (s *Server) handleServerSSHTest(w http.ResponseWriter, r *http.Request, serverID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}
	server, target, err := s.loadServerTarget(r.Context(), serverID)
	if err != nil {
		s.writeServerTargetError(w, err)
		return
	}
	start := time.Now()
	result, err := s.sshTester.TestConnection(r.Context(), target)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		msg := err.Error()
		_ = s.st.UpdateServerConnectionResult(r.Context(), server.ID, db.ServerConnectionResult{LastError: &msg})
		writeDependencyUnavailable(w, "ssh connection test failed: "+msg)
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_ = s.st.UpdateServerConnectionResult(r.Context(), server.ID, db.ServerConnectionResult{LastSeenAt: &now})

	hostname := ""
	if res, execErr := s.sshExec.ExecuteCommand(r.Context(), target, "hostname", nil); execErr == nil && res.ExitCode == 0 {
		hostname = strings.TrimSpace(res.StdoutTail)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":                   true,
		"latency_ms":           latency,
		"remote_user":          target.User,
		"hostname":             hostname,
		"host_key_fingerprint": result.HostKeyFingerprint,
	})
}

func (s *Server) handleServerProjectValidate(w http.ResponseWriter, r *http.Request, serverID string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}
	server, target, err := s.loadServerTarget(r.Context(), serverID)
	if err != nil {
		s.writeServerTargetError(w, err)
		return
	}
	validation, err := s.deploy.ValidateProject(r.Context(), target, server.ProjectDir)
	if err != nil {
		writeDependencyUnavailable(w, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, validation)
}

func (s *Server) handleServerSystem(w http.ResponseWriter, r *http.Request, serverID string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	_, target, err := s.loadServerTarget(r.Context(), serverID)
	if err != nil {
		s.writeServerTargetError(w, err)
		return
	}
	lines := []string{
		"set -eu",
		"hostname",
		"uname -s",
		"uname -m",
		"nproc",
		`awk '/MemTotal/ {print $2}' /proc/meminfo`,
	}
	result, err := s.sshExec.ExecuteCommand(r.Context(), target, "sh -lc "+shellQuote(strings.Join(lines, "; ")), nil)
	if err != nil || result.ExitCode != 0 {
		writeDependencyUnavailable(w, "failed to fetch remote system info")
		return
	}
	values := splitPreservingNonEmpty(result.StdoutTail)
	if len(values) < 5 {
		writeDependencyUnavailable(w, "failed to parse remote system info")
		return
	}
	cpuCount, _ := strconv.Atoi(strings.TrimSpace(values[3]))
	memTotalKB, _ := strconv.ParseInt(strings.TrimSpace(values[4]), 10, 64)
	writeJSON(w, http.StatusOK, map[string]any{
		"hostname":     values[0],
		"os":           values[1],
		"arch":         values[2],
		"cpu_count":    cpuCount,
		"mem_total_kb": memTotalKB,
	})
}

func (s *Server) handleServerDocker(w http.ResponseWriter, r *http.Request, serverID string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	_, target, err := s.loadServerTarget(r.Context(), serverID)
	if err != nil {
		s.writeServerTargetError(w, err)
		return
	}
	status, err := s.docker.CheckDocker(r.Context(), target)
	if err != nil {
		writeDependencyUnavailable(w, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) writeServerTargetError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, sql.ErrNoRows):
		writeNotFound(w, "server", nil)
	case errors.Is(err, db.ErrMasterKeyRequired):
		writeMasterKeyRequired(w, "SSH credentials")
	default:
		writeInvalidInput(w, "server is not ready for remote operations", map[string]any{"cause": err.Error()})
	}
}

func parseServerRoute(path string) (string, []string, bool) {
	if !strings.HasPrefix(path, "/servers/") {
		return "", nil, false
	}
	raw := strings.Trim(strings.TrimPrefix(path, "/servers/"), "/")
	if raw == "" {
		return "", nil, false
	}
	parts := strings.Split(raw, "/")
	if strings.TrimSpace(parts[0]) == "" {
		return "", nil, false
	}
	return parts[0], parts[1:], true
}

func shellQuote(value string) string {
	value = strings.ReplaceAll(value, `'`, `'"'"'`)
	return "'" + value + "'"
}
