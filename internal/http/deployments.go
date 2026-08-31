package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	opsdeploy "github.com/LevonGhukas/O_Rabbit/internal/ops/deploy"
	sshops "github.com/LevonGhukas/O_Rabbit/internal/ops/ssh"
)

type deploymentRequest struct {
	ServerID  string                     `json:"server_id"`
	Component string                     `json:"component"`
	Params    opsdeploy.DeploymentParams `json:"params"`
}

func (s *Server) handleDeployments(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		serverID := strings.TrimSpace(r.URL.Query().Get("server_id"))
		items, err := s.st.ListDeployments(r.Context(), serverID, 100)
		if err != nil {
			writeInternalError(w, "failed to list deployments")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		var req deploymentRequest
		if err := readJSON(r, &req); err != nil {
			handleJSONReadError(w, err)
			return
		}
		server, target, err := s.loadServerTarget(r.Context(), strings.TrimSpace(req.ServerID))
		if err != nil {
			s.writeServerTargetError(w, err)
			return
		}
		plan, err := s.deploy.PrepareDeployment(r.Context(), target, server.ProjectDir, req.Component, req.Params)
		if err != nil {
			if errors.Is(err, opsdeploy.ErrNotImplemented) {
				writeNotImplemented(w, err.Error(), map[string]any{"component": req.Component})
				return
			}
			writeInvalidInput(w, "deployment validation failed", map[string]any{"cause": err.Error()})
			return
		}
		execRec, err := s.st.CreateCommandExecution(r.Context(), db.CommandExecution{
			ID:          newID(),
			ServerID:    server.ID,
			Kind:        "deployment",
			AllowlistID: plan.AllowlistID,
			ParamsJSON:  deploymentParamsJSON(req.Params),
			Status:      "RUNNING",
			RequestedBy: requestActor(r),
		})
		if err != nil {
			writeInternalError(w, "failed to create command execution")
			return
		}
		dep, err := s.st.CreateDeployment(r.Context(), db.Deployment{
			ID:          newID(),
			ServerID:    server.ID,
			Component:   plan.Component,
			ScriptID:    plan.ScriptID,
			Status:      "RUNNING",
			ExecutionID: execRec.ID,
		})
		if err != nil {
			writeInternalError(w, "failed to create deployment")
			return
		}

		s.launchCommandExecution(server, execRec, []streamBinding{
			{StreamID: execRec.ID, EventBase: "execution", ServerID: server.ID},
			{StreamID: dep.ID, EventBase: "deployment", ServerID: server.ID},
		}, func(ctx context.Context, onStream func(chunk sshops.StreamChunk)) (sshops.CommandResult, error) {
			return s.sshExec.ExecuteCommand(ctx, target, plan.Command, onStream)
		}, func(ctx context.Context, execRec db.CommandExecution, execErr *string) {
			_ = s.st.UpdateDeploymentStatus(ctx, dep.ID, db.DeploymentUpdate{
				Status:      deploymentStatusForExecution(execRec.Status),
				ExecutionID: &execRec.ID,
				Error:       execErr,
			})
		})

		writeJSON(w, http.StatusAccepted, map[string]any{
			"deployment":        dep,
			"events_stream_url": "/deployments/" + dep.ID + "/events/stream",
		})
	default:
		writeMethodNotAllowed(w, r.Method, http.MethodGet, http.MethodPost)
	}
}

func (s *Server) handleDeploymentByID(w http.ResponseWriter, r *http.Request) {
	deploymentID, subpath, ok := parseRootIDRoute("/deployments/", r.URL.Path)
	if !ok {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	if len(subpath) == 0 {
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, r.Method, http.MethodGet)
			return
		}
		dep, err := s.st.GetDeployment(r.Context(), deploymentID)
		if err != nil {
			if handleLookupError(w, err, "deployment") {
				return
			}
			writeInternalError(w, "failed to fetch deployment")
			return
		}
		var execRec *db.CommandExecution
		if strings.TrimSpace(dep.ExecutionID) != "" {
			if exec, err := s.st.GetCommandExecution(r.Context(), dep.ExecutionID); err == nil {
				execRec = &exec
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"deployment": dep, "execution": execRec})
		return
	}
	if len(subpath) == 2 && subpath[0] == "events" && subpath[1] == "stream" {
		s.handleDeploymentStream(w, r, deploymentID)
		return
	}
	writeUnknownRoute(w, r.URL.Path)
}

func (s *Server) handleExecutionByID(w http.ResponseWriter, r *http.Request) {
	executionID, subpath, ok := parseRootIDRoute("/executions/", r.URL.Path)
	if !ok {
		writeUnknownRoute(w, r.URL.Path)
		return
	}
	if len(subpath) == 0 {
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, r.Method, http.MethodGet)
			return
		}
		execRec, err := s.st.GetCommandExecution(r.Context(), executionID)
		if err != nil {
			if handleLookupError(w, err, "execution") {
				return
			}
			writeInternalError(w, "failed to fetch execution")
			return
		}
		writeJSON(w, http.StatusOK, execRec)
		return
	}
	if len(subpath) == 2 && subpath[0] == "events" && subpath[1] == "stream" {
		s.handleExecutionStream(w, r, executionID)
		return
	}
	writeUnknownRoute(w, r.URL.Path)
}

func (s *Server) handleExecutionStream(w http.ResponseWriter, r *http.Request, executionID string) {
	execRec, err := s.st.GetCommandExecution(r.Context(), executionID)
	if err != nil {
		if handleLookupError(w, err, "execution") {
			return
		}
		writeInternalError(w, "failed to fetch execution")
		return
	}
	s.streamOperation(w, r, executionID, snapshotExecutionStream(execRec))
}

func (s *Server) handleDeploymentStream(w http.ResponseWriter, r *http.Request, deploymentID string) {
	dep, err := s.st.GetDeployment(r.Context(), deploymentID)
	if err != nil {
		if handleLookupError(w, err, "deployment") {
			return
		}
		writeInternalError(w, "failed to fetch deployment")
		return
	}
	var execRec *db.CommandExecution
	if strings.TrimSpace(dep.ExecutionID) != "" {
		if exec, err := s.st.GetCommandExecution(r.Context(), dep.ExecutionID); err == nil {
			execRec = &exec
		}
	}
	s.streamOperation(w, r, deploymentID, snapshotDeploymentStream(dep, execRec))
}

func (s *Server) streamOperation(w http.ResponseWriter, r *http.Request, streamID string, snapshot []StreamEnvelope) {
	if s.streams == nil {
		writeDependencyUnavailable(w, "event stream unavailable")
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		writeInternalError(w, "streaming unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	for _, item := range snapshot {
		writeSSEEnvelope(w, fl, item.Type, item)
	}

	ch, unsub := s.streams.Subscribe(streamID)
	defer unsub()

	keep := time.NewTicker(15 * time.Second)
	defer keep.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case event := <-ch:
			writeSSEEnvelope(w, fl, event.Type, event)
		case <-keep.C:
			w.Write([]byte(": keepalive\n\n"))
			fl.Flush()
		}
	}
}

func parseRootIDRoute(prefix string, route string) (string, []string, bool) {
	if !strings.HasPrefix(route, prefix) {
		return "", nil, false
	}
	raw := strings.Trim(strings.TrimPrefix(route, prefix), "/")
	if raw == "" {
		return "", nil, false
	}
	parts := strings.Split(raw, "/")
	if strings.TrimSpace(parts[0]) == "" {
		return "", nil, false
	}
	return parts[0], parts[1:], true
}
