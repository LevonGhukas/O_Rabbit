package httpapi

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	opsdocker "github.com/LevonGhukas/O_Rabbit/internal/ops/docker"
	sshops "github.com/LevonGhukas/O_Rabbit/internal/ops/ssh"
)

func (s *Server) handleServerContainers(w http.ResponseWriter, r *http.Request, serverID string, parts []string) {
	if len(parts) == 0 {
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w, r.Method, http.MethodGet)
			return
		}
		_, target, err := s.loadServerTarget(r.Context(), serverID)
		if err != nil {
			s.writeServerTargetError(w, err)
			return
		}
		items, err := s.docker.ListContainers(r.Context(), target)
		if err != nil {
			writeDependencyUnavailable(w, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
		return
	}

	containerID := strings.TrimSpace(parts[0])
	if len(parts) >= 3 && parts[1] == "actions" {
		s.handleContainerAction(w, r, serverID, containerID, parts[2])
		return
	}
	if len(parts) >= 2 && parts[1] == "logs" {
		if len(parts) == 2 {
			s.handleContainerLogs(w, r, serverID, containerID)
			return
		}
		if len(parts) == 3 && parts[2] == "stream" {
			s.handleContainerLogsStream(w, r, serverID, containerID)
			return
		}
	}
	writeUnknownRoute(w, r.URL.Path)
}

func (s *Server) handleContainerAction(w http.ResponseWriter, r *http.Request, serverID string, containerID string, action string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, r.Method, http.MethodPost)
		return
	}
	if err := opsdocker.ValidateContainerRef(containerID); err != nil {
		writeInvalidInput(w, "invalid container reference", map[string]any{"field": "container_id"})
		return
	}
	if action != "start" && action != "stop" && action != "restart" {
		writeInvalidInput(w, "unsupported container action", map[string]any{"action": action})
		return
	}

	server, target, err := s.loadServerTarget(r.Context(), serverID)
	if err != nil {
		s.writeServerTargetError(w, err)
		return
	}
	execRec, err := s.st.CreateCommandExecution(r.Context(), db.CommandExecution{
		ID:          newID(),
		ServerID:    server.ID,
		Kind:        "container.action",
		AllowlistID: "container." + action,
		ParamsJSON:  toRawJSON(map[string]any{"container_id": containerID, "action": action}, `{}`),
		Status:      "RUNNING",
		RequestedBy: requestActor(r),
	})
	if err != nil {
		writeInternalError(w, "failed to create command execution")
		return
	}

	var runner asyncCommandRunner
	switch action {
	case "start":
		runner = func(ctx context.Context, onStream func(sshops.StreamChunk)) (sshops.CommandResult, error) {
			return s.sshExec.ExecuteCommand(ctx, target, "docker start "+shellQuote(containerID), onStream)
		}
	case "stop":
		runner = func(ctx context.Context, onStream func(sshops.StreamChunk)) (sshops.CommandResult, error) {
			return s.sshExec.ExecuteCommand(ctx, target, "docker stop "+shellQuote(containerID), onStream)
		}
	case "restart":
		runner = func(ctx context.Context, onStream func(sshops.StreamChunk)) (sshops.CommandResult, error) {
			return s.sshExec.ExecuteCommand(ctx, target, "docker restart "+shellQuote(containerID), onStream)
		}
	}
	s.launchCommandExecution(server, execRec, []streamBinding{{
		StreamID:  execRec.ID,
		EventBase: "execution",
		ServerID:  server.ID,
	}}, runner, nil)

	writeJSON(w, http.StatusAccepted, map[string]any{
		"accepted":     true,
		"execution_id": execRec.ID,
	})
}

func (s *Server) handleContainerLogs(w http.ResponseWriter, r *http.Request, serverID string, containerID string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	if err := opsdocker.ValidateContainerRef(containerID); err != nil {
		writeInvalidInput(w, "invalid container reference", map[string]any{"field": "container_id"})
		return
	}
	_, target, err := s.loadServerTarget(r.Context(), serverID)
	if err != nil {
		s.writeServerTargetError(w, err)
		return
	}
	tail := opsdocker.ParseTailParam(r.URL.Query().Get("tail"), 200)
	items, err := s.docker.TailContainerLogs(r.Context(), target, containerID, tail)
	if err != nil {
		writeDependencyUnavailable(w, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleContainerLogsStream(w http.ResponseWriter, r *http.Request, serverID string, containerID string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, r.Method, http.MethodGet)
		return
	}
	if err := opsdocker.ValidateContainerRef(containerID); err != nil {
		writeInvalidInput(w, "invalid container reference", map[string]any{"field": "container_id"})
		return
	}
	server, target, err := s.loadServerTarget(r.Context(), serverID)
	if err != nil {
		s.writeServerTargetError(w, err)
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

	tail := opsdocker.ParseTailParam(r.URL.Query().Get("tail"), 200)
	_, err = s.docker.StreamContainerLogs(r.Context(), target, containerID, tail, func(line opsdocker.LogLine) {
		writeSSEEnvelope(w, fl, "service.log", StreamEnvelope{
			Type:      "service.log",
			StreamID:  server.ID + ":" + containerID,
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Level:     "INFO",
			ServerID:  server.ID,
			Data:      map[string]any{"line": line.Line},
		})
	})
	if err != nil && r.Context().Err() == nil {
		writeSSEEnvelope(w, fl, "service.log", StreamEnvelope{
			Type:      "service.log",
			StreamID:  server.ID + ":" + containerID,
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Level:     "ERROR",
			ServerID:  server.ID,
			Data:      map[string]any{"line": err.Error()},
		})
	}
}
