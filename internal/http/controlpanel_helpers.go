package httpapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	opsconfigs "github.com/LevonGhukas/O_Rabbit/internal/ops/configs"
	opsdeploy "github.com/LevonGhukas/O_Rabbit/internal/ops/deploy"
	sshops "github.com/LevonGhukas/O_Rabbit/internal/ops/ssh"
)

type serverCredentialRequest struct {
	AuthType           string `json:"auth_type"`
	Username           string `json:"username,omitempty"`
	Password           string `json:"password,omitempty"`
	PrivateKey         string `json:"private_key,omitempty"`
	Passphrase         string `json:"passphrase,omitempty"`
	HostKeyFingerprint string `json:"host_key_fingerprint,omitempty"`
}

type serverResponse struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Host       string            `json:"host"`
	SSHPort    int               `json:"ssh_port"`
	SSHUser    string            `json:"ssh_user"`
	ProjectDir string            `json:"project_dir"`
	RoleHints  []string          `json:"role_hints"`
	Labels     map[string]string `json:"labels"`
	LastSeenAt *string           `json:"last_seen_at"`
	LastError  *string           `json:"last_error"`
}

type streamBinding struct {
	StreamID  string
	EventBase string
	ServerID  string
}

func (s *Server) serverDTO(srv db.Server) serverResponse {
	return serverResponse{
		ID:         srv.ID,
		Name:       srv.Name,
		Host:       srv.Host,
		SSHPort:    srv.SSHPort,
		SSHUser:    srv.SSHUser,
		ProjectDir: srv.ProjectDir,
		RoleHints:  parseStringSliceJSON(srv.RoleHintsJSON),
		Labels:     parseStringMapJSON(srv.LabelsJSON),
		LastSeenAt: srv.LastSeenAt,
		LastError:  srv.LastError,
	}
}

func parseStringSliceJSON(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return []string{}
	}
	var out []string
	if err := json.Unmarshal(raw, &out); err != nil {
		return []string{}
	}
	return out
}

func parseStringMapJSON(raw json.RawMessage) map[string]string {
	if len(raw) == 0 {
		return map[string]string{}
	}
	var out map[string]string
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]string{}
	}
	if out == nil {
		out = map[string]string{}
	}
	return out
}

func toRawJSON(v any, fallback string) json.RawMessage {
	if v == nil {
		return json.RawMessage(fallback)
	}
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage(fallback)
	}
	return json.RawMessage(b)
}

func (s *Server) saveCredentialInput(ctx context.Context, srv db.Server, req *serverCredentialRequest) error {
	if req == nil {
		return nil
	}
	if s.k.IsZero() {
		return db.ErrMasterKeyRequired
	}
	username := strings.TrimSpace(req.Username)
	if username == "" {
		username = srv.SSHUser
	}
	cred, err := db.EncryptServerCredential(s.k, srv.ID, db.ServerCredentialSecret{
		AuthType:           req.AuthType,
		Username:           username,
		Password:           req.Password,
		PrivateKey:         req.PrivateKey,
		Passphrase:         req.Passphrase,
		HostKeyFingerprint: req.HostKeyFingerprint,
	})
	if err != nil {
		return err
	}
	_, err = s.st.SaveServerCredential(ctx, cred)
	return err
}

func (s *Server) loadServerTarget(ctx context.Context, serverID string) (db.Server, sshops.SSHTarget, error) {
	srv, err := s.st.GetServer(ctx, serverID)
	if err != nil {
		return db.Server{}, sshops.SSHTarget{}, err
	}
	cred, err := s.st.GetServerCredential(ctx, serverID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return db.Server{}, sshops.SSHTarget{}, fmt.Errorf("server has no stored credential")
		}
		return db.Server{}, sshops.SSHTarget{}, err
	}
	secret, err := db.DecryptServerCredential(s.k, cred)
	if err != nil {
		return db.Server{}, sshops.SSHTarget{}, err
	}
	fingerprint := firstNonEmptyString(secret.HostKeyFingerprint, cred.HostKeyFingerprint)
	if fingerprint == "" {
		return db.Server{}, sshops.SSHTarget{}, fmt.Errorf("server credential has no trusted SSH host key fingerprint; update credential with the server SHA-256 fingerprint")
	}
	target := sshops.SSHTarget{
		Host:               srv.Host,
		Port:               srv.SSHPort,
		User:               firstNonEmptyString(secret.Username, srv.SSHUser),
		Password:           secret.Password,
		PrivateKey:         secret.PrivateKey,
		Passphrase:         secret.Passphrase,
		HostKeyFingerprint: fingerprint,
	}
	return srv, target, nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func writeMasterKeyRequired(w http.ResponseWriter, resource string) {
	writeDependencyUnavailable(w, fmt.Sprintf("ORABBIT_MASTER_KEY is required to store %s", resource))
}

func requestActor(r *http.Request) string {
	if r == nil {
		return "api"
	}
	if auth := strings.TrimSpace(r.Header.Get("Authorization")); auth != "" {
		return "api"
	}
	if strings.TrimSpace(r.RemoteAddr) != "" {
		return r.RemoteAddr
	}
	return "api"
}

type asyncCommandRunner func(ctx context.Context, onStream func(sshops.StreamChunk)) (sshops.CommandResult, error)

func (s *Server) launchCommandExecution(server db.Server, execRec db.CommandExecution, bindings []streamBinding, runner asyncCommandRunner, onCompleted func(context.Context, db.CommandExecution, *string)) {
	if s.streams != nil {
		for _, binding := range bindings {
			s.streams.Publish(binding.StreamID, binding.EventBase+".started", "INFO", binding.ServerID, map[string]any{
				"execution_id": execRec.ID,
				"status":       execRec.Status,
			})
		}
	}

	go func() {
		runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()

		lineStreams := make([]*lineStreamBuffer, 0, len(bindings)*2)
		for _, binding := range bindings {
			binding := binding
			lineStreams = append(lineStreams,
				newLineStreamBuffer(func(line string) {
					if s.streams != nil {
						s.streams.Publish(binding.StreamID, binding.EventBase+".stdout", "INFO", binding.ServerID, map[string]any{"line": line})
					}
				}),
				newLineStreamBuffer(func(line string) {
					if s.streams != nil {
						s.streams.Publish(binding.StreamID, binding.EventBase+".stderr", "INFO", binding.ServerID, map[string]any{"line": line})
					}
				}),
			)
		}

		var stdoutPublishers []func(string)
		var stderrPublishers []func(string)
		for idx := 0; idx < len(lineStreams); idx += 2 {
			stdoutPublishers = append(stdoutPublishers, lineStreams[idx].Write)
			stderrPublishers = append(stderrPublishers, lineStreams[idx+1].Write)
		}

		result, err := runner(runCtx, func(chunk sshops.StreamChunk) {
			switch chunk.Stream {
			case sshops.StreamStdout:
				_ = s.st.AppendCommandExecutionOutputTail(context.Background(), execRec.ID, chunk.Data, "")
				for _, publish := range stdoutPublishers {
					publish(chunk.Data)
				}
			case sshops.StreamStderr:
				_ = s.st.AppendCommandExecutionOutputTail(context.Background(), execRec.ID, "", chunk.Data)
				for _, publish := range stderrPublishers {
					publish(chunk.Data)
				}
			}
		})
		for _, buf := range lineStreams {
			buf.Flush()
		}

		status := "SUCCEEDED"
		var execErr *string
		if err != nil {
			status = "FAILED"
			msg := err.Error()
			execErr = &msg
		} else if result.ExitCode != 0 {
			status = "FAILED"
			msg := firstNonEmptyString(result.StderrTail, result.StdoutTail, fmt.Sprintf("command failed with exit code %d", result.ExitCode))
			execErr = &msg
		}
		if runCtx.Err() == context.DeadlineExceeded && execErr == nil {
			status = "FAILED"
			msg := runCtx.Err().Error()
			execErr = &msg
		}

		if result.StartedAt.IsZero() {
			started := nowTimeUTC()
			result.StartedAt = started
		}
		if result.FinishedAt.IsZero() {
			result.FinishedAt = nowTimeUTC()
		}

		_ = s.st.UpdateCommandExecutionStatus(context.Background(), execRec.ID, db.CommandExecutionUpdate{
			Status:     status,
			StartedAt:  ptrString(result.StartedAt.Format(time.RFC3339Nano)),
			FinishedAt: ptrString(result.FinishedAt.Format(time.RFC3339Nano)),
			ExitCode:   &result.ExitCode,
			Error:      execErr,
		})
		_ = s.st.MarkCommandExecutionCompleted(context.Background(), execRec.ID, status, result.ExitCode, result.StdoutTail, result.StderrTail, execErr)

		finalExec, getErr := s.st.GetCommandExecution(context.Background(), execRec.ID)
		if getErr != nil {
			finalExec = execRec
			finalExec.Status = status
			finalExec.ExitCode = &result.ExitCode
			finalExec.Error = execErr
		}
		if onCompleted != nil {
			onCompleted(context.Background(), finalExec, execErr)
		}
		if s.streams != nil {
			for _, binding := range bindings {
				s.streams.Publish(binding.StreamID, binding.EventBase+".completed", "INFO", binding.ServerID, map[string]any{
					"execution_id": execRec.ID,
					"status":       status,
					"exit_code":    result.ExitCode,
					"error":        derefString(execErr),
				})
			}
		}
	}()
}

func nowTimeUTC() time.Time {
	return time.Now().UTC()
}

func ptrString(value string) *string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	v := value
	return &v
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

type lineStreamBuffer struct {
	emit    func(string)
	pending string
}

func newLineStreamBuffer(emit func(string)) *lineStreamBuffer {
	return &lineStreamBuffer{emit: emit}
}

func (b *lineStreamBuffer) Write(chunk string) {
	if b == nil || chunk == "" {
		return
	}
	b.pending += chunk
	for {
		idx := strings.IndexByte(b.pending, '\n')
		if idx < 0 {
			break
		}
		line := strings.TrimRight(b.pending[:idx], "\r")
		if strings.TrimSpace(line) != "" && b.emit != nil {
			b.emit(line)
		}
		b.pending = b.pending[idx+1:]
	}
}

func (b *lineStreamBuffer) Flush() {
	if b == nil {
		return
	}
	line := strings.TrimSpace(strings.TrimRight(b.pending, "\r"))
	if line != "" && b.emit != nil {
		b.emit(line)
	}
	b.pending = ""
}

func snapshotExecutionStream(execRec db.CommandExecution) []StreamEnvelope {
	events := make([]StreamEnvelope, 0, 3)
	for _, line := range splitPreservingNonEmpty(execRec.StdoutTail) {
		events = append(events, StreamEnvelope{
			Type:      "execution.stdout",
			StreamID:  execRec.ID,
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Level:     "INFO",
			Data:      map[string]any{"line": line},
		})
	}
	for _, line := range splitPreservingNonEmpty(execRec.StderrTail) {
		events = append(events, StreamEnvelope{
			Type:      "execution.stderr",
			StreamID:  execRec.ID,
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Level:     "INFO",
			Data:      map[string]any{"line": line},
		})
	}
	if execRec.Status == "SUCCEEDED" || execRec.Status == "FAILED" || execRec.Status == "CANCELED" {
		events = append(events, StreamEnvelope{
			Type:      "execution.completed",
			StreamID:  execRec.ID,
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Level:     "INFO",
			Data: map[string]any{
				"status":    execRec.Status,
				"exit_code": execRec.ExitCode,
				"error":     derefString(execRec.Error),
			},
		})
	}
	return events
}

func snapshotDeploymentStream(dep db.Deployment, execRec *db.CommandExecution) []StreamEnvelope {
	events := make([]StreamEnvelope, 0, 3)
	if execRec != nil {
		for _, line := range splitPreservingNonEmpty(execRec.StdoutTail) {
			events = append(events, StreamEnvelope{
				Type:      "deployment.stdout",
				StreamID:  dep.ID,
				Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
				Level:     "INFO",
				ServerID:  dep.ServerID,
				Data:      map[string]any{"line": line},
			})
		}
		for _, line := range splitPreservingNonEmpty(execRec.StderrTail) {
			events = append(events, StreamEnvelope{
				Type:      "deployment.stderr",
				StreamID:  dep.ID,
				Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
				Level:     "INFO",
				ServerID:  dep.ServerID,
				Data:      map[string]any{"line": line},
			})
		}
	}
	if dep.Status == "SUCCEEDED" || dep.Status == "FAILED" || dep.Status == "CANCELED" {
		events = append(events, StreamEnvelope{
			Type:      "deployment.completed",
			StreamID:  dep.ID,
			Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
			Level:     "INFO",
			ServerID:  dep.ServerID,
			Data: map[string]any{
				"status": dep.Status,
				"error":  derefString(dep.Error),
			},
		})
	}
	return events
}

func splitPreservingNonEmpty(content string) []string {
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		out = append(out, line)
	}
	return out
}

func validationStatusString(validation opsconfigs.ValidationResult) string {
	if validation.OK {
		return "valid"
	}
	return "invalid"
}

func deploymentStatusForExecution(status string) string {
	switch status {
	case "SUCCEEDED", "FAILED", "CANCELED":
		return status
	default:
		return "RUNNING"
	}
}

func deploymentParamsJSON(params opsdeploy.DeploymentParams) json.RawMessage {
	return toRawJSON(params, `{}`)
}
