package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	secretcrypto "github.com/LevonGhukas/O_Rabbit/internal/crypto"
)

const (
	DefaultSSHPort           = 22
	executionTailLimitBytes  = 64 * 1024
	serverAuthTypePassword   = "password"
	serverAuthTypePrivateKey = "private_key"
)

var (
	ErrMasterKeyRequired          = errors.New("ORABBIT_MASTER_KEY is required to store encrypted secrets")
	ErrUnsupportedServerAuthType  = errors.New("unsupported server credential auth_type")
	ErrInvalidExecutionStatus     = errors.New("invalid command execution status")
	ErrInvalidDeploymentStatus    = errors.New("invalid deployment status")
	ErrInvalidDeploymentComponent = errors.New("invalid deployment component")
)

type Server struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Host          string          `json:"host"`
	SSHPort       int             `json:"ssh_port"`
	SSHUser       string          `json:"ssh_user"`
	ProjectDir    string          `json:"project_dir"`
	RoleHintsJSON json.RawMessage `json:"role_hints_json"`
	LabelsJSON    json.RawMessage `json:"labels_json"`
	CreatedAt     string          `json:"created_at"`
	UpdatedAt     string          `json:"updated_at"`
	LastSeenAt    *string         `json:"last_seen_at,omitempty"`
	LastError     *string         `json:"last_error,omitempty"`
}

type ServerConnectionResult struct {
	LastSeenAt *string
	LastError  *string
}

type ServerCredential struct {
	ID                 string `json:"id"`
	ServerID           string `json:"server_id"`
	AuthType           string `json:"auth_type"`
	Username           string `json:"username"`
	PrivateKeyEnc      []byte `json:"-"`
	PasswordEnc        []byte `json:"-"`
	PassphraseEnc      []byte `json:"-"`
	HostKeyFingerprint string `json:"host_key_fingerprint,omitempty"`
	CreatedAt          string `json:"created_at"`
	UpdatedAt          string `json:"updated_at"`
}

type ServerCredentialSecret struct {
	AuthType           string `json:"auth_type"`
	Username           string `json:"username"`
	PrivateKey         string `json:"private_key,omitempty"`
	Password           string `json:"password,omitempty"`
	Passphrase         string `json:"passphrase,omitempty"`
	HostKeyFingerprint string `json:"host_key_fingerprint,omitempty"`
}

type CommandExecution struct {
	ID          string          `json:"id"`
	ServerID    string          `json:"server_id"`
	Kind        string          `json:"kind"`
	AllowlistID string          `json:"allowlist_id"`
	ParamsJSON  json.RawMessage `json:"params_json"`
	Status      string          `json:"status"`
	ExitCode    *int            `json:"exit_code,omitempty"`
	StartedAt   *string         `json:"started_at,omitempty"`
	FinishedAt  *string         `json:"finished_at,omitempty"`
	StdoutTail  string          `json:"stdout_tail,omitempty"`
	StderrTail  string          `json:"stderr_tail,omitempty"`
	Error       *string         `json:"error,omitempty"`
	RequestedBy string          `json:"requested_by,omitempty"`
}

type CommandExecutionUpdate struct {
	Status     string
	ExitCode   *int
	StartedAt  *string
	FinishedAt *string
	Error      *string
}

type Deployment struct {
	ID          string  `json:"id"`
	ServerID    string  `json:"server_id"`
	Component   string  `json:"component"`
	ScriptID    string  `json:"script_id"`
	Status      string  `json:"status"`
	ExecutionID string  `json:"execution_id,omitempty"`
	StartedAt   *string `json:"started_at,omitempty"`
	FinishedAt  *string `json:"finished_at,omitempty"`
	Error       *string `json:"error,omitempty"`
}

type DeploymentUpdate struct {
	Status      string
	ExecutionID *string
	StartedAt   *string
	FinishedAt  *string
	Error       *string
}

type ConfigVersion struct {
	ID                   string          `json:"id"`
	ServerID             string          `json:"server_id"`
	ConfigID             string          `json:"config_id"`
	Version              int             `json:"version"`
	ContentEnc           []byte          `json:"-"`
	CreatedAt            string          `json:"created_at"`
	ValidationStatus     string          `json:"validation_status"`
	ValidationErrorsJSON json.RawMessage `json:"validation_errors_json"`
}

func EncryptServerCredential(k secretcrypto.Key, serverID string, secret ServerCredentialSecret) (ServerCredential, error) {
	if k.IsZero() {
		return ServerCredential{}, ErrMasterKeyRequired
	}
	if strings.TrimSpace(serverID) == "" {
		return ServerCredential{}, fmt.Errorf("missing server id")
	}
	cred := ServerCredential{
		ID:                 newStoreID(),
		ServerID:           strings.TrimSpace(serverID),
		AuthType:           normalizeServerCredentialAuthType(secret.AuthType),
		Username:           strings.TrimSpace(secret.Username),
		HostKeyFingerprint: strings.TrimSpace(secret.HostKeyFingerprint),
	}
	if err := validateServerCredentialSecret(cred.AuthType, secret); err != nil {
		return ServerCredential{}, err
	}
	var err error
	if cred.PrivateKeyEnc, err = encryptOptionalSecret(k, secret.PrivateKey, serverCredentialAAD(serverID, "private_key")); err != nil {
		return ServerCredential{}, err
	}
	if cred.PasswordEnc, err = encryptOptionalSecret(k, secret.Password, serverCredentialAAD(serverID, "password")); err != nil {
		return ServerCredential{}, err
	}
	if cred.PassphraseEnc, err = encryptOptionalSecret(k, secret.Passphrase, serverCredentialAAD(serverID, "passphrase")); err != nil {
		return ServerCredential{}, err
	}
	return prepareServerCredentialForCreate(cred)
}

func DecryptServerCredential(k secretcrypto.Key, cred ServerCredential) (ServerCredentialSecret, error) {
	var out ServerCredentialSecret
	out.AuthType = normalizeServerCredentialAuthType(cred.AuthType)
	out.Username = strings.TrimSpace(cred.Username)
	out.HostKeyFingerprint = strings.TrimSpace(cred.HostKeyFingerprint)

	var err error
	if out.PrivateKey, err = decryptOptionalSecret(k, cred.PrivateKeyEnc, serverCredentialAAD(cred.ServerID, "private_key")); err != nil {
		return ServerCredentialSecret{}, err
	}
	if out.Password, err = decryptOptionalSecret(k, cred.PasswordEnc, serverCredentialAAD(cred.ServerID, "password")); err != nil {
		return ServerCredentialSecret{}, err
	}
	if out.Passphrase, err = decryptOptionalSecret(k, cred.PassphraseEnc, serverCredentialAAD(cred.ServerID, "passphrase")); err != nil {
		return ServerCredentialSecret{}, err
	}
	return out, nil
}

func EncryptConfigVersionContent(k secretcrypto.Key, aad string, content []byte) ([]byte, error) {
	if k.IsZero() {
		return nil, ErrMasterKeyRequired
	}
	if len(content) == 0 {
		return nil, fmt.Errorf("missing config content")
	}
	return secretcrypto.Encrypt(k, content, []byte(aad))
}

func DecryptConfigVersionContent(k secretcrypto.Key, aad string, blob []byte) ([]byte, error) {
	return secretcrypto.Decrypt(k, blob, []byte(aad))
}

func (s *Store) CreateServer(ctx context.Context, srv Server) (Server, error) {
	srv, err := prepareServerForCreate(srv)
	if err != nil {
		return Server{}, err
	}
	if err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return createServerTx(ctx, tx, srv)
	}); err != nil {
		return Server{}, err
	}
	return srv, nil
}

func (s *Store) ListServers(ctx context.Context) ([]Server, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, host, ssh_port, ssh_user, project_dir, role_hints_json, labels_json, created_at, updated_at, last_seen_at, last_error
		FROM servers
		ORDER BY created_at DESC, id DESC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Server
	for rows.Next() {
		srv, err := scanServer(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, srv)
	}
	return out, rows.Err()
}

func (s *Store) GetServer(ctx context.Context, id string) (Server, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, name, host, ssh_port, ssh_user, project_dir, role_hints_json, labels_json, created_at, updated_at, last_seen_at, last_error
		FROM servers
		WHERE id=?;`, id)
	return scanServer(row)
}

func (s *Store) UpdateServer(ctx context.Context, before Server, after Server) (Server, error) {
	after, err := prepareServerForUpdate(before, after)
	if err != nil {
		return Server{}, err
	}
	if err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return updateServerTx(ctx, tx, after)
	}); err != nil {
		return Server{}, err
	}
	return after, nil
}

func (s *Store) DeleteServer(ctx context.Context, id string) error {
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		for _, stmt := range []string{
			`DELETE FROM server_credentials WHERE server_id=?;`,
			`DELETE FROM config_versions WHERE server_id=?;`,
			`DELETE FROM deployments WHERE server_id=?;`,
			`DELETE FROM command_executions WHERE server_id=?;`,
		} {
			if _, err := tx.ExecContext(ctx, stmt, id); err != nil {
				return err
			}
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM servers WHERE id=?;`, id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return sql.ErrNoRows
		}
		return nil
	})
}

func (s *Store) UpdateServerConnectionResult(ctx context.Context, id string, result ServerConnectionResult) error {
	var lastSeenAt any
	if result.LastSeenAt != nil {
		lastSeenAt = *result.LastSeenAt
	}
	var lastError any
	if result.LastError != nil {
		lastError = *result.LastError
	}
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE servers
			SET last_seen_at=COALESCE(?, last_seen_at),
				last_error=?,
				updated_at=?
			WHERE id=?;`,
			lastSeenAt, lastError, nowUTC(), id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return sql.ErrNoRows
		}
		return nil
	})
}

func (s *Store) CreateServerCredential(ctx context.Context, cred ServerCredential) (ServerCredential, error) {
	cred, err := prepareServerCredentialForCreate(cred)
	if err != nil {
		return ServerCredential{}, err
	}
	if err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return createServerCredentialTx(ctx, tx, cred)
	}); err != nil {
		return ServerCredential{}, err
	}
	return cred, nil
}

func (s *Store) SaveServerCredential(ctx context.Context, cred ServerCredential) (ServerCredential, error) {
	current, err := s.GetServerCredential(ctx, cred.ServerID)
	switch {
	case err == nil:
		if cred.ID == "" {
			cred.ID = current.ID
		}
		return s.UpdateServerCredential(ctx, current, cred)
	case errors.Is(err, sql.ErrNoRows):
		return s.CreateServerCredential(ctx, cred)
	default:
		return ServerCredential{}, err
	}
}

func (s *Store) GetServerCredential(ctx context.Context, serverID string) (ServerCredential, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, server_id, auth_type, username, private_key_enc, password_enc, passphrase_enc, host_key_fingerprint, created_at, updated_at
		FROM server_credentials
		WHERE server_id=?;`, serverID)
	return scanServerCredential(row)
}

func (s *Store) UpdateServerCredential(ctx context.Context, before ServerCredential, after ServerCredential) (ServerCredential, error) {
	after, err := prepareServerCredentialForUpdate(before, after)
	if err != nil {
		return ServerCredential{}, err
	}
	if err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return updateServerCredentialTx(ctx, tx, after)
	}); err != nil {
		return ServerCredential{}, err
	}
	return after, nil
}

func (s *Store) DeleteServerCredential(ctx context.Context, serverID string) error {
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `DELETE FROM server_credentials WHERE server_id=?;`, serverID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return sql.ErrNoRows
		}
		return nil
	})
}

func (s *Store) CreateCommandExecution(ctx context.Context, exec CommandExecution) (CommandExecution, error) {
	exec, err := prepareCommandExecutionForCreate(exec)
	if err != nil {
		return CommandExecution{}, err
	}
	if err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return createCommandExecutionTx(ctx, tx, exec)
	}); err != nil {
		return CommandExecution{}, err
	}
	return exec, nil
}

func (s *Store) UpdateCommandExecutionStatus(ctx context.Context, id string, update CommandExecutionUpdate) error {
	update, err := prepareCommandExecutionUpdate(update)
	if err != nil {
		return err
	}
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return updateCommandExecutionStatusTx(ctx, tx, id, update)
	})
}

func (s *Store) MarkCommandExecutionRunning(ctx context.Context, id string) error {
	return s.UpdateCommandExecutionStatus(ctx, id, CommandExecutionUpdate{Status: "RUNNING"})
}

func (s *Store) AppendCommandExecutionOutput(ctx context.Context, id string, stdoutChunk string, stderrChunk string) error {
	if stdoutChunk == "" && stderrChunk == "" {
		return nil
	}
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx, `SELECT stdout_tail, stderr_tail FROM command_executions WHERE id=?;`, id)
		var stdoutTail, stderrTail string
		if err := row.Scan(&stdoutTail, &stderrTail); err != nil {
			return err
		}
		stdoutTail = appendTail(stdoutTail, stdoutChunk, executionTailLimitBytes)
		stderrTail = appendTail(stderrTail, stderrChunk, executionTailLimitBytes)
		_, err := tx.ExecContext(ctx, `
			UPDATE command_executions
			SET stdout_tail=?, stderr_tail=?
			WHERE id=?;`,
			stdoutTail, stderrTail, id)
		return err
	})
}

func (s *Store) AppendCommandExecutionOutputTail(ctx context.Context, id string, stdoutChunk string, stderrChunk string) error {
	return s.AppendCommandExecutionOutput(ctx, id, stdoutChunk, stderrChunk)
}

func (s *Store) GetCommandExecution(ctx context.Context, id string) (CommandExecution, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, server_id, kind, allowlist_id, params_json, status, exit_code, started_at, finished_at, stdout_tail, stderr_tail, error, requested_by
		FROM command_executions
		WHERE id=?;`, id)
	return scanCommandExecution(row)
}

func (s *Store) ListCommandExecutionsByServer(ctx context.Context, serverID string, limit int) ([]CommandExecution, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, server_id, kind, allowlist_id, params_json, status, exit_code, started_at, finished_at, stdout_tail, stderr_tail, error, requested_by
		FROM command_executions
		WHERE server_id=?
		ORDER BY COALESCE(started_at, finished_at, '') DESC, id DESC
		LIMIT ?;`, serverID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CommandExecution
	for rows.Next() {
		exec, err := scanCommandExecution(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, exec)
	}
	return out, rows.Err()
}

func (s *Store) CompleteCommandExecution(ctx context.Context, id string, status string, exitCode int, stdoutTail string, stderrTail string, execErr *string) error {
	if err := validateCommandExecutionStatus(status); err != nil {
		return err
	}
	if status != "SUCCEEDED" && status != "FAILED" && status != "CANCELED" {
		return fmt.Errorf("%w: completion requires a terminal status", ErrInvalidExecutionStatus)
	}
	stdoutTail = trimTail(stdoutTail, executionTailLimitBytes)
	stderrTail = trimTail(stderrTail, executionTailLimitBytes)
	finishedAt := nowUTC()
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE command_executions
			SET status=?, exit_code=?, finished_at=?, stdout_tail=?, stderr_tail=?, error=?
			WHERE id=?;`,
			status, exitCode, finishedAt, stdoutTail, stderrTail, execErr, id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return sql.ErrNoRows
		}
		return nil
	})
}

func (s *Store) MarkCommandExecutionCompleted(ctx context.Context, id string, status string, exitCode int, stdoutTail string, stderrTail string, execErr *string) error {
	return s.CompleteCommandExecution(ctx, id, status, exitCode, stdoutTail, stderrTail, execErr)
}

func (s *Store) CreateDeployment(ctx context.Context, dep Deployment) (Deployment, error) {
	dep, err := prepareDeploymentForCreate(dep)
	if err != nil {
		return Deployment{}, err
	}
	if err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return createDeploymentTx(ctx, tx, dep)
	}); err != nil {
		return Deployment{}, err
	}
	return dep, nil
}

func (s *Store) UpdateDeploymentStatus(ctx context.Context, id string, update DeploymentUpdate) error {
	update, err := prepareDeploymentUpdate(update)
	if err != nil {
		return err
	}
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return updateDeploymentStatusTx(ctx, tx, id, update)
	})
}

func (s *Store) GetDeployment(ctx context.Context, id string) (Deployment, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, server_id, component, script_id, status, execution_id, started_at, finished_at, error
		FROM deployments
		WHERE id=?;`, id)
	return scanDeployment(row)
}

func (s *Store) ListDeployments(ctx context.Context, serverID string, limit int) ([]Deployment, error) {
	if limit <= 0 {
		limit = 50
	}
	baseQuery := `
		SELECT id, server_id, component, script_id, status, execution_id, started_at, finished_at, error
		FROM deployments`
	var (
		rows *sql.Rows
		err  error
	)
	if strings.TrimSpace(serverID) == "" {
		rows, err = s.db.QueryContext(ctx, baseQuery+` ORDER BY COALESCE(started_at, finished_at, '') DESC, id DESC LIMIT ?;`, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, baseQuery+` WHERE server_id=? ORDER BY COALESCE(started_at, finished_at, '') DESC, id DESC LIMIT ?;`, serverID, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Deployment
	for rows.Next() {
		dep, err := scanDeployment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, dep)
	}
	return out, rows.Err()
}

func (s *Store) SaveConfigVersion(ctx context.Context, cfg ConfigVersion) (ConfigVersion, error) {
	cfg, err := prepareConfigVersionForSave(cfg)
	if err != nil {
		return ConfigVersion{}, err
	}
	err = s.withTx(ctx, nil, func(tx *sql.Tx) error {
		if cfg.Version <= 0 {
			row := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM config_versions WHERE server_id=? AND config_id=?;`, cfg.ServerID, cfg.ConfigID)
			if err := row.Scan(&cfg.Version); err != nil {
				return err
			}
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO config_versions(id, server_id, config_id, version, content_enc, created_at, validation_status, validation_errors_json)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
			cfg.ID, cfg.ServerID, cfg.ConfigID, cfg.Version, cfg.ContentEnc, cfg.CreatedAt, cfg.ValidationStatus, string(cfg.ValidationErrorsJSON))
		return err
	})
	if err != nil {
		return ConfigVersion{}, err
	}
	return cfg, nil
}

func (s *Store) ListConfigVersions(ctx context.Context, serverID string, configID string, limit int) ([]ConfigVersion, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, server_id, config_id, version, content_enc, created_at, validation_status, validation_errors_json
		FROM config_versions
		WHERE server_id=? AND config_id=?
		ORDER BY version DESC, created_at DESC, id DESC
		LIMIT ?;`, serverID, configID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConfigVersion
	for rows.Next() {
		cfg, err := scanConfigVersion(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, cfg)
	}
	return out, rows.Err()
}

func (s *Store) GetLatestConfigVersion(ctx context.Context, serverID string, configID string) (ConfigVersion, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, server_id, config_id, version, content_enc, created_at, validation_status, validation_errors_json
		FROM config_versions
		WHERE server_id=? AND config_id=?
		ORDER BY version DESC, created_at DESC, id DESC
		LIMIT 1;`, serverID, configID)
	return scanConfigVersion(row)
}

func prepareServerForCreate(srv Server) (Server, error) {
	srv.ID = strings.TrimSpace(srv.ID)
	if srv.ID == "" {
		srv.ID = newStoreID()
	}
	srv.Name = strings.TrimSpace(srv.Name)
	srv.Host = strings.TrimSpace(srv.Host)
	srv.SSHUser = strings.TrimSpace(srv.SSHUser)
	srv.ProjectDir = strings.TrimSpace(srv.ProjectDir)
	if srv.Name == "" || srv.Host == "" || srv.SSHUser == "" || srv.ProjectDir == "" {
		return Server{}, fmt.Errorf("server requires name, host, ssh_user, and project_dir")
	}
	srv.ProjectDir = path.Clean(srv.ProjectDir)
	if !path.IsAbs(srv.ProjectDir) {
		return Server{}, fmt.Errorf("project_dir must be an absolute path")
	}
	if srv.SSHPort <= 0 {
		srv.SSHPort = DefaultSSHPort
	}
	if len(srv.RoleHintsJSON) == 0 {
		srv.RoleHintsJSON = []byte(`[]`)
	}
	if len(srv.LabelsJSON) == 0 {
		srv.LabelsJSON = []byte(`{}`)
	}
	ts := nowUTC()
	if srv.CreatedAt == "" {
		srv.CreatedAt = ts
	}
	srv.UpdatedAt = ts
	return srv, nil
}

func prepareServerForUpdate(before Server, after Server) (Server, error) {
	after.ID = before.ID
	after.CreatedAt = before.CreatedAt
	if after.Name == "" {
		after.Name = before.Name
	}
	if after.Host == "" {
		after.Host = before.Host
	}
	if after.SSHPort <= 0 {
		after.SSHPort = before.SSHPort
	}
	if after.SSHUser == "" {
		after.SSHUser = before.SSHUser
	}
	if after.ProjectDir == "" {
		after.ProjectDir = before.ProjectDir
	}
	if after.RoleHintsJSON == nil {
		after.RoleHintsJSON = before.RoleHintsJSON
	}
	if after.LabelsJSON == nil {
		after.LabelsJSON = before.LabelsJSON
	}
	after.LastSeenAt = before.LastSeenAt
	after.LastError = before.LastError
	after.UpdatedAt = nowUTC()
	return prepareServerForCreate(after)
}

func prepareServerCredentialForCreate(cred ServerCredential) (ServerCredential, error) {
	cred.ID = strings.TrimSpace(cred.ID)
	if cred.ID == "" {
		cred.ID = newStoreID()
	}
	cred.ServerID = strings.TrimSpace(cred.ServerID)
	cred.AuthType = normalizeServerCredentialAuthType(cred.AuthType)
	cred.Username = strings.TrimSpace(cred.Username)
	cred.HostKeyFingerprint = strings.TrimSpace(cred.HostKeyFingerprint)
	if cred.ServerID == "" {
		return ServerCredential{}, fmt.Errorf("missing server_id")
	}
	if cred.Username == "" {
		return ServerCredential{}, fmt.Errorf("missing username")
	}
	if err := validateServerCredentialBlobs(cred); err != nil {
		return ServerCredential{}, err
	}
	ts := nowUTC()
	if cred.CreatedAt == "" {
		cred.CreatedAt = ts
	}
	cred.UpdatedAt = ts
	return cred, nil
}

func prepareServerCredentialForUpdate(before ServerCredential, after ServerCredential) (ServerCredential, error) {
	after.ID = before.ID
	after.ServerID = before.ServerID
	if after.AuthType == "" {
		after.AuthType = before.AuthType
	}
	if after.Username == "" {
		after.Username = before.Username
	}
	if after.PrivateKeyEnc == nil {
		after.PrivateKeyEnc = before.PrivateKeyEnc
	}
	if after.PasswordEnc == nil {
		after.PasswordEnc = before.PasswordEnc
	}
	if after.PassphraseEnc == nil {
		after.PassphraseEnc = before.PassphraseEnc
	}
	if after.HostKeyFingerprint == "" {
		after.HostKeyFingerprint = before.HostKeyFingerprint
	}
	after.CreatedAt = before.CreatedAt
	after.UpdatedAt = nowUTC()
	return prepareServerCredentialForCreate(after)
}

func prepareCommandExecutionForCreate(exec CommandExecution) (CommandExecution, error) {
	exec.ID = strings.TrimSpace(exec.ID)
	if exec.ID == "" {
		exec.ID = newStoreID()
	}
	exec.ServerID = strings.TrimSpace(exec.ServerID)
	exec.Kind = strings.TrimSpace(exec.Kind)
	exec.AllowlistID = strings.TrimSpace(exec.AllowlistID)
	exec.RequestedBy = strings.TrimSpace(exec.RequestedBy)
	if exec.ServerID == "" || exec.Kind == "" || exec.AllowlistID == "" {
		return CommandExecution{}, fmt.Errorf("command execution requires server_id, kind, and allowlist_id")
	}
	if len(exec.ParamsJSON) == 0 {
		exec.ParamsJSON = []byte(`{}`)
	}
	if exec.Status == "" {
		exec.Status = "PENDING"
	}
	if err := validateCommandExecutionStatus(exec.Status); err != nil {
		return CommandExecution{}, err
	}
	if exec.Status == "RUNNING" && exec.StartedAt == nil {
		started := nowUTC()
		exec.StartedAt = &started
	}
	if isTerminalExecutionStatus(exec.Status) && exec.FinishedAt == nil {
		finished := nowUTC()
		exec.FinishedAt = &finished
	}
	exec.StdoutTail = trimTail(exec.StdoutTail, executionTailLimitBytes)
	exec.StderrTail = trimTail(exec.StderrTail, executionTailLimitBytes)
	return exec, nil
}

func prepareCommandExecutionUpdate(update CommandExecutionUpdate) (CommandExecutionUpdate, error) {
	update.Status = strings.TrimSpace(update.Status)
	if update.Status == "" {
		return CommandExecutionUpdate{}, fmt.Errorf("%w: missing status", ErrInvalidExecutionStatus)
	}
	if err := validateCommandExecutionStatus(update.Status); err != nil {
		return CommandExecutionUpdate{}, err
	}
	if update.Status == "RUNNING" && update.StartedAt == nil {
		started := nowUTC()
		update.StartedAt = &started
	}
	if isTerminalExecutionStatus(update.Status) && update.FinishedAt == nil {
		finished := nowUTC()
		update.FinishedAt = &finished
	}
	return update, nil
}

func prepareDeploymentForCreate(dep Deployment) (Deployment, error) {
	dep.ID = strings.TrimSpace(dep.ID)
	if dep.ID == "" {
		dep.ID = newStoreID()
	}
	dep.ServerID = strings.TrimSpace(dep.ServerID)
	dep.Component = strings.TrimSpace(dep.Component)
	dep.ScriptID = strings.TrimSpace(dep.ScriptID)
	dep.ExecutionID = strings.TrimSpace(dep.ExecutionID)
	if dep.ServerID == "" || dep.Component == "" || dep.ScriptID == "" {
		return Deployment{}, fmt.Errorf("deployment requires server_id, component, and script_id")
	}
	if err := validateDeploymentComponent(dep.Component); err != nil {
		return Deployment{}, err
	}
	if dep.Status == "" {
		dep.Status = "PENDING"
	}
	if err := validateCommandExecutionStatus(dep.Status); err != nil {
		return Deployment{}, fmt.Errorf("%w: %v", ErrInvalidDeploymentStatus, err)
	}
	if dep.Status == "RUNNING" && dep.StartedAt == nil {
		started := nowUTC()
		dep.StartedAt = &started
	}
	if isTerminalExecutionStatus(dep.Status) && dep.FinishedAt == nil {
		finished := nowUTC()
		dep.FinishedAt = &finished
	}
	return dep, nil
}

func prepareDeploymentUpdate(update DeploymentUpdate) (DeploymentUpdate, error) {
	update.Status = strings.TrimSpace(update.Status)
	if update.Status == "" {
		return DeploymentUpdate{}, fmt.Errorf("%w: missing status", ErrInvalidDeploymentStatus)
	}
	if err := validateCommandExecutionStatus(update.Status); err != nil {
		return DeploymentUpdate{}, fmt.Errorf("%w: %v", ErrInvalidDeploymentStatus, err)
	}
	if update.Status == "RUNNING" && update.StartedAt == nil {
		started := nowUTC()
		update.StartedAt = &started
	}
	if isTerminalExecutionStatus(update.Status) && update.FinishedAt == nil {
		finished := nowUTC()
		update.FinishedAt = &finished
	}
	if update.ExecutionID != nil {
		execID := strings.TrimSpace(*update.ExecutionID)
		update.ExecutionID = &execID
	}
	return update, nil
}

func prepareConfigVersionForSave(cfg ConfigVersion) (ConfigVersion, error) {
	cfg.ID = strings.TrimSpace(cfg.ID)
	if cfg.ID == "" {
		cfg.ID = newStoreID()
	}
	cfg.ServerID = strings.TrimSpace(cfg.ServerID)
	cfg.ConfigID = strings.TrimSpace(cfg.ConfigID)
	cfg.ValidationStatus = strings.TrimSpace(cfg.ValidationStatus)
	if cfg.ServerID == "" || cfg.ConfigID == "" {
		return ConfigVersion{}, fmt.Errorf("config version requires server_id and config_id")
	}
	if len(cfg.ContentEnc) == 0 {
		return ConfigVersion{}, fmt.Errorf("missing config content")
	}
	if cfg.ValidationStatus == "" {
		cfg.ValidationStatus = "unknown"
	}
	if len(cfg.ValidationErrorsJSON) == 0 {
		cfg.ValidationErrorsJSON = []byte(`[]`)
	}
	if cfg.CreatedAt == "" {
		cfg.CreatedAt = nowUTC()
	}
	return cfg, nil
}

func createServerTx(ctx context.Context, tx *sql.Tx, srv Server) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO servers(id, name, host, ssh_port, ssh_user, project_dir, role_hints_json, labels_json, created_at, updated_at, last_seen_at, last_error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		srv.ID, srv.Name, srv.Host, srv.SSHPort, srv.SSHUser, srv.ProjectDir, string(srv.RoleHintsJSON), string(srv.LabelsJSON), srv.CreatedAt, srv.UpdatedAt, srv.LastSeenAt, srv.LastError)
	return err
}

func updateServerTx(ctx context.Context, tx *sql.Tx, srv Server) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE servers
		SET name=?, host=?, ssh_port=?, ssh_user=?, project_dir=?, role_hints_json=?, labels_json=?, updated_at=?, last_seen_at=?, last_error=?
		WHERE id=?;`,
		srv.Name, srv.Host, srv.SSHPort, srv.SSHUser, srv.ProjectDir, string(srv.RoleHintsJSON), string(srv.LabelsJSON), srv.UpdatedAt, srv.LastSeenAt, srv.LastError, srv.ID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func createServerCredentialTx(ctx context.Context, tx *sql.Tx, cred ServerCredential) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO server_credentials(id, server_id, auth_type, username, private_key_enc, password_enc, passphrase_enc, host_key_fingerprint, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		cred.ID, cred.ServerID, cred.AuthType, cred.Username, cred.PrivateKeyEnc, cred.PasswordEnc, cred.PassphraseEnc, cred.HostKeyFingerprint, cred.CreatedAt, cred.UpdatedAt)
	return err
}

func updateServerCredentialTx(ctx context.Context, tx *sql.Tx, cred ServerCredential) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE server_credentials
		SET auth_type=?, username=?, private_key_enc=?, password_enc=?, passphrase_enc=?, host_key_fingerprint=?, updated_at=?
		WHERE server_id=?;`,
		cred.AuthType, cred.Username, cred.PrivateKeyEnc, cred.PasswordEnc, cred.PassphraseEnc, cred.HostKeyFingerprint, cred.UpdatedAt, cred.ServerID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func createCommandExecutionTx(ctx context.Context, tx *sql.Tx, exec CommandExecution) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO command_executions(id, server_id, kind, allowlist_id, params_json, status, exit_code, started_at, finished_at, stdout_tail, stderr_tail, error, requested_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		exec.ID, exec.ServerID, exec.Kind, exec.AllowlistID, string(exec.ParamsJSON), exec.Status, exec.ExitCode, exec.StartedAt, exec.FinishedAt, exec.StdoutTail, exec.StderrTail, exec.Error, exec.RequestedBy)
	return err
}

func updateCommandExecutionStatusTx(ctx context.Context, tx *sql.Tx, id string, update CommandExecutionUpdate) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE command_executions
		SET status=?,
			exit_code=COALESCE(?, exit_code),
			started_at=COALESCE(?, started_at),
			finished_at=COALESCE(?, finished_at),
			error=?
		WHERE id=?;`,
		update.Status, update.ExitCode, update.StartedAt, update.FinishedAt, update.Error, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func createDeploymentTx(ctx context.Context, tx *sql.Tx, dep Deployment) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO deployments(id, server_id, component, script_id, status, execution_id, started_at, finished_at, error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		dep.ID, dep.ServerID, dep.Component, dep.ScriptID, dep.Status, dep.ExecutionID, dep.StartedAt, dep.FinishedAt, dep.Error)
	return err
}

func updateDeploymentStatusTx(ctx context.Context, tx *sql.Tx, id string, update DeploymentUpdate) error {
	var executionID any
	if update.ExecutionID != nil {
		executionID = *update.ExecutionID
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE deployments
		SET status=?,
			execution_id=COALESCE(?, execution_id),
			started_at=COALESCE(?, started_at),
			finished_at=COALESCE(?, finished_at),
			error=?
		WHERE id=?;`,
		update.Status, executionID, update.StartedAt, update.FinishedAt, update.Error, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

type serverScanner interface {
	Scan(dest ...any) error
}

func scanServer(scanner serverScanner) (Server, error) {
	var (
		srv       Server
		roleHints string
		labels    string
	)
	if err := scanner.Scan(&srv.ID, &srv.Name, &srv.Host, &srv.SSHPort, &srv.SSHUser, &srv.ProjectDir, &roleHints, &labels, &srv.CreatedAt, &srv.UpdatedAt, &srv.LastSeenAt, &srv.LastError); err != nil {
		return Server{}, err
	}
	srv.RoleHintsJSON = []byte(roleHints)
	srv.LabelsJSON = []byte(labels)
	return srv, nil
}

func scanServerCredential(scanner serverScanner) (ServerCredential, error) {
	var cred ServerCredential
	if err := scanner.Scan(&cred.ID, &cred.ServerID, &cred.AuthType, &cred.Username, &cred.PrivateKeyEnc, &cred.PasswordEnc, &cred.PassphraseEnc, &cred.HostKeyFingerprint, &cred.CreatedAt, &cred.UpdatedAt); err != nil {
		return ServerCredential{}, err
	}
	return cred, nil
}

func scanCommandExecution(scanner serverScanner) (CommandExecution, error) {
	var (
		exec      CommandExecution
		params    string
		exitCode  sql.NullInt64
		startedAt sql.NullString
		finished  sql.NullString
		execErr   sql.NullString
	)
	if err := scanner.Scan(&exec.ID, &exec.ServerID, &exec.Kind, &exec.AllowlistID, &params, &exec.Status, &exitCode, &startedAt, &finished, &exec.StdoutTail, &exec.StderrTail, &execErr, &exec.RequestedBy); err != nil {
		return CommandExecution{}, err
	}
	exec.ParamsJSON = []byte(params)
	if exitCode.Valid {
		v := int(exitCode.Int64)
		exec.ExitCode = &v
	}
	if startedAt.Valid {
		v := startedAt.String
		exec.StartedAt = &v
	}
	if finished.Valid {
		v := finished.String
		exec.FinishedAt = &v
	}
	if execErr.Valid {
		v := execErr.String
		exec.Error = &v
	}
	return exec, nil
}

func scanDeployment(scanner serverScanner) (Deployment, error) {
	var (
		dep       Deployment
		execution sql.NullString
		startedAt sql.NullString
		finished  sql.NullString
		depErr    sql.NullString
	)
	if err := scanner.Scan(&dep.ID, &dep.ServerID, &dep.Component, &dep.ScriptID, &dep.Status, &execution, &startedAt, &finished, &depErr); err != nil {
		return Deployment{}, err
	}
	if execution.Valid {
		dep.ExecutionID = execution.String
	}
	if startedAt.Valid {
		v := startedAt.String
		dep.StartedAt = &v
	}
	if finished.Valid {
		v := finished.String
		dep.FinishedAt = &v
	}
	if depErr.Valid {
		v := depErr.String
		dep.Error = &v
	}
	return dep, nil
}

func scanConfigVersion(scanner serverScanner) (ConfigVersion, error) {
	var (
		cfg              ConfigVersion
		validationErrors string
	)
	if err := scanner.Scan(&cfg.ID, &cfg.ServerID, &cfg.ConfigID, &cfg.Version, &cfg.ContentEnc, &cfg.CreatedAt, &cfg.ValidationStatus, &validationErrors); err != nil {
		return ConfigVersion{}, err
	}
	cfg.ValidationErrorsJSON = []byte(validationErrors)
	return cfg, nil
}

func normalizeServerCredentialAuthType(authType string) string {
	switch strings.TrimSpace(strings.ToLower(authType)) {
	case "", serverAuthTypePassword:
		return serverAuthTypePassword
	case serverAuthTypePrivateKey:
		return serverAuthTypePrivateKey
	default:
		return strings.TrimSpace(strings.ToLower(authType))
	}
}

func validateServerCredentialSecret(authType string, secret ServerCredentialSecret) error {
	switch normalizeServerCredentialAuthType(authType) {
	case serverAuthTypePassword:
		if strings.TrimSpace(secret.Password) == "" {
			return fmt.Errorf("password auth requires password")
		}
	case serverAuthTypePrivateKey:
		if strings.TrimSpace(secret.PrivateKey) == "" {
			return fmt.Errorf("private_key auth requires private_key")
		}
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedServerAuthType, authType)
	}
	if strings.TrimSpace(secret.Username) == "" {
		return fmt.Errorf("missing username")
	}
	return nil
}

func validateServerCredentialBlobs(cred ServerCredential) error {
	switch cred.AuthType {
	case serverAuthTypePassword:
		if len(cred.PasswordEnc) == 0 {
			return fmt.Errorf("password auth requires password")
		}
	case serverAuthTypePrivateKey:
		if len(cred.PrivateKeyEnc) == 0 {
			return fmt.Errorf("private_key auth requires private_key")
		}
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedServerAuthType, cred.AuthType)
	}
	return nil
}

func encryptOptionalSecret(k secretcrypto.Key, value string, aad []byte) ([]byte, error) {
	if strings.TrimSpace(value) == "" {
		return nil, nil
	}
	if k.IsZero() {
		return nil, ErrMasterKeyRequired
	}
	return secretcrypto.Encrypt(k, []byte(value), aad)
}

func decryptOptionalSecret(k secretcrypto.Key, blob []byte, aad []byte) (string, error) {
	if len(blob) == 0 {
		return "", nil
	}
	plain, err := secretcrypto.Decrypt(k, blob, aad)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func serverCredentialAAD(serverID string, field string) []byte {
	return []byte("server:" + strings.TrimSpace(serverID) + ":" + field)
}

func validateCommandExecutionStatus(status string) error {
	switch strings.TrimSpace(status) {
	case "PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELED":
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrInvalidExecutionStatus, status)
	}
}

func isTerminalExecutionStatus(status string) bool {
	switch strings.TrimSpace(status) {
	case "SUCCEEDED", "FAILED", "CANCELED":
		return true
	default:
		return false
	}
}

func validateDeploymentComponent(component string) error {
	switch strings.TrimSpace(component) {
	case "master", "worker", "minio", "postgres", "ice-rest-catalog":
		return nil
	default:
		return fmt.Errorf("%w: %q", ErrInvalidDeploymentComponent, component)
	}
}

func appendTail(current string, chunk string, limit int) string {
	if chunk == "" {
		return current
	}
	return trimTail(current+chunk, limit)
}

func trimTail(s string, limit int) string {
	if limit <= 0 || len(s) <= limit {
		return s
	}
	return s[len(s)-limit:]
}
