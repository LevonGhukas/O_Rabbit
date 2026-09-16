package ssh

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	gossh "golang.org/x/crypto/ssh"
)

const (
	DefaultPort     = 22
	DefaultTailSize = 64 * 1024
)

type SSHTarget struct {
	Host               string
	Port               int
	User               string
	Password           string
	PrivateKey         string
	Passphrase         string
	HostKeyFingerprint string
}

type StreamKind string

const (
	StreamStdout StreamKind = "stdout"
	StreamStderr StreamKind = "stderr"
)

type StreamChunk struct {
	Stream StreamKind
	Data   string
}

type CommandResult struct {
	ExitCode   int
	StdoutTail string
	StderrTail string
	StartedAt  time.Time
	FinishedAt time.Time
}

type TestResult struct {
	HostKeyFingerprint string
	ServerVersion      string
}

type StreamCallback func(StreamChunk)

func TestConnection(ctx context.Context, target SSHTarget) (TestResult, error) {
	client, meta, err := dial(ctx, target)
	if err != nil {
		return TestResult{}, err
	}
	defer client.Close()
	return meta, nil
}

func ExecuteCommand(ctx context.Context, target SSHTarget, command string, onStream StreamCallback) (CommandResult, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return CommandResult{}, fmt.Errorf("missing SSH command")
	}
	startedAt := time.Now().UTC()

	client, _, err := dial(ctx, target)
	if err != nil {
		return CommandResult{}, err
	}
	defer client.Close()

	session, err := client.NewSession()
	if err != nil {
		return CommandResult{}, fmt.Errorf("create SSH session: %w", err)
	}
	defer session.Close()

	stdoutTail := newTailWriter(DefaultTailSize, StreamStdout, onStream)
	stderrTail := newTailWriter(DefaultTailSize, StreamStderr, onStream)
	session.Stdout = stdoutTail
	session.Stderr = stderrTail

	if err := session.Start(command); err != nil {
		return CommandResult{}, fmt.Errorf("start SSH command: %w", err)
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- session.Wait()
	}()

	select {
	case <-ctx.Done():
		_ = session.Close()
		_ = client.Close()
		return CommandResult{
			ExitCode:   -1,
			StdoutTail: stdoutTail.String(),
			StderrTail: stderrTail.String(),
			StartedAt:  startedAt,
			FinishedAt: time.Now().UTC(),
		}, ctx.Err()
	case err := <-waitCh:
		result := CommandResult{
			ExitCode:   0,
			StdoutTail: stdoutTail.String(),
			StderrTail: stderrTail.String(),
			StartedAt:  startedAt,
			FinishedAt: time.Now().UTC(),
		}
		if err == nil {
			return result, nil
		}
		var exitErr *gossh.ExitError
		if errors.As(err, &exitErr) {
			result.ExitCode = exitErr.ExitStatus()
			return result, nil
		}
		return result, fmt.Errorf("wait for SSH command: %w", err)
	}
}

func dial(ctx context.Context, target SSHTarget) (*gossh.Client, TestResult, error) {
	target = normalizeTarget(target)
	if err := validateTarget(target); err != nil {
		return nil, TestResult{}, err
	}

	authMethods, err := buildAuthMethods(target)
	if err != nil {
		return nil, TestResult{}, err
	}

	var meta TestResult
	hostKeyCallback := func(hostname string, remote net.Addr, key gossh.PublicKey) error {
		sha256fp := gossh.FingerprintSHA256(key)
		meta.HostKeyFingerprint = sha256fp
		if !matchesFingerprint(target.HostKeyFingerprint, key) {
			return fmt.Errorf("SSH host key fingerprint mismatch: expected %s, got %s", target.HostKeyFingerprint, sha256fp)
		}
		return nil
	}

	cfg := &gossh.ClientConfig{
		User:            target.User,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
	}

	address := net.JoinHostPort(target.Host, fmt.Sprintf("%d", target.Port))
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, TestResult{}, fmt.Errorf("dial SSH target: %w", err)
	}

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
		defer conn.SetDeadline(time.Time{})
	}

	clientConn, chans, reqs, err := gossh.NewClientConn(conn, address, cfg)
	if err != nil {
		_ = conn.Close()
		return nil, TestResult{}, fmt.Errorf("SSH handshake failed: %w", err)
	}
	meta.ServerVersion = string(clientConn.ServerVersion())
	return gossh.NewClient(clientConn, chans, reqs), meta, nil
}

func normalizeTarget(target SSHTarget) SSHTarget {
	target.Host = strings.TrimSpace(target.Host)
	target.User = strings.TrimSpace(target.User)
	target.HostKeyFingerprint = strings.TrimSpace(target.HostKeyFingerprint)
	if target.Port <= 0 {
		target.Port = DefaultPort
	}
	return target
}

func validateTarget(target SSHTarget) error {
	if target.Host == "" {
		return fmt.Errorf("missing SSH host")
	}
	if target.User == "" {
		return fmt.Errorf("missing SSH user")
	}
	if strings.TrimSpace(target.Password) == "" && strings.TrimSpace(target.PrivateKey) == "" {
		return fmt.Errorf("missing SSH authentication material")
	}
	return nil
}

func buildAuthMethods(target SSHTarget) ([]gossh.AuthMethod, error) {
	methods := make([]gossh.AuthMethod, 0, 2)
	if strings.TrimSpace(target.PrivateKey) != "" {
		signer, err := parseSigner(target.PrivateKey, target.Passphrase)
		if err != nil {
			return nil, err
		}
		methods = append(methods, gossh.PublicKeys(signer))
	}
	if strings.TrimSpace(target.Password) != "" {
		methods = append(methods, gossh.Password(target.Password))
	}
	if len(methods) == 0 {
		return nil, fmt.Errorf("missing SSH authentication method")
	}
	return methods, nil
}

func parseSigner(privateKey string, passphrase string) (gossh.Signer, error) {
	keyBytes := []byte(privateKey)
	if strings.TrimSpace(passphrase) == "" {
		signer, err := gossh.ParsePrivateKey(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("parse SSH private key: %w", err)
		}
		return signer, nil
	}
	signer, err := gossh.ParsePrivateKeyWithPassphrase(keyBytes, []byte(passphrase))
	if err != nil {
		return nil, fmt.Errorf("parse SSH private key with passphrase: %w", err)
	}
	return signer, nil
}

func matchesFingerprint(expected string, key gossh.PublicKey) bool {
	expected = strings.TrimSpace(expected)
	if expected == "" {
		return false
	}
	sha256fp := gossh.FingerprintSHA256(key)
	if constantTimeStringEqual(expected, sha256fp) {
		return true
	}
	legacy := gossh.FingerprintLegacyMD5(key)
	if constantTimeStringEqual(strings.ToLower(expected), strings.ToLower(legacy)) {
		return true
	}
	if strings.HasPrefix(strings.ToUpper(expected), "MD5:") {
		return constantTimeStringEqual(strings.ToLower(strings.TrimPrefix(expected, "MD5:")), strings.ToLower(legacy))
	}
	return false
}

func constantTimeStringEqual(left, right string) bool {
	if len(left) != len(right) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(left), []byte(right)) == 1
}

type tailWriter struct {
	mu       sync.Mutex
	tail     string
	limit    int
	stream   StreamKind
	callback StreamCallback
}

func newTailWriter(limit int, stream StreamKind, callback StreamCallback) *tailWriter {
	if limit <= 0 {
		limit = DefaultTailSize
	}
	return &tailWriter{
		limit:    limit,
		stream:   stream,
		callback: callback,
	}
}

func (w *tailWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	chunk := string(p)
	w.mu.Lock()
	w.tail = trimTail(w.tail+chunk, w.limit)
	w.mu.Unlock()
	if w.callback != nil {
		w.callback(StreamChunk{Stream: w.stream, Data: chunk})
	}
	return len(p), nil
}

func (w *tailWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.tail
}

func trimTail(value string, limit int) string {
	if limit <= 0 || len(value) <= limit {
		return value
	}
	return value[len(value)-limit:]
}

var _ io.Writer = (*tailWriter)(nil)
