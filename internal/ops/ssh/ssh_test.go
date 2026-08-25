package ssh

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	gossh "golang.org/x/crypto/ssh"
)

type testSSHServer struct {
	addr             string
	hostFingerprint  string
	passwordUser     string
	passwordValue    string
	allowedPublicKey gossh.PublicKey

	listener net.Listener
	done     chan struct{}
	wg       sync.WaitGroup
}

var errUnauthorized = errors.New("unauthorized")

func startTestSSHServer(t *testing.T, passwordUser string, passwordValue string, allowedPublicKey gossh.PublicKey) *testSSHServer {
	t.Helper()

	hostPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate host key: %v", err)
	}
	hostSigner, err := gossh.NewSignerFromKey(hostPrivateKey)
	if err != nil {
		t.Fatalf("host signer: %v", err)
	}

	cfg := &gossh.ServerConfig{
		PasswordCallback: func(meta gossh.ConnMetadata, password []byte) (*gossh.Permissions, error) {
			if passwordUser == "" {
				return nil, errUnauthorized
			}
			if meta.User() == passwordUser && string(password) == passwordValue {
				return nil, nil
			}
			return nil, errUnauthorized
		},
		PublicKeyCallback: func(meta gossh.ConnMetadata, pubKey gossh.PublicKey) (*gossh.Permissions, error) {
			if allowedPublicKey == nil {
				return nil, errUnauthorized
			}
			if string(pubKey.Marshal()) == string(allowedPublicKey.Marshal()) {
				return nil, nil
			}
			return nil, errUnauthorized
		},
	}
	cfg.AddHostKey(hostSigner)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := &testSSHServer{
		addr:             ln.Addr().String(),
		hostFingerprint:  gossh.FingerprintSHA256(hostSigner.PublicKey()),
		passwordUser:     passwordUser,
		passwordValue:    passwordValue,
		allowedPublicKey: allowedPublicKey,
		listener:         ln,
		done:             make(chan struct{}),
	}

	srv.wg.Add(1)
	go func() {
		defer srv.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-srv.done:
					return
				default:
					return
				}
			}
			srv.wg.Add(1)
			go func() {
				defer srv.wg.Done()
				srv.handleConn(cfg, conn)
			}()
		}
	}()

	t.Cleanup(func() {
		close(srv.done)
		_ = srv.listener.Close()
		srv.wg.Wait()
	})
	return srv
}

func (s *testSSHServer) handleConn(cfg *gossh.ServerConfig, conn net.Conn) {
	serverConn, chans, reqs, err := gossh.NewServerConn(conn, cfg)
	if err != nil {
		_ = conn.Close()
		return
	}
	defer serverConn.Close()
	go gossh.DiscardRequests(reqs)
	for newChannel := range chans {
		if newChannel.ChannelType() != "session" {
			_ = newChannel.Reject(gossh.UnknownChannelType, "unsupported channel")
			continue
		}
		channel, requests, err := newChannel.Accept()
		if err != nil {
			continue
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer channel.Close()
			s.handleSession(channel, requests)
		}()
	}
}

func (s *testSSHServer) handleSession(channel gossh.Channel, requests <-chan *gossh.Request) {
	for req := range requests {
		switch req.Type {
		case "exec":
			var payload struct {
				Command string
			}
			_ = gossh.Unmarshal(req.Payload, &payload)
			_ = req.Reply(true, nil)
			switch payload.Command {
			case "mixed":
				_, _ = channel.Write([]byte("stdout line 1\n"))
				_, _ = channel.Stderr().Write([]byte("stderr line 1\n"))
				sendExitStatus(channel, 7)
			case "sleep":
				time.Sleep(300 * time.Millisecond)
				_, _ = channel.Write([]byte("done\n"))
				sendExitStatus(channel, 0)
			default:
				_, _ = channel.Write([]byte("ok\n"))
				sendExitStatus(channel, 0)
			}
			return
		default:
			_ = req.Reply(false, nil)
		}
	}
}

func sendExitStatus(channel gossh.Channel, status uint32) {
	var payload [4]byte
	binary.BigEndian.PutUint32(payload[:], status)
	_, _ = channel.SendRequest("exit-status", false, payload[:])
}

func makePrivateKeyPEM(t *testing.T) (string, gossh.PublicKey) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate private key: %v", err)
	}
	pubKey, err := gossh.NewPublicKey(&privateKey.PublicKey)
	if err != nil {
		t.Fatalf("make public key: %v", err)
	}
	block := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})
	return string(block), pubKey
}

func TestConnectionCapturesFingerprintWithPasswordAuth(t *testing.T) {
	srv := startTestSSHServer(t, "deploy", "secret", nil)

	host, port := splitHostPort(t, srv.addr)
	result, err := TestConnection(context.Background(), SSHTarget{
		Host:               host,
		Port:               port,
		User:               "deploy",
		Password:           "secret",
		HostKeyFingerprint: srv.hostFingerprint,
	})
	if err != nil {
		t.Fatalf("TestConnection: %v", err)
	}
	if result.HostKeyFingerprint != srv.hostFingerprint {
		t.Fatalf("fingerprint=%q want %q", result.HostKeyFingerprint, srv.hostFingerprint)
	}
	if result.ServerVersion == "" {
		t.Fatal("expected server version")
	}
}

func TestConnectionSupportsPrivateKeyAuth(t *testing.T) {
	privateKeyPEM, publicKey := makePrivateKeyPEM(t)
	srv := startTestSSHServer(t, "", "", publicKey)

	host, port := splitHostPort(t, srv.addr)
	result, err := TestConnection(context.Background(), SSHTarget{
		Host:               host,
		Port:               port,
		User:               "deploy",
		PrivateKey:         privateKeyPEM,
		HostKeyFingerprint: srv.hostFingerprint,
	})
	if err != nil {
		t.Fatalf("TestConnection(private key): %v", err)
	}
	if result.HostKeyFingerprint != srv.hostFingerprint {
		t.Fatalf("fingerprint=%q want %q", result.HostKeyFingerprint, srv.hostFingerprint)
	}
}

func TestConnectionRejectsMissingPinnedFingerprint(t *testing.T) {
	srv := startTestSSHServer(t, "deploy", "secret", nil)
	host, port := splitHostPort(t, srv.addr)
	_, err := TestConnection(context.Background(), SSHTarget{Host: host, Port: port, User: "deploy", Password: "secret"})
	if err == nil || !strings.Contains(err.Error(), "fingerprint mismatch") {
		t.Fatalf("missing fingerprint err=%v", err)
	}
}

func TestConnectionRejectsPinnedFingerprintMismatch(t *testing.T) {
	srv := startTestSSHServer(t, "deploy", "secret", nil)
	host, port := splitHostPort(t, srv.addr)

	_, err := TestConnection(context.Background(), SSHTarget{
		Host:               host,
		Port:               port,
		User:               "deploy",
		Password:           "secret",
		HostKeyFingerprint: "SHA256:not-the-right-fingerprint",
	})
	if err == nil {
		t.Fatal("expected fingerprint mismatch")
	}
	if !strings.Contains(err.Error(), "fingerprint mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecuteCommandCapturesStreamsAndExitCode(t *testing.T) {
	srv := startTestSSHServer(t, "deploy", "secret", nil)
	host, port := splitHostPort(t, srv.addr)

	var (
		mu     sync.Mutex
		chunks []StreamChunk
	)

	result, err := ExecuteCommand(context.Background(), SSHTarget{
		Host:               host,
		Port:               port,
		User:               "deploy",
		Password:           "secret",
		HostKeyFingerprint: srv.hostFingerprint,
	}, "mixed", func(chunk StreamChunk) {
		mu.Lock()
		defer mu.Unlock()

		chunks = append(chunks, chunk)
	})
	if err != nil {
		t.Fatalf("ExecuteCommand: %v", err)
	}
	if result.ExitCode != 7 {
		t.Fatalf("exit_code=%d want 7", result.ExitCode)
	}
	if !strings.Contains(result.StdoutTail, "stdout line 1") {
		t.Fatalf("stdout_tail=%q", result.StdoutTail)
	}
	if !strings.Contains(result.StderrTail, "stderr line 1") {
		t.Fatalf("stderr_tail=%q", result.StderrTail)
	}

	mu.Lock()
	chunkCount := len(chunks)
	mu.Unlock()
	if chunkCount < 2 {
		t.Fatalf("expected streamed chunks, got %d", chunkCount)
	}
}

func TestExecuteCommandHonorsContextTimeout(t *testing.T) {
	srv := startTestSSHServer(t, "deploy", "secret", nil)
	host, port := splitHostPort(t, srv.addr)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result, err := ExecuteCommand(ctx, SSHTarget{
		Host:               host,
		Port:               port,
		User:               "deploy",
		Password:           "secret",
		HostKeyFingerprint: srv.hostFingerprint,
	}, "sleep", nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errorsIsDeadline(err) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if result.ExitCode != -1 {
		t.Fatalf("exit_code=%d want -1", result.ExitCode)
	}
}

func splitHostPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	port, err := net.LookupPort("tcp", portStr)
	if err != nil {
		t.Fatalf("lookup port: %v", err)
	}
	return host, port
}

func errorsIsDeadline(err error) bool {
	return err == context.DeadlineExceeded || strings.Contains(err.Error(), context.DeadlineExceeded.Error())
}
