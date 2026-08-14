package orabbitcli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
)

// httpJSON handles http json behavior.
// It exists to keep this logic isolated and reusable.
func httpJSON(ctx context.Context, method, url string, body any, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = strings.NewReader(string(b))
	}
	req, _ := http.NewRequestWithContext(ctx, method, url, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if tok := strings.TrimSpace(os.Getenv("ORABBIT_HTTP_AUTH_TOKEN")); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if out == nil {
		return nil
	}
	if len(b) == 0 {
		return nil
	}
	return json.Unmarshal(b, out)
}

// normalizeListenAddr normalizes listen addr into a stable format.
// It exists to avoid scattered address/format edge-case handling.
func normalizeListenAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "127.0.0.1")
	addr = strings.TrimPrefix(addr, "localhost")
	if strings.HasPrefix(addr, ":") {
		return addr
	}
	if strings.Contains(addr, ":") {
		_, port, ok := strings.Cut(addr, ":")
		if ok {
			return ":" + port
		}
	}
	return addr
}

// localHTTPBase handles local http base behavior.
// It exists to keep this logic isolated and reusable.
func localHTTPBase(listenAddr string) string {
	return "http://127.0.0.1" + normalizeListenAddr(listenAddr)
}

// localGRPCTarget handles local grpc target behavior.
// It exists to keep this logic isolated and reusable.
func localGRPCTarget(listenAddr string) string {
	return "127.0.0.1" + normalizeListenAddr(listenAddr)
}

// normalizeHTTPBase normalizes http base into a stable format.
// It exists to avoid scattered address/format edge-case handling.
func normalizeHTTPBase(base string) string {
	base = strings.TrimSpace(base)
	if base == "" {
		return ""
	}
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		if strings.HasPrefix(base, ":") {
			base = "127.0.0.1" + base
		}
		base = "http://" + base
	}
	return strings.TrimRight(base, "/")
}

// normalizeGRPCTarget normalizes grpc target into a stable format.
// It exists to avoid scattered address/format edge-case handling.
func normalizeGRPCTarget(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	return addr
}

// guessGRPCTargetFromHTTPBase handles guess grpc target from http base behavior.
// It exists to keep this logic isolated and reusable.
func guessGRPCTargetFromHTTPBase(base string) string {
	base = strings.TrimSpace(base)
	if base == "" {
		return localGRPCTarget(defaultGRPCAddr)
	}
	base = strings.TrimPrefix(base, "http://")
	base = strings.TrimPrefix(base, "https://")
	base = strings.TrimRight(base, "/")
	if slash := strings.IndexByte(base, '/'); slash >= 0 {
		base = base[:slash]
	}
	if base == "" {
		return localGRPCTarget(defaultGRPCAddr)
	}
	host, _, err := net.SplitHostPort(base)
	if err == nil {
		if host == "" {
			host = "127.0.0.1"
		}
		return net.JoinHostPort(host, strings.TrimPrefix(defaultGRPCAddr, ":"))
	}
	if strings.HasPrefix(base, ":") {
		return localGRPCTarget(defaultGRPCAddr)
	}
	if h, _, ok := strings.Cut(base, ":"); ok && h != "" {
		return net.JoinHostPort(h, strings.TrimPrefix(defaultGRPCAddr, ":"))
	}
	return net.JoinHostPort(base, strings.TrimPrefix(defaultGRPCAddr, ":"))
}

// checkHealth handles check health behavior.
// It exists to keep this logic isolated and reusable.
func checkHealth(ctx context.Context, base string) bool {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(base, "/")+"/healthz", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode == 200
}

// checkGRPCTCPHealth is a cheap readiness probe for gRPC listeners.
// If standard gRPC health RPC isn't implemented, TCP dial is the safest fallback.
func checkGRPCTCPHealth(ctx context.Context, addr string) bool {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return false
	}

	if checkGRPCServiceHealth(ctx, addr) {
		return true
	}

	dialer := net.Dialer{Timeout: 600 * time.Millisecond}
	c, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func checkGRPCServiceHealth(ctx context.Context, addr string) bool {
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return false
	}
	defer cc.Close()

	client := grpc_health_v1.NewHealthClient(cc)
	hctx, hcancel := context.WithTimeout(ctx, 700*time.Millisecond)
	defer hcancel()

	resp, err := client.Check(hctx, &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		return false
	}
	return resp.GetStatus() == grpc_health_v1.HealthCheckResponse_SERVING
}

// waitTCP blocks until tcp or timeout.
// It exists to make startup and coordination timing deterministic.
func waitTCP(ctx context.Context, addr string, timeout time.Duration) error {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return fmt.Errorf("empty addr")
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout waiting for tcp %s", addr)
}

// waitHTTP blocks until http or timeout.
// It exists to make startup and coordination timing deterministic.
func waitHTTP(ctx context.Context, url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	cli := &http.Client{Timeout: 1 * time.Second}
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := cli.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout")
}
