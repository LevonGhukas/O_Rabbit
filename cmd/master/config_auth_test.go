package main

import (
	"strings"
	"testing"
)

func TestValidateAuthentication(t *testing.T) {
	tests := []struct {
		name        string
		http        string
		grpc        string
		httpToken   string
		workerToken string
		insecure    bool
		wantErr     string
	}{
		{name: "local without token", http: "127.0.0.1:9100", grpc: "localhost:9102"},
		{name: "ipv6 loopback", http: "[::1]:9100", grpc: "[::1]:9102"},
		{name: "remote http with token", http: "0.0.0.0:9100", grpc: "127.0.0.1:9102", httpToken: "http-secret"},
		{name: "remote http without token", http: "0.0.0.0:9100", grpc: "127.0.0.1:9102", wantErr: "requires ORABBIT_HTTP_AUTH_TOKEN"},
		{name: "remote insecure grpc without worker token", http: "127.0.0.1:9100", grpc: "0.0.0.0:9102", insecure: true, wantErr: "requires ORABBIT_WORKER_AUTH_TOKEN"},
		{name: "remote insecure grpc with worker token", http: "127.0.0.1:9100", grpc: "0.0.0.0:9102", workerToken: "worker-secret", insecure: true},
		{name: "unspecified grpc without worker token", http: "127.0.0.1:9100", grpc: ":9102", wantErr: "requires ORABBIT_WORKER_AUTH_TOKEN"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := masterConfig{
				HTTPAddr:        tc.http,
				GRPCAddr:        tc.grpc,
				HTTPAuthToken:   tc.httpToken,
				WorkerAuthToken: tc.workerToken,
				Insecure:        tc.insecure,
			}
			err := cfg.validateAuthentication()
			if tc.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error=%v want substring %q", err, tc.wantErr)
			}
			if err != nil {
				for _, token := range []string{tc.httpToken, tc.workerToken} {
					if token != "" && strings.Contains(err.Error(), token) {
						t.Fatal("validation error leaked token")
					}
				}
			}
		})
	}
}
