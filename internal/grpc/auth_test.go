package grpcapi

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestWorkerAuthUnaryServerInterceptor(t *testing.T) {
	const token = "worker-token-that-must-not-leak"
	tests := []struct {
		name          string
		requiredToken string
		method        string
		authorization string
		wantCode      codes.Code
		wantHandled   bool
	}{
		{
			name:          "missing token rejected",
			requiredToken: token,
			method:        grpcpb.ControlPlane_RegisterWorker_FullMethodName,
			wantCode:      codes.Unauthenticated,
		},
		{
			name:          "invalid token rejected",
			requiredToken: token,
			method:        grpcpb.ControlPlane_RegisterWorker_FullMethodName,
			authorization: "Bearer wrong-token",
			wantCode:      codes.Unauthenticated,
		},
		{
			name:          "valid token accepted",
			requiredToken: token,
			method:        grpcpb.ControlPlane_RegisterWorker_FullMethodName,
			authorization: "Bearer " + token,
			wantCode:      codes.OK,
			wantHandled:   true,
		},
		{
			name:          "health RPC remains unauthenticated",
			requiredToken: token,
			method:        "/grpc.health.v1.Health/Check",
			wantCode:      codes.OK,
			wantHandled:   true,
		},
		{
			name:        "empty requirement preserves local development",
			method:      grpcpb.ControlPlane_RegisterWorker_FullMethodName,
			wantCode:    codes.OK,
			wantHandled: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.authorization != "" {
				ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(
					workerAuthorizationMetadataKey,
					tc.authorization,
				))
			}
			handled := false
			interceptor := workerAuthUnaryServerInterceptor(tc.requiredToken)
			_, err := interceptor(
				ctx,
				&grpcpb.RegisterWorkerRequest{},
				&grpc.UnaryServerInfo{FullMethod: tc.method},
				func(context.Context, any) (any, error) {
					handled = true
					return &grpcpb.RegisterWorkerResponse{}, nil
				},
			)

			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("status=%v want %v; err=%v", got, tc.wantCode, err)
			}
			if handled != tc.wantHandled {
				t.Fatalf("handled=%t want %t", handled, tc.wantHandled)
			}
			if err != nil && strings.Contains(err.Error(), token) {
				t.Fatal("authentication error leaked worker token")
			}
		})
	}
}

func TestWorkerAuthUnaryClientInterceptor(t *testing.T) {
	const token = "worker-token"
	interceptor := WorkerAuthUnaryClientInterceptor(token)
	invoked := false

	err := interceptor(
		context.Background(),
		grpcpb.ControlPlane_Heartbeat_FullMethodName,
		&grpcpb.HeartbeatRequest{},
		&grpcpb.HeartbeatResponse{},
		nil,
		func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			invoked = true
			md, ok := metadata.FromOutgoingContext(ctx)
			if !ok {
				t.Fatal("worker authentication metadata missing")
			}
			got := md.Get(workerAuthorizationMetadataKey)
			if len(got) != 1 || got[0] != "Bearer "+token {
				t.Fatalf("authorization metadata=%q", got)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !invoked {
		t.Fatal("invoker was not called")
	}
}

func TestWorkerAuthFailureDoesNotLogOrReturnTokens(t *testing.T) {
	const (
		expected  = "expected-worker-token"
		presented = "invalid-presented-token"
	)
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previousLogger)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		workerAuthorizationMetadataKey,
		"Bearer "+presented,
	))
	_, err := workerAuthUnaryServerInterceptor(expected)(
		ctx,
		&grpcpb.RegisterWorkerRequest{},
		&grpc.UnaryServerInfo{FullMethod: grpcpb.ControlPlane_RegisterWorker_FullMethodName},
		func(context.Context, any) (any, error) {
			t.Fatal("unauthenticated request reached handler")
			return nil, nil
		},
	)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("status=%v want %v", status.Code(err), codes.Unauthenticated)
	}
	for _, token := range []string{expected, presented} {
		if strings.Contains(err.Error(), token) {
			t.Fatal("authentication error returned a token")
		}
		if strings.Contains(logs.String(), token) {
			t.Fatal("authentication failure logged a token")
		}
	}
}
