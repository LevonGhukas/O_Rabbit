package grpcapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	workerAuthorizationMetadataKey = "authorization"
	controlPlaneMethodPrefix       = "/orabbit.v1.ControlPlane/"
)

// WorkerAuthUnaryClientInterceptor attaches the configured worker credential
// to every unary control-plane call. An empty token preserves loopback-only
// local development.
func WorkerAuthUnaryClientInterceptor(token string) grpc.UnaryClientInterceptor {
	token = strings.TrimSpace(token)
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, workerAuthorizationMetadataKey, "Bearer "+token)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func workerAuthUnaryServerInterceptor(token string) grpc.UnaryServerInterceptor {
	token = strings.TrimSpace(token)
	expectedDigest := sha256.Sum256([]byte(token))

	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if token == "" || !strings.HasPrefix(info.FullMethod, controlPlaneMethodPrefix) {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok || !constantTimeWorkerBearerMatch(md.Get(workerAuthorizationMetadataKey), expectedDigest) {
			return nil, grpcstatus.Error(codes.Unauthenticated, "worker authentication failed")
		}
		return handler(ctx, req)
	}
}

func constantTimeWorkerBearerMatch(values []string, expectedDigest [sha256.Size]byte) bool {
	if len(values) != 1 {
		return false
	}
	const prefix = "Bearer "
	header := strings.TrimSpace(values[0])
	validScheme := len(header) >= len(prefix) && strings.EqualFold(header[:len(prefix)], prefix)
	presented := ""
	if validScheme {
		presented = strings.TrimSpace(header[len(prefix):])
	}
	presentedDigest := sha256.Sum256([]byte(presented))
	return validScheme &&
		presented != "" &&
		subtle.ConstantTimeCompare(presentedDigest[:], expectedDigest[:]) == 1
}
