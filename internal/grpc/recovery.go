package grpcapi

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// RecoverPanic logs a panic with its stack instead of letting it terminate
// the master. Use it directly as a deferred call at the top of a goroutine:
//
//	defer grpcapi.RecoverPanic(log, "multipart cleanup")
func RecoverPanic(log *slog.Logger, component string) {
	rec := recover()
	if rec == nil {
		return
	}
	if log == nil {
		log = slog.Default()
	}
	log.Error("panic recovered",
		slog.String("component", component),
		slog.String("panic", fmt.Sprint(rec)),
		slog.String("stack", string(debug.Stack())),
	)
}

// recoveryUnaryServerInterceptor turns a handler panic into codes.Internal so
// one faulty request cannot crash the master. It must be the outermost
// interceptor so it also covers the authentication interceptors.
func recoveryUnaryServerInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				logHandlerPanic(log, info.FullMethod, requestWorkerID(req), rec)
				resp, err = nil, grpcstatus.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

func recoveryStreamServerInterceptor(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if rec := recover(); rec != nil {
				logHandlerPanic(log, info.FullMethod, "", rec)
				err = grpcstatus.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(srv, ss)
	}
}

func logHandlerPanic(log *slog.Logger, method, workerID string, rec any) {
	if log == nil {
		log = slog.Default()
	}
	attrs := []any{
		slog.String("method", method),
		slog.String("panic", fmt.Sprint(rec)),
		slog.String("stack", string(debug.Stack())),
	}
	if workerID != "" {
		attrs = append(attrs, slog.String("worker_id", workerID))
	}
	log.Error("grpc handler panic recovered", attrs...)
}

// requestWorkerID reads the request's worker_id. By the time a handler runs,
// the identity interceptor has bound it to the authenticated worker.
func requestWorkerID(req any) string {
	m, ok := req.(proto.Message)
	if !ok {
		return ""
	}
	msg := m.ProtoReflect()
	fd := msg.Descriptor().Fields().ByName("worker_id")
	if fd == nil || fd.Kind() != protoreflect.StringKind {
		return ""
	}
	return msg.Get(fd).String()
}
