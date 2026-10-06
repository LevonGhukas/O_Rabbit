package grpcapi

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestHandlerPanicReturnsInternalAndServerKeepsServing(t *testing.T) {
	var logs syncBuffer
	// A nil store makes RegisterWorker dereference nil: a genuine handler panic.
	srv := NewServer(slog.New(slog.NewTextHandler(&logs, nil)), nil, nil, testCryptoKey, time.Second, nil)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, lis, Config{Insecure: true}, srv) }()
	t.Cleanup(func() { cancel(); <-done })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cp := grpcpb.NewControlPlaneClient(conn)

	for i := 0; i < 2; i++ {
		_, err := cp.RegisterWorker(context.Background(), &grpcpb.RegisterWorkerRequest{WorkerId: "worker-1"})
		requireCode(t, err, codes.Internal)
		if strings.Contains(err.Error(), "nil pointer") {
			t.Fatalf("panic details must not reach the client: %v", err)
		}
	}
	if _, err := grpc_health_v1.NewHealthClient(conn).Check(context.Background(), &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatalf("server must keep serving after a handler panic: %v", err)
	}
	out := logs.String()
	if !strings.Contains(out, "grpc handler panic recovered") || !strings.Contains(out, "RegisterWorker") || !strings.Contains(out, "worker-1") || !strings.Contains(out, "goroutine") {
		t.Fatalf("panic must be logged with method, worker and stack:\n%s", out)
	}
}

func TestStopGracefullyIsBoundedByOpenRPCs(t *testing.T) {
	srv := NewServer(nil, nil, nil, testCryptoKey, time.Second, nil)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(serverOptions(Config{Insecure: true}, srv)...)
	grpc_health_v1.RegisterHealthServer(g, blockingHealth{})
	go func() { _ = g.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := grpc_health_v1.NewHealthClient(conn).Watch(context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	stopGracefully(g, 200*time.Millisecond)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("shutdown took %v with an open RPC", elapsed)
	}
}

// blockingHealth keeps a Watch stream open until the server is stopped.
type blockingHealth struct {
	grpc_health_v1.UnimplementedHealthServer
}

func (blockingHealth) Watch(_ *grpc_health_v1.HealthCheckRequest, s grpc.ServerStreamingServer[grpc_health_v1.HealthCheckResponse]) error {
	if err := s.Send(&grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}); err != nil {
		return err
	}
	<-s.Context().Done()
	return nil
}

func TestRecoverPanicContainsGoroutinePanics(t *testing.T) {
	var logs syncBuffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer RecoverPanic(log, "test job")
		var m map[string]int
		m["boom"] = 1
	}()
	<-done
	if out := logs.String(); !strings.Contains(out, "panic recovered") || !strings.Contains(out, "test job") {
		t.Fatalf("goroutine panic must be logged:\n%s", out)
	}
}
