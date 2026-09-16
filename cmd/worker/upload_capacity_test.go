package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"google.golang.org/grpc"
)

func TestUploadCapacityWaitsWithoutFailingAndReleases(t *testing.T) {
	var calls atomic.Int32
	var releases atomic.Int32
	cp := fakeControlPlaneClient{
		acquireUpload: func(context.Context, *grpcpb.AcquireUploadCapacityRequest, ...grpc.CallOption) (*grpcpb.AcquireUploadCapacityResponse, error) {
			if calls.Add(1) < 3 {
				return &grpcpb.AcquireUploadCapacityResponse{RetryAfterMs: 1}, nil
			}
			return &grpcpb.AcquireUploadCapacityResponse{
				Acquired: true, LeaseId: "lease", LeaseToken: "secret", LeaseDeadlineUnixMs: time.Now().Add(time.Minute).UnixMilli(),
			}, nil
		},
		releaseUpload: func(_ context.Context, req *grpcpb.ReleaseUploadCapacityRequest, _ ...grpc.CallOption) (*grpcpb.ReleaseUploadCapacityResponse, error) {
			if req.LeaseId != "lease" || req.LeaseToken != "secret" || req.BootId != "boot-test" {
				t.Fatalf("unexpected release credential")
			}
			releases.Add(1)
			return &grpcpb.ReleaseUploadCapacityResponse{}, nil
		},
	}
	task := &grpcpb.TaskAssignment{TaskId: "task", AttemptId: "attempt", FencingToken: "fence"}
	uploadCtx, closeGuard, err := holdUploadCapacity(context.Background(), cp, "worker", "boot-test", task)
	if err != nil {
		t.Fatalf("hold capacity: %v", err)
	}
	if uploadCtx.Err() != nil {
		t.Fatalf("upload context canceled while admitted: %v", uploadCtx.Err())
	}
	if err := closeGuard(); err != nil {
		t.Fatalf("close capacity guard: %v", err)
	}
	if calls.Load() != 3 || releases.Load() != 1 {
		t.Fatalf("acquire_calls=%d releases=%d want 3/1", calls.Load(), releases.Load())
	}
}

func TestUploadCapacityRevokesContextOnLostLease(t *testing.T) {
	var calls int
	cp := fakeControlPlaneClient{
		acquireUpload: func(context.Context, *grpcpb.AcquireUploadCapacityRequest, ...grpc.CallOption) (*grpcpb.AcquireUploadCapacityResponse, error) {
			calls++
			if calls == 1 {
				return &grpcpb.AcquireUploadCapacityResponse{
					Acquired: true, LeaseId: "lease", LeaseToken: "secret", LeaseDeadlineUnixMs: time.Now().Add(100 * time.Millisecond).UnixMilli(),
				}, nil
			}
			return &grpcpb.AcquireUploadCapacityResponse{
				Acquired: false, RetryAfterMs: 500,
			}, nil
		},
		releaseUpload: func(context.Context, *grpcpb.ReleaseUploadCapacityRequest, ...grpc.CallOption) (*grpcpb.ReleaseUploadCapacityResponse, error) {
			return &grpcpb.ReleaseUploadCapacityResponse{}, nil
		},
	}
	task := &grpcpb.TaskAssignment{TaskId: "task", AttemptId: "attempt", FencingToken: "fence"}

	uploadCtx, closeGuard, err := holdUploadCapacity(context.Background(), cp, "worker", "boot-test", task)
	if err != nil {
		t.Fatalf("hold capacity: %v", err)
	}

	// Wait for the lease to expire (and fail to renew).
	select {
	case <-uploadCtx.Done():
		// Expected: context is revoked when lease is lost
	case <-time.After(1 * time.Second):
		t.Fatal("upload context was not canceled when lease expired")
	}

	// Close guard should now return the ownership lost error.
	err = closeGuard()
	if err == nil {
		t.Fatal("expected ownership lost error from closeGuard, got nil")
	}
}
