package main

import (
	"context"
	"fmt"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func acquireUploadCapacity(ctx context.Context, cp grpcpb.ControlPlaneClient, workerID string, task *grpcpb.TaskAssignment) (*grpcpb.AcquireUploadCapacityResponse, error) {
	for {
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		resp, err := cp.AcquireUploadCapacity(callCtx, &grpcpb.AcquireUploadCapacityRequest{
			WorkerId: workerID, TaskId: task.TaskId, AttemptId: task.AttemptId, FencingToken: task.FencingToken,
		})
		cancel()
		if err == nil && resp.GetAcquired() {
			return resp, nil
		}
		if status.Code(err) == codes.FailedPrecondition {
			return nil, &taskOwnershipLostError{err: err}
		}
		if err != nil && !isTransientRPCError(err) {
			return nil, err
		}
		delay := 250 * time.Millisecond
		if resp != nil && resp.RetryAfterMs > 0 {
			delay = time.Duration(resp.RetryAfterMs) * time.Millisecond
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
}

// holdUploadCapacity waits without failing the task, then keeps the
// master-issued global capacity lease alive until closeGuard is called.
func holdUploadCapacity(ctx context.Context, cp grpcpb.ControlPlaneClient, workerID string, task *grpcpb.TaskAssignment) (context.Context, func() error, error) {
	grant, err := acquireUploadCapacity(ctx, cp, workerID, task)
	if err != nil {
		return nil, nil, err
	}
	uploadCtx, cancelUploads := context.WithCancel(ctx)
	renewDone := make(chan struct{})
	lost := make(chan error, 1)
	go func() {
		defer close(renewDone)
		deadline := time.UnixMilli(grant.LeaseDeadlineUnixMs)
		for {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				select {
				case lost <- context.DeadlineExceeded:
				default:
				}
				cancelUploads()
				return
			}
			delay := remaining / 3
			if delay > 10*time.Second {
				delay = 10 * time.Second
			}
			if delay < 100*time.Millisecond {
				delay = 100 * time.Millisecond
			}
			timer := time.NewTimer(delay)
			select {
			case <-uploadCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			callCtx, cancel := context.WithTimeout(uploadCtx, 3*time.Second)
			resp, renewErr := cp.AcquireUploadCapacity(callCtx, &grpcpb.AcquireUploadCapacityRequest{
				WorkerId: workerID, TaskId: task.TaskId, AttemptId: task.AttemptId, FencingToken: task.FencingToken,
			})
			cancel()
			if renewErr == nil && resp.GetAcquired() && resp.LeaseId == grant.LeaseId && resp.LeaseToken == grant.LeaseToken {
				deadline = time.UnixMilli(resp.LeaseDeadlineUnixMs)
				continue
			}
			if renewErr != nil && isTransientRPCError(renewErr) && time.Now().Before(deadline) {
				continue
			}
			if renewErr == nil {
				renewErr = fmt.Errorf("upload capacity lease was not renewed")
			}
			select {
			case lost <- renewErr:
			default:
			}
			cancelUploads()
			return
		}
	}()

	closeGuard := func() error {
		cancelUploads()
		<-renewDone
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		_, _ = cp.ReleaseUploadCapacity(releaseCtx, &grpcpb.ReleaseUploadCapacityRequest{
			WorkerId: workerID, TaskId: task.TaskId, AttemptId: task.AttemptId, LeaseId: grant.LeaseId, LeaseToken: grant.LeaseToken,
		})
		releaseCancel()
		select {
		case err := <-lost:
			return &taskOwnershipLostError{err: err}
		default:
			return nil
		}
	}
	return uploadCtx, closeGuard, nil
}
