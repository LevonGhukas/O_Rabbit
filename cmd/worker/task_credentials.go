package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// taskCredentials are the secrets of one task attempt. They are fetched from
// the master only while this worker holds the attempt's lease and live only
// in the attempt's context.
type taskCredentials struct {
	SourceDSN string
	S3        aws.CredentialsProvider
}

type taskCredentialsKey struct{}

func withTaskCredentials(ctx context.Context, c taskCredentials) context.Context {
	return context.WithValue(ctx, taskCredentialsKey{}, c)
}

func taskCredentialsFromContext(ctx context.Context) taskCredentials {
	c, _ := ctx.Value(taskCredentialsKey{}).(taskCredentials)
	return c
}

func fetchTaskCredentials(ctx context.Context, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment) (taskCredentials, error) {
	resp, err := requestTaskCredentials(ctx, cp, workerID, t)
	if err != nil {
		return taskCredentials{}, err
	}
	return taskCredentials{
		SourceDSN: resp.SourceDsn,
		S3:        &taskS3Credentials{cp: cp, workerID: workerID, task: t, pending: resp},
	}, nil
}

func requestTaskCredentials(ctx context.Context, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment) (*grpcpb.GetTaskCredentialsResponse, error) {
	req := &grpcpb.GetTaskCredentialsRequest{WorkerId: workerID, TaskId: t.TaskId, AttemptId: t.AttemptId, FencingToken: t.FencingToken}
	backoff := 500 * time.Millisecond
	for attempt := 1; ; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		resp, err := cp.GetTaskCredentials(callCtx, req)
		cancel()
		if err == nil {
			return resp, nil
		}
		switch status.Code(err) {
		case codes.FailedPrecondition, codes.PermissionDenied:
			return nil, &taskOwnershipLostError{err: err}
		case codes.Unavailable, codes.DeadlineExceeded:
			if attempt < 4 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(backoff):
				}
				backoff *= 2
				continue
			}
		}
		return nil, fmt.Errorf("fetch task credentials: %w", err)
	}
}

// taskS3Credentials serves the attempt's S3 credentials to the AWS SDK. The
// first retrieval uses the credentials fetched with the task; temporary
// (STS) credentials are fetched again from the master when they near expiry.
type taskS3Credentials struct {
	cp       grpcpb.ControlPlaneClient
	workerID string
	task     *grpcpb.TaskAssignment

	mu      sync.Mutex
	pending *grpcpb.GetTaskCredentialsResponse
}

func (p *taskS3Credentials) Retrieve(ctx context.Context) (aws.Credentials, error) {
	p.mu.Lock()
	resp := p.pending
	p.pending = nil
	p.mu.Unlock()
	if resp == nil {
		var err error
		if resp, err = requestTaskCredentials(ctx, p.cp, p.workerID, p.task); err != nil {
			return aws.Credentials{}, err
		}
	}
	c := aws.Credentials{
		AccessKeyID:     resp.S3AccessKeyId,
		SecretAccessKey: resp.S3SecretAccessKey,
		SessionToken:    resp.S3SessionToken,
		Source:          "orabbit-task-credentials",
	}
	if resp.S3ExpiresAtUnixMs > 0 {
		c.CanExpire, c.Expires = true, time.UnixMilli(resp.S3ExpiresAtUnixMs)
	}
	return c, nil
}
