package grpcapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	targetCredentialModeStatic = "static"
	targetCredentialModeSTS    = "sts"

	defaultSTSDurationSeconds = 3600
	minSTSDurationSeconds     = 900
	maxSTSDurationSeconds     = 43200
)

// GetTaskCredentials hands a task's source and target credentials to the
// worker that holds the attempt's live lease, and to no one else. Target
// connections with credential_mode "sts" receive temporary S3 credentials
// restricted to the run's object prefix instead of the stored keys.
func (s *Server) GetTaskCredentials(ctx context.Context, req *grpcpb.GetTaskCredentialsRequest) (*grpcpb.GetTaskCredentialsResponse, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.AttemptId) == "" || strings.TrimSpace(req.FencingToken) == "" {
		return nil, grpcstatus.Error(codes.FailedPrecondition, "fenced task protocol required")
	}
	runID, err := s.st.VerifyTaskAttemptOwner(ctx, req.BootId, req.TaskId, req.AttemptId, req.FencingToken, req.WorkerId, s.nowFn())
	if err != nil {
		if db.IsAttemptFenced(err) {
			s.recordAttemptRejection(ctx, req.TaskId, req.AttemptId, req.WorkerId, "STALE_CREDENTIAL_REQUEST_REJECTED", "OWNERSHIP_FENCED")
			return nil, grpcstatus.Error(codes.FailedPrecondition, "task ownership lost")
		}
		return nil, err
	}
	resp, mode, err := s.taskCredentials(ctx, runID, req.AttemptId)
	if err != nil {
		s.log.Error("task credentials unavailable", slog.String("worker_id", req.WorkerId), slog.String("task_id", req.TaskId), slog.String("attempt_id", req.AttemptId), slog.String("err", err.Error()))
		return nil, grpcstatus.Error(codes.Unavailable, "task credentials unavailable")
	}
	s.log.Info("task credentials issued", slog.String("worker_id", req.WorkerId), slog.String("run_id", runID), slog.String("task_id", req.TaskId), slog.String("attempt_id", req.AttemptId), slog.String("target_credential_mode", mode))
	return resp, nil
}

func (s *Server) taskCredentials(ctx context.Context, runID, attemptID string) (*grpcpb.GetTaskCredentialsResponse, string, error) {
	run, err := s.st.GetRun(ctx, runID)
	if err != nil {
		return nil, "", err
	}
	r, err := s.resolveRun(ctx, run)
	if err != nil {
		return nil, "", err
	}
	srcConn, err := s.st.GetConnection(ctx, r.config.Job.SourceConnectionID)
	if err != nil {
		return nil, "", err
	}
	src, err := decryptSecretMap(s.k, srcConn)
	if err != nil {
		return nil, "", fmt.Errorf("source connection secret: %w", err)
	}
	s3cfg, err := s.targetS3Config(ctx, r)
	if err != nil {
		return nil, "", err
	}

	resp := &grpcpb.GetTaskCredentialsResponse{SourceDsn: stringMapValue(src, "dsn")}
	tgtMeta := r.target.Metadata
	mode := strings.ToLower(stringMapValue(tgtMeta, "credential_mode"))
	switch mode {
	case "", targetCredentialModeStatic:
		resp.S3AccessKeyId, resp.S3SecretAccessKey, resp.S3SessionToken = s3cfg.AccessKeyID, s3cfg.SecretAccessKey, s3cfg.SessionToken
		return resp, targetCredentialModeStatic, nil
	case targetCredentialModeSTS:
	default:
		return nil, "", fmt.Errorf("target credential_mode %q is not supported", mode)
	}

	prefix := strings.TrimSuffix(r.prefix, "/") + "/_runs/run-" + runID
	durationSeconds := defaultSTSDurationSeconds
	if v, ok := tgtMeta["sts_duration_seconds"].(float64); ok && v > 0 {
		durationSeconds = int(math.Min(math.Max(v, minSTSDurationSeconds), maxSTSDurationSeconds))
	}
	creds, err := s.assumeRoleFn(ctx, stsRequest{
		Endpoint:        stsEndpoint(tgtMeta),
		Region:          r.target.Region,
		RoleARN:         stringMapValue(tgtMeta, "sts_role_arn"),
		SessionName:     stsSessionName(attemptID),
		Policy:          s3PrefixSessionPolicy(stsPartition(stringMapValue(tgtMeta, "sts_role_arn")), r.target.Bucket, prefix),
		DurationSeconds: int32(durationSeconds),
		AccessKeyID:     s3cfg.AccessKeyID,
		SecretAccessKey: s3cfg.SecretAccessKey,
		SessionToken:    s3cfg.SessionToken,
	})
	if err != nil {
		return nil, "", fmt.Errorf("assume scoped S3 role: %w", err)
	}
	resp.S3AccessKeyId, resp.S3SecretAccessKey, resp.S3SessionToken = creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken
	if creds.CanExpire {
		resp.S3ExpiresAtUnixMs = creds.Expires.UnixMilli()
	}
	return resp, targetCredentialModeSTS, nil
}

// stsEndpoint selects the STS endpoint: explicit sts_endpoint, else the S3
// endpoint for S3-compatible stores that serve STS themselves (MinIO), else
// the AWS default.
func stsEndpoint(tgtMeta map[string]any) string {
	if v := stringMapValue(tgtMeta, "sts_endpoint"); v != "" {
		return v
	}
	if v := stringMapValue(tgtMeta, "endpoint"); v != "" && !strings.Contains(strings.ToLower(v), "amazonaws.com") {
		return v
	}
	return ""
}

// stsRequest is an AssumeRole call made with the target connection's stored
// credentials.
type stsRequest struct {
	Endpoint, Region, RoleARN, SessionName, Policy string
	DurationSeconds                                int32
	AccessKeyID, SecretAccessKey, SessionToken     string
}

func assumeRoleWithSTS(ctx context.Context, req stsRequest) (aws.Credentials, error) {
	if strings.TrimSpace(req.RoleARN) == "" {
		return aws.Credentials{}, fmt.Errorf("target metadata sts_role_arn is required for credential_mode sts")
	}
	cfg := aws.Config{
		Region:      req.Region,
		Credentials: credentials.NewStaticCredentialsProvider(req.AccessKeyID, req.SecretAccessKey, req.SessionToken),
	}
	if strings.TrimSpace(req.Endpoint) != "" {
		cfg.BaseEndpoint = aws.String(req.Endpoint)
	}
	out, err := sts.NewFromConfig(cfg).AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(req.RoleARN),
		RoleSessionName: aws.String(req.SessionName),
		Policy:          aws.String(req.Policy),
		DurationSeconds: aws.Int32(req.DurationSeconds),
	})
	if err != nil {
		return aws.Credentials{}, err
	}
	if out.Credentials == nil {
		return aws.Credentials{}, fmt.Errorf("STS returned no credentials")
	}
	c := aws.Credentials{
		AccessKeyID:     aws.ToString(out.Credentials.AccessKeyId),
		SecretAccessKey: aws.ToString(out.Credentials.SecretAccessKey),
		SessionToken:    aws.ToString(out.Credentials.SessionToken),
	}
	if out.Credentials.Expiration != nil {
		c.CanExpire, c.Expires = true, *out.Credentials.Expiration
	}
	return c, nil
}

// s3PrefixSessionPolicy limits temporary credentials to the object operations
// a worker performs, under the run's own staging prefix.
func s3PrefixSessionPolicy(partition, bucket, prefix string) string {
	objects := fmt.Sprintf("arn:%s:s3:::%s/%s/*", partition, bucket, prefix)
	policy := map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":   "Allow",
			"Action":   []string{"s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"},
			"Resource": []string{objects},
		}},
	}
	b, _ := json.Marshal(policy)
	return string(b)
}

// stsPartition returns the ARN partition of roleARN (aws, aws-cn,
// aws-us-gov, ...), defaulting to aws.
func stsPartition(roleARN string) string {
	parts := strings.SplitN(strings.TrimSpace(roleARN), ":", 3)
	if len(parts) >= 2 && parts[0] == "arn" && parts[1] != "" {
		return parts[1]
	}
	return "aws"
}

// stsSessionName identifies the attempt in the provider's audit trail.
// AssumeRole accepts at most 64 characters from [\w+=,.@-].
func stsSessionName(attemptID string) string {
	var b strings.Builder
	b.WriteString("orabbit-")
	for _, r := range attemptID {
		if b.Len() >= 64 {
			break
		}
		if r == '_' || r == '-' || r == '.' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func decryptSecretMap(k crypto.Key, conn db.Connection) (map[string]any, error) {
	plain, err := crypto.Decrypt(k, conn.SecretEncBlob, []byte(conn.ID))
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(plain, &out); err != nil {
		return nil, fmt.Errorf("secret is not a JSON object: %w", err)
	}
	return out, nil
}
