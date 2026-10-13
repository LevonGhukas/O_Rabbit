package grpcapi

import (
	"context"
	"fmt"

	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// resolvedRun is a run's planned configuration with its options and target
// parsed. Every step of a run (assignment, credentials, commit, cleanup)
// resolves it the same way, from the run's configuration snapshot.
type resolvedRun struct {
	config db.RunConfig
	opts   jobopts.Options
	target dataset.Target
	prefix string // dataset prefix in the target bucket
}

func (s *Server) resolveRun(ctx context.Context, run db.Run) (resolvedRun, error) {
	cfg, err := s.st.RunConfig(ctx, run)
	if err != nil {
		return resolvedRun{}, err
	}
	opts, err := jobopts.Parse(cfg.Job.OptionsJSON)
	if err != nil {
		return resolvedRun{}, fmt.Errorf("run %s job options are invalid: %w", run.ID, err)
	}
	target, err := dataset.ParseTarget(cfg.TargetMetadata)
	if err != nil {
		return resolvedRun{}, fmt.Errorf("run %s: %w", run.ID, err)
	}
	return resolvedRun{
		config: cfg,
		opts:   opts,
		target: target,
		prefix: datasetPrefixForJob(cfg.Job, cfg.SourceEngine, opts, target.Metadata),
	}, nil
}

// targetS3Config returns the S3 client configuration for the run's target,
// with credentials from the (current) target connection.
func (s *Server) targetS3Config(ctx context.Context, r resolvedRun) (s3io.Config, error) {
	conn, err := s.st.GetConnection(ctx, r.config.Job.TargetConnectionID)
	if err != nil {
		return s3io.Config{}, err
	}
	secret, err := decryptSecretMap(s.k, conn)
	if err != nil {
		return s3io.Config{}, fmt.Errorf("target connection secret: %w", err)
	}
	return s3io.Config{
		Endpoint:        r.target.Endpoint,
		Region:          r.target.Region,
		Bucket:          r.target.Bucket,
		ForcePathStyle:  r.target.ForcePathStyle,
		AccessKeyID:     stringMapValue(secret, "access_key_id"),
		SecretAccessKey: stringMapValue(secret, "secret_access_key"),
		SessionToken:    stringMapValue(secret, "session_token"),
	}, nil
}
