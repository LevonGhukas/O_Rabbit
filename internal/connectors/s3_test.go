package connectors

import (
	"context"
	"os"
	"testing"
)

func TestOpenS3_InvalidDSN(t *testing.T) {
	ctx := context.Background()

	// Should fail on invalid prefix
	_, err := OpenS3(ctx, "postgres://user:pass@host/db")
	if err == nil {
		t.Fatal("expected error for non-s3 dsn")
	}

	// Should fail on invalid structure
	_, err = OpenS3(ctx, "s3://bucket_without_key")
	if err == nil {
		t.Fatal("expected error for missing key in s3 dsn")
	}

	// Should fail if no credentials in env
	os.Unsetenv("ORABBIT_DEFAULT_S3_ACCESS_KEY_ID")
	os.Unsetenv("ORABBIT_DEFAULT_S3_SECRET_ACCESS_KEY")
	_, err = OpenS3(ctx, "s3://bucket/key.csv")
	if err == nil {
		t.Fatal("expected error for missing credentials")
	}

	// Set mock credentials
	os.Setenv("ORABBIT_DEFAULT_S3_ACCESS_KEY_ID", "mock")
	os.Setenv("ORABBIT_DEFAULT_S3_SECRET_ACCESS_KEY", "mock")

	reader, err := OpenS3(ctx, "s3://bucket/key.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s3Reader, ok := reader.(*S3Reader)
	if !ok {
		t.Fatal("expected S3Reader")
	}
	if s3Reader.bucket != "bucket" {
		t.Errorf("expected bucket 'bucket', got %q", s3Reader.bucket)
	}
	if s3Reader.key != "key.csv" {
		t.Errorf("expected key 'key.csv', got %q", s3Reader.key)
	}
	if s3Reader.format != "csv" {
		t.Errorf("expected format 'csv', got %q", s3Reader.format)
	}
}

func TestOpenS3_JSONFormat(t *testing.T) {
	ctx := context.Background()
	os.Setenv("ORABBIT_DEFAULT_S3_ACCESS_KEY_ID", "mock")
	os.Setenv("ORABBIT_DEFAULT_S3_SECRET_ACCESS_KEY", "mock")

	reader, err := OpenS3(ctx, "s3://my-bucket/path/to/file.json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s3Reader, ok := reader.(*S3Reader)
	if !ok {
		t.Fatal("expected S3Reader")
	}
	if s3Reader.format != "json" {
		t.Errorf("expected format 'json', got %q", s3Reader.format)
	}
}
