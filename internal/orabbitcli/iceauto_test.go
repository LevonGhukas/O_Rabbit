package orabbitcli

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"

	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
)

func TestRestGoTableLocation(t *testing.T) {
	got := restGoTableLocation("bucket1", "/postgres/public__demo_orders/")
	want := "s3://bucket1/postgres/public__demo_orders"
	if got != want {
		t.Fatalf("restGoTableLocation()=%q want %q", got, want)
	}
}

func TestIsBrokenRESTMetadataErr(t *testing.T) {
	tableLoc := "s3://bucket1/postgres/public__demo_orders"
	// err := errors.New("NotFoundException: Location does not exist: s3://bucket1/postgres/public__demo_orders/metadata/00001-abc.metadata.json")
	err := errors.New("NotFoundException: Location does not exist: s3://bucket1/postgres/public__demo_orders/metadata/00001-abc.json")
	if !isBrokenRESTMetadataErr(err, tableLoc) {
		t.Fatalf("expected broken metadata error to match")
	}
}

func TestIsBrokenRESTMetadataErrRejectsOtherLocations(t *testing.T) {
	tableLoc := "s3://bucket1/postgres/public__demo_orders"
	// err := errors.New("NotFoundException: Location does not exist: s3://bucket1/other/table/metadata/00001-abc.metadata.json")
	err := errors.New("NotFoundException: Location does not exist: s3://bucket1/other/table/metadata/00001-abc.json")
	if isBrokenRESTMetadataErr(err, tableLoc) {
		t.Fatalf("unexpected match for different table location")
	}
}

func TestIcebergS3IOSchemeRegisteredForRestGo(t *testing.T) {
	props := iceberg.Properties{
		"s3.endpoint":                 "http://127.0.0.1:9000",
		"s3.region":                   "us-east-1",
		"s3.access-key-id":            "minioadmin",
		"s3.secret-access-key":        "minioadmin",
		"s3.force-virtual-addressing": "false",
	}

	// _, err := icebergio.LoadFS(context.Background(), props, "s3://bucket1/postgres/public__demo_orders/metadata/00000-test.metadata.json")
	_, err := icebergio.LoadFS(context.Background(), props, "s3://bucket1/postgres/public__demo_orders/metadata/00000-test.json")
	if errors.Is(err, icebergio.ErrIOSchemeNotFound) {
		t.Fatalf("s3 IO scheme is not registered: %v", err)
	}
}

func TestResolveIcebergRegistrationS3ConfigPrefersIceConfig(t *testing.T) {
	pathStyle := false
	got := resolveIcebergRegistrationS3Config(ranConfig{
		S3Endpoint:        "http://minio:9000",
		S3Region:          "docker-region",
		S3Bucket:          "bucket1",
		S3ForcePathStyle:  true,
		S3AccessKeyID:     "docker-user",
		S3SecretAccessKey: "docker-secret",
	}, icebergreg.IceYAML{
		S3: icebergreg.IceS3{
			Endpoint:        "http://127.0.0.1:9000",
			Region:          "host-region",
			PathStyleAccess: &pathStyle,
			AccessKeyID:     "host-user",
			SecretAccessKey: "host-secret",
		},
	})

	if got.Endpoint != "http://127.0.0.1:9000" {
		t.Fatalf("Endpoint = %q want %q", got.Endpoint, "http://127.0.0.1:9000")
	}
	if got.Region != "host-region" {
		t.Fatalf("Region = %q want %q", got.Region, "host-region")
	}
	if got.Bucket != "bucket1" {
		t.Fatalf("Bucket = %q want %q", got.Bucket, "bucket1")
	}
	if got.ForcePathStyle {
		t.Fatalf("ForcePathStyle = %v want false", got.ForcePathStyle)
	}
	if got.AccessKeyID != "host-user" {
		t.Fatalf("AccessKeyID = %q want %q", got.AccessKeyID, "host-user")
	}
	if got.SecretAccessKey != "host-secret" {
		t.Fatalf("SecretAccessKey = %q want %q", got.SecretAccessKey, "host-secret")
	}
}

func TestResolveIcebergRegistrationS3ConfigFallsBackWhenIceConfigIsPartial(t *testing.T) {
	got := resolveIcebergRegistrationS3Config(ranConfig{
		S3Endpoint:        "http://minio:9000",
		S3Region:          "us-east-1",
		S3Bucket:          "bucket1",
		S3ForcePathStyle:  true,
		S3AccessKeyID:     "minioadmin",
		S3SecretAccessKey: "minioadmin",
	}, icebergreg.IceYAML{
		S3: icebergreg.IceS3{
			Endpoint: "http://127.0.0.1:9000",
		},
	})

	if got.Endpoint != "http://127.0.0.1:9000" {
		t.Fatalf("Endpoint = %q want %q", got.Endpoint, "http://127.0.0.1:9000")
	}
	if got.Region != "us-east-1" {
		t.Fatalf("Region = %q want %q", got.Region, "us-east-1")
	}
	if !got.ForcePathStyle {
		t.Fatalf("ForcePathStyle = %v want true", got.ForcePathStyle)
	}
	if got.AccessKeyID != "minioadmin" {
		t.Fatalf("AccessKeyID = %q want %q", got.AccessKeyID, "minioadmin")
	}
	if got.SecretAccessKey != "minioadmin" {
		t.Fatalf("SecretAccessKey = %q want %q", got.SecretAccessKey, "minioadmin")
	}
}

func TestBuildIcebergRegistrationSnapshotUsesResolvedIceConfig(t *testing.T) {
	icePath := filepath.Join(t.TempDir(), "ice.yaml")
	if err := os.WriteFile(icePath, []byte(`
uri: http://catalog:8181
bearerToken: test-token
s3:
  endpoint: http://127.0.0.1:9000
  region: us-east-1
  pathStyleAccess: true
  accessKeyID: host-user
  secretAccessKey: host-secret
`), 0o600); err != nil {
		t.Fatalf("write ice config: %v", err)
	}

	raw, err := buildIcebergRegistrationSnapshot(ranConfig{
		AutoIceberg:       true,
		IcebergEngine:     "rest-go",
		IceConfig:         icePath,
		SourceEngine:      "mssql",
		Table:             "SalesDB.dbo.Orders",
		S3Endpoint:        "http://minio:9000",
		S3Region:          "docker-region",
		S3Bucket:          "bucket1",
		S3ForcePathStyle:  false,
		S3AccessKeyID:     "docker-user",
		S3SecretAccessKey: "docker-secret",
	})
	if err != nil {
		t.Fatalf("buildIcebergRegistrationSnapshot: %v", err)
	}

	var cfg icebergreg.RunConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if !cfg.Enabled {
		t.Fatalf("Enabled=%v want true", cfg.Enabled)
	}
	if cfg.URI != "http://catalog:8181" {
		t.Fatalf("URI=%q want %q", cfg.URI, "http://catalog:8181")
	}
	if cfg.BearerToken != "test-token" {
		t.Fatalf("BearerToken=%q want %q", cfg.BearerToken, "test-token")
	}
	if cfg.S3.Endpoint != "http://127.0.0.1:9000" {
		t.Fatalf("S3.Endpoint=%q want %q", cfg.S3.Endpoint, "http://127.0.0.1:9000")
	}
	if cfg.S3.AccessKeyID != "host-user" {
		t.Fatalf("S3.AccessKeyID=%q want %q", cfg.S3.AccessKeyID, "host-user")
	}
	if !cfg.S3.PathStyleAccess {
		t.Fatalf("S3.PathStyleAccess=%v want true", cfg.S3.PathStyleAccess)
	}
}

func TestBuildIcebergRegistrationSnapshotPersistsRawConfigForIceEngine(t *testing.T) {
	iceText := strings.TrimSpace(`
uri: http://catalog:8181
bearerToken: test-token
httpCacheDir: data/ice/http/cache
s3:
  endpoint: http://127.0.0.1:9000
  region: us-east-1
  pathStyleAccess: true
`) + "\n"
	icePath := filepath.Join(t.TempDir(), "ice.yaml")
	if err := os.WriteFile(icePath, []byte(iceText), 0o600); err != nil {
		t.Fatalf("write ice config: %v", err)
	}

	raw, err := buildIcebergRegistrationSnapshot(ranConfig{
		AutoIceberg:       true,
		IcebergEngine:     "ice",
		IceConfig:         icePath,
		SourceEngine:      "mssql",
		Table:             "SalesDB.dbo.Orders",
		S3Endpoint:        "http://minio:9000",
		S3Region:          "us-east-1",
		S3Bucket:          "bucket1",
		S3ForcePathStyle:  true,
		S3AccessKeyID:     "minioadmin",
		S3SecretAccessKey: "minioadmin",
	})
	if err != nil {
		t.Fatalf("buildIcebergRegistrationSnapshot: %v", err)
	}

	var cfg icebergreg.RunConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if cfg.Engine != "ice" {
		t.Fatalf("Engine=%q want ice", cfg.Engine)
	}
	if cfg.ConfigYAML != iceText {
		t.Fatalf("ConfigYAML=%q want %q", cfg.ConfigYAML, iceText)
	}
}
