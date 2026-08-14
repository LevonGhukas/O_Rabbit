package orabbitcli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/apache/iceberg-go/io/gocloud"

	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

func defaultIceTable(cfg ranConfig) string {
	return icebergreg.DefaultTable(normalizeSourceEngine(cfg.SourceEngine), cfg.Table)
}

func effectiveIceTable(cfg ranConfig) string {
	if v := strings.TrimSpace(cfg.IceTable); v != "" {
		return v
	}
	return defaultIceTable(cfg)
}

func registrationEngine(cfg ranConfig) string {
	e := strings.ToLower(strings.TrimSpace(cfg.IcebergEngine))
	if e == "" {
		return "rest-go"
	}
	return e
}

// resolveIcebergRegistrationS3Config selects client-side S3 connectivity for the
// post-run Iceberg registration phase.
//
// The run/export config remains the source of truth for the logical dataset
// location (bucket/prefix and the daemon-facing endpoint used by workers during
// export). The `.ice.yaml` `s3:` block overrides only the host-side connectivity
// and credentials needed by `orabbit-client` to read the exported objects and
// talk to the REST catalog's backing store after the run completes.
//
// Field precedence is:
// 1. `.ice.yaml` `s3:` value when present
// 2. run/export config fallback when the `.ice.yaml` field is omitted
func resolveIcebergRegistrationS3Config(cfg ranConfig, iceCfg icebergreg.IceYAML) s3io.Config {
	return icebergreg.ResolveRegistrationS3Config(s3io.Config{
		Endpoint:        cfg.S3Endpoint,
		Region:          cfg.S3Region,
		Bucket:          cfg.S3Bucket,
		ForcePathStyle:  cfg.S3ForcePathStyle,
		AccessKeyID:     cfg.S3AccessKeyID,
		SecretAccessKey: cfg.S3SecretAccessKey,
	}, iceCfg)
}

func buildIcebergRegistrationSnapshot(cfg ranConfig) (json.RawMessage, error) {
	if !cfg.AutoIceberg {
		return nil, nil
	}

	engine := registrationEngine(cfg)
	table := effectiveIceTable(cfg)
	if strings.TrimSpace(cfg.IceConfig) == "" {
		return nil, fmt.Errorf("missing iceberg config file")
	}

	absIceCfg, err := filepath.Abs(cfg.IceConfig)
	if err != nil {
		absIceCfg = cfg.IceConfig
	}
	rawCfg, err := os.ReadFile(absIceCfg)
	if err != nil {
		return nil, fmt.Errorf("read iceberg config %q: %w", cfg.IceConfig, err)
	}
	iceCfg, err := icebergreg.ParseIceYAMLBytes(rawCfg)
	if err != nil {
		return nil, fmt.Errorf("read iceberg config %q: %w", cfg.IceConfig, err)
	}

	runCfg := icebergreg.ResolveRunConfig(
		true,
		engine,
		table,
		s3io.Config{
			Endpoint:        cfg.S3Endpoint,
			Region:          cfg.S3Region,
			Bucket:          cfg.S3Bucket,
			ForcePathStyle:  cfg.S3ForcePathStyle,
			AccessKeyID:     cfg.S3AccessKeyID,
			SecretAccessKey: cfg.S3SecretAccessKey,
		},
		iceCfg,
	)
	if engine == "ice" {
		runCfg.ConfigYAML = string(rawCfg)
	}

	raw, err := icebergreg.MarshalRunConfig(runCfg)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func restGoTableLocation(bucket, basePrefix string) string {
	return fmt.Sprintf("s3://%s/%s", strings.TrimSpace(bucket), strings.Trim(strings.TrimSpace(basePrefix), "/"))
}

func isBrokenRESTMetadataErr(err error, tableLoc string) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if !strings.Contains(msg, "location does not exist:") || !strings.Contains(msg, "/metadata/") {
		return false
	}
	tableLoc = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(tableLoc), "/"))
	if tableLoc == "" {
		return false
	}
	return strings.Contains(msg, tableLoc+"/metadata/")
}
