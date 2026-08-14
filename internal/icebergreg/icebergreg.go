package icebergreg

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"

	"gopkg.in/yaml.v3"
)

const JobOptionsKey = "iceberg_registration"

type IceYAML struct {
	URI         string `yaml:"uri"`
	BearerToken string `yaml:"bearerToken"`
	S3          IceS3  `yaml:"s3"`
}

type IceS3 struct {
	Endpoint        string `yaml:"endpoint"`
	Region          string `yaml:"region"`
	PathStyleAccess *bool  `yaml:"pathStyleAccess"`
	AccessKeyID     string `yaml:"accessKeyID"`
	SecretAccessKey string `yaml:"secretAccessKey"`
}

type JobConfig struct {
	Enabled bool   `json:"enabled"`
	Engine  string `json:"engine,omitempty"`
	Table   string `json:"table,omitempty"`
}

type RunConfig struct {
	Enabled     bool        `json:"enabled"`
	Engine      string      `json:"engine,omitempty"`
	Table       string      `json:"table,omitempty"`
	URI         string      `json:"uri,omitempty"`
	BearerToken string      `json:"bearer_token,omitempty"`
	ConfigYAML  string      `json:"config_yaml,omitempty"`
	S3          RunConfigS3 `json:"s3"`
}

type RunConfigS3 struct {
	Endpoint        string `json:"endpoint,omitempty"`
	Region          string `json:"region,omitempty"`
	PathStyleAccess bool   `json:"path_style_access"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
}

func ParseIceYAML(path string) (IceYAML, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return IceYAML{}, err
	}
	return ParseIceYAMLBytes(b)
}

func ParseIceYAMLBytes(b []byte) (IceYAML, error) {
	var cfg IceYAML
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return IceYAML{}, err
	}
	cfg.URI = strings.TrimSpace(cfg.URI)
	cfg.BearerToken = strings.TrimSpace(cfg.BearerToken)
	cfg.S3.Endpoint = strings.TrimSpace(cfg.S3.Endpoint)
	cfg.S3.Region = strings.TrimSpace(cfg.S3.Region)
	cfg.S3.AccessKeyID = strings.TrimSpace(cfg.S3.AccessKeyID)
	cfg.S3.SecretAccessKey = strings.TrimSpace(cfg.S3.SecretAccessKey)
	return cfg, nil
}

func (c JobConfig) Normalize() JobConfig {
	c.Engine = strings.ToLower(strings.TrimSpace(c.Engine))
	if c.Engine == "" {
		c.Engine = "rest-go"
	}
	c.Table = strings.TrimSpace(c.Table)
	return c
}

func ParseJobConfig(raw json.RawMessage) (JobConfig, error) {
	if len(raw) == 0 {
		return JobConfig{}, nil
	}

	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return JobConfig{}, fmt.Errorf("parse job options: %w", err)
	}

	nested, ok := root[JobOptionsKey]
	if !ok || len(nested) == 0 || string(nested) == "null" {
		return JobConfig{}, nil
	}

	var cfg JobConfig
	if err := json.Unmarshal(nested, &cfg); err != nil {
		return JobConfig{}, fmt.Errorf("parse %s: %w", JobOptionsKey, err)
	}
	return cfg.Normalize(), nil
}

func MergeJobConfig(options map[string]any, cfg JobConfig) map[string]any {
	if options == nil {
		options = map[string]any{}
	}

	cfg = cfg.Normalize()
	if !cfg.Enabled {
		delete(options, JobOptionsKey)
		return options
	}

	options[JobOptionsKey] = map[string]any{
		"enabled": cfg.Enabled,
		"engine":  cfg.Engine,
		"table":   cfg.Table,
	}
	return options
}

func (c RunConfig) Normalize() RunConfig {
	c.Engine = strings.ToLower(strings.TrimSpace(c.Engine))
	if c.Enabled && c.Engine == "" {
		c.Engine = "rest-go"
	}
	c.Table = strings.TrimSpace(c.Table)
	c.URI = strings.TrimSpace(c.URI)
	c.BearerToken = strings.TrimSpace(c.BearerToken)
	if strings.TrimSpace(c.ConfigYAML) == "" {
		c.ConfigYAML = ""
	}
	c.S3.Endpoint = strings.TrimSpace(c.S3.Endpoint)
	c.S3.Region = strings.TrimSpace(c.S3.Region)
	c.S3.AccessKeyID = strings.TrimSpace(c.S3.AccessKeyID)
	c.S3.SecretAccessKey = strings.TrimSpace(c.S3.SecretAccessKey)
	return c
}

func ParseRunConfig(raw json.RawMessage) (RunConfig, error) {
	if len(raw) == 0 {
		return RunConfig{}, nil
	}
	var cfg RunConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return RunConfig{}, fmt.Errorf("parse run registration config: %w", err)
	}
	return cfg.Normalize(), nil
}

func MarshalRunConfig(cfg RunConfig) (json.RawMessage, error) {
	cfg = cfg.Normalize()
	if !cfg.Enabled {
		return nil, nil
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

func ResolveRegistrationS3Config(base s3io.Config, iceCfg IceYAML) s3io.Config {
	out := base
	if v := strings.TrimSpace(iceCfg.S3.Endpoint); v != "" {
		out.Endpoint = v
	}
	if v := strings.TrimSpace(iceCfg.S3.Region); v != "" {
		out.Region = v
	}
	if iceCfg.S3.PathStyleAccess != nil {
		out.ForcePathStyle = *iceCfg.S3.PathStyleAccess
	}
	if v := strings.TrimSpace(iceCfg.S3.AccessKeyID); v != "" {
		out.AccessKeyID = v
	}
	if v := strings.TrimSpace(iceCfg.S3.SecretAccessKey); v != "" {
		out.SecretAccessKey = v
	}
	return out
}

func ResolveRunConfig(enabled bool, engine, table string, baseS3 s3io.Config, iceCfg IceYAML) RunConfig {
	cfg := RunConfig{
		Enabled:     enabled,
		Engine:      strings.ToLower(strings.TrimSpace(engine)),
		Table:       strings.TrimSpace(table),
		URI:         strings.TrimSpace(iceCfg.URI),
		BearerToken: strings.TrimSpace(iceCfg.BearerToken),
	}
	regS3 := ResolveRegistrationS3Config(baseS3, iceCfg)
	cfg.S3 = RunConfigS3{
		Endpoint:        strings.TrimSpace(regS3.Endpoint),
		Region:          strings.TrimSpace(regS3.Region),
		PathStyleAccess: regS3.ForcePathStyle,
		AccessKeyID:     strings.TrimSpace(regS3.AccessKeyID),
		SecretAccessKey: strings.TrimSpace(regS3.SecretAccessKey),
	}
	return cfg.Normalize()
}

func DefaultTable(sourceEngine, sourceTable string) string {
	if table, ok := dataset.IcebergTableFromStoragePrefix(sourceTable); ok {
		return table
	}

	name := dataset.TableName(sourceTable)
	if strings.TrimSpace(name) == "" {
		name = "table"
	}
	engine := connectors.NormalizeSourceEngine(sourceEngine)
	if engine == "" {
		engine = "mssql"
	}
	return engine + "." + name
}
