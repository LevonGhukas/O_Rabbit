package dataset

import (
	"encoding/json"
	"fmt"
	"strings"
)

// DefaultRegion is used when target metadata sets no region. It is the S3
// default region and the region S3-compatible stores such as MinIO expect.
const DefaultRegion = "us-east-1"

// Target is an S3 destination read from target connection metadata.
type Target struct {
	// Endpoint is the S3 API endpoint, for example
	// https://s3.eu-west-1.amazonaws.com or http://minio:9000.
	Endpoint       string
	Region         string
	Bucket         string
	Prefix         string
	ForcePathStyle bool
	// Metadata is the full decoded metadata, for options such as STS
	// settings and the dataset prefix derivation.
	Metadata map[string]any
}

// ParseTarget decodes and validates target connection metadata. Invalid
// JSON, a wrongly typed field, or a missing endpoint or bucket is an error
// rather than a silently different destination. The endpoint is required
// because it is part of a committed dataset's durable identity.
func ParseTarget(metadata json.RawMessage) (Target, error) {
	var meta map[string]any
	if err := json.Unmarshal(metadata, &meta); err != nil {
		return Target{}, fmt.Errorf("target connection metadata is not a JSON object: %w", err)
	}
	if meta == nil {
		meta = map[string]any{}
	}
	t := Target{Region: DefaultRegion, ForcePathStyle: true, Metadata: meta}
	for key, dst := range map[string]*string{"endpoint": &t.Endpoint, "region": &t.Region, "bucket": &t.Bucket, "prefix": &t.Prefix} {
		v, ok := meta[key]
		if !ok || v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			return Target{}, fmt.Errorf("target connection metadata %q must be a string", key)
		}
		if s = strings.TrimSpace(s); s != "" {
			*dst = s
		}
	}
	if v, ok := meta["force_path_style"]; ok && v != nil {
		b, ok := v.(bool)
		if !ok {
			return Target{}, fmt.Errorf("target connection metadata %q must be a boolean", "force_path_style")
		}
		t.ForcePathStyle = b
	}
	if t.Endpoint == "" {
		return Target{}, fmt.Errorf("target connection metadata is missing endpoint (for AWS use https://s3.<region>.amazonaws.com)")
	}
	if t.Bucket == "" {
		return Target{}, fmt.Errorf("target connection metadata is missing bucket")
	}
	return t, nil
}
