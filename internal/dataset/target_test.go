package dataset

import (
	"strings"
	"testing"
)

func TestParseTargetDefaultsAndValidation(t *testing.T) {
	got, err := ParseTarget([]byte(`{"endpoint":" http://minio:9000 ","bucket":"b","prefix":"exports"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got.Endpoint != "http://minio:9000" || got.Bucket != "b" || got.Prefix != "exports" || got.Region != DefaultRegion || !got.ForcePathStyle {
		t.Fatalf("target=%+v", got)
	}
	got, err = ParseTarget([]byte(`{"endpoint":"https://s3.eu-west-1.amazonaws.com","bucket":"b","region":"eu-west-1","force_path_style":false}`))
	if err != nil || got.Region != "eu-west-1" || got.ForcePathStyle {
		t.Fatalf("explicit values target=%+v err=%v", got, err)
	}

	for body, want := range map[string]string{
		`not json`:                         "not a JSON object",
		`{"endpoint":"http://minio:9000"}`: "missing bucket",
		`{"bucket":"b"}`:                   "missing endpoint",
		`{"endpoint":7,"bucket":"b"}`:      `"endpoint" must be a string`,
		`{"endpoint":"http://m","bucket":"b","force_path_style":"yes"}`: `"force_path_style" must be a boolean`,
	} {
		if _, err := ParseTarget([]byte(body)); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("metadata %s: err=%v, want %q", body, err, want)
		}
	}
}
