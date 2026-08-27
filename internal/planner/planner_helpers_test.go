package planner

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
)

func TestPartitionSpecSQLCursorSinglePreservesTableProjectionMetadata(t *testing.T) {
	part := partitionSpecSQLCursorSingle(
		"default.users", "table", "", "active = 1",
		[]string{"id", "name"}, map[string]string{"id": "UInt64"},
		"id", "int64", "", false, "",
	)
	var got map[string]any
	if err := json.Unmarshal(part, &got); err != nil {
		t.Fatal(err)
	}
	if columns, ok := got["select_columns"].([]any); !ok || len(columns) != 2 {
		t.Fatalf("select_columns=%#v", got["select_columns"])
	}
	if types, ok := got["column_types"].(map[string]any); !ok || types["id"] != "UInt64" {
		t.Fatalf("column_types=%#v", got["column_types"])
	}
}

func TestNewID(t *testing.T) {
	a := newID()
	b := newID()

	if a == "" {
		t.Fatal("newID returned empty string")
	}

	if len(a) != 32 {
		t.Fatalf("newID length = %d, want 32", len(a))
	}

	if a == b {
		t.Fatal("newID returned duplicate values")
	}

	for _, c := range a {
		if !strings.ContainsRune("0123456789abcdef", c) {
			t.Fatalf("newID contains non-hex character: %q", c)
		}
	}
}

func TestPartitionSpecSingleWithRecordPath(t *testing.T) {
	part := string(PartitionSpecSingleWithRecordPath("file.json", "table", "", "/data/items"))
	if !strings.Contains(part, `"record_path":"/data/items"`) {
		t.Fatalf("partition spec=%s", part)
	}
}

func TestPartitionSpecSingleWithFileOptions(t *testing.T) {
	part := string(PartitionSpecSingleWithFileOptions("file.txt", "table", "", "/airports", "json"))
	for _, expected := range []string{`"record_path":"/airports"`, `"format":"json"`} {
		if !strings.Contains(part, expected) {
			t.Fatalf("partition spec=%s missing %s", part, expected)
		}
	}
}

func TestIsLocalEndpoint(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{
			name: "empty",
			in:   "",
			want: false,
		},
		{
			name: "localhost with port",
			in:   "localhost:9000",
			want: true,
		},
		{
			name: "localhost url",
			in:   "http://localhost:9000",
			want: true,
		},
		{
			name: "ipv4 loopback",
			in:   "127.0.0.1:9000",
			want: true,
		},
		{
			name: "ipv6 loopback",
			in:   "http://[::1]:9000",
			want: true,
		},
		{
			name: "remote host",
			in:   "https://example.com",
			want: false,
		},
		{
			name: "invalid",
			in:   "://bad",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isLocalEndpoint(tt.in); got != tt.want {
				t.Fatalf("isLocalEndpoint(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestSourceDatasetName(t *testing.T) {
	tests := []struct {
		name string
		job  db.Job
		opts jobopts.Options
		want string
	}{
		{
			name: "explicit source name wins",
			job: db.Job{
				TargetTable: "target_table",
			},
			opts: jobopts.Options{
				SourceName: "custom_source",
			},
			want: "custom_source",
		},
		{
			name: "query uses hash",
			opts: jobopts.Options{
				SourceMode: "query",
				QueryHash:  "abc123",
			},
			want: "query_abc123",
		},
		{
			name: "query without hash",
			opts: jobopts.Options{
				SourceMode: "query",
			},
			want: "query",
		},
		{
			name: "table uses table name",
			opts: jobopts.Options{
				Table: "users",
			},
			want: "users",
		},
		{
			name: "fallback target table",
			job: db.Job{
				TargetTable: "orders",
			},
			want: "orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sourceDatasetName(tt.job, tt.opts)
			if got != tt.want {
				t.Fatalf("sourceDatasetName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSourceQueryForJob(t *testing.T) {
	job := db.Job{
		SourceSQL: "select * from jobs",
	}

	t.Run("options query wins", func(t *testing.T) {
		opts := jobopts.Options{
			Query: "select * from users",
		}

		got := sourceQueryForJob(job, opts)

		if got != opts.Query {
			t.Fatalf("got %q want %q", got, opts.Query)
		}
	})

	t.Run("falls back to job SQL", func(t *testing.T) {
		got := sourceQueryForJob(job, jobopts.Options{})

		if got != job.SourceSQL {
			t.Fatalf("got %q want %q", got, job.SourceSQL)
		}
	})
}

func TestValidateDatasetSourceState(t *testing.T) {
	t.Run("missing state is valid", func(t *testing.T) {
		err := validateDatasetSourceState(
			datasetState{},
			false,
			jobopts.Options{},
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("source mode mismatch", func(t *testing.T) {
		err := validateDatasetSourceState(
			datasetState{
				SourceMode: "table",
			},
			true,
			jobopts.Options{
				SourceMode: "query",
			},
		)

		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("query hash mismatch", func(t *testing.T) {
		err := validateDatasetSourceState(
			datasetState{
				SourceMode: "query",
				QueryHash:  "old",
			},
			true,
			jobopts.Options{
				SourceMode: "query",
				QueryHash:  "new",
			},
		)

		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("matching query hash is valid", func(t *testing.T) {
		err := validateDatasetSourceState(
			datasetState{
				SourceMode: "query",
				QueryHash:  "same",
			},
			true,
			jobopts.Options{
				SourceMode: "query",
				QueryHash:  "same",
			},
		)

		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
