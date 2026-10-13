package main

import (
	"context"
	"testing"
	"time"
)

func TestNormalizePartitionSpecMapsLegacyIntRange(t *testing.T) {
	got := normalizePartitionSpec(partitionSpec{Type: "sql_int_range", IDColumn: " id ", From: 10, To: 20})

	if got.SourceMode != "table" || got.CursorColumn != "id" || got.CursorDomain != "int64" {
		t.Fatalf("spec=%+v, want table mode, id cursor, int64 domain", got)
	}
	if got.Lower != "10" || !got.LowerExclusive || got.Upper != "20" || !got.UpperInclusive {
		t.Fatalf("bounds=%+v, want (10, 20]", got)
	}
}

func TestNormalizePartitionSpecKeepsExplicitValues(t *testing.T) {
	in := partitionSpec{Type: "sql_cursor_range", SourceMode: " QUERY ", CursorColumn: "ts", IDColumn: "id", CursorDomain: "timestamp", Lower: "a", Upper: "b"}

	got := normalizePartitionSpec(in)

	if got.SourceMode != "query" || got.CursorColumn != "ts" || got.CursorDomain != "timestamp" || got.Lower != "a" || got.Upper != "b" {
		t.Fatalf("spec=%+v, explicit values must win over legacy aliases", got)
	}
}

func TestSourceQueryTimeout(t *testing.T) {
	saved := sourceQueryTimeout
	t.Cleanup(func() { sourceQueryTimeout = saved })

	sourceQueryTimeout = time.Minute
	ctx, cancel := withSourceQueryTimeout(context.Background())
	deadline, ok := ctx.Deadline()
	cancel()
	if !ok || time.Until(deadline) > time.Minute {
		t.Fatalf("deadline=%v ok=%v, want within a minute", deadline, ok)
	}

	sourceQueryTimeout = 0
	ctx, cancel = withSourceQueryTimeout(context.Background())
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("timeout 0 must mean no deadline")
	}
}

func TestTaskCredentialsLiveOnlyInTheirContext(t *testing.T) {
	ctx := withTaskCredentials(context.Background(), taskCredentials{SourceDSN: "postgres://x"})

	if got := taskCredentialsFromContext(ctx).SourceDSN; got != "postgres://x" {
		t.Fatalf("SourceDSN=%q", got)
	}
	if got := taskCredentialsFromContext(context.Background()); got.SourceDSN != "" || got.S3 != nil {
		t.Fatalf("credentials leaked into an unrelated context: %+v", got)
	}
}
