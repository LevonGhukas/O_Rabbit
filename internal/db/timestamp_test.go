package db

import (
	"context"
	"testing"
	"time"
)

func TestFormatTimestampSortsChronologically(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a, b := FormatTimestamp(base.Add(100*time.Millisecond)), FormatTimestamp(base.Add(120*time.Millisecond))
	if len(a) != len(TimestampLayout) || !(a < b) {
		t.Fatalf("a=%s b=%s", a, b)
	}
	// RFC3339Nano gets this wrong: ".1Z" sorts after ".12Z".
	if base.Add(100*time.Millisecond).Format(time.RFC3339Nano) < base.Add(120*time.Millisecond).Format(time.RFC3339Nano) {
		t.Fatal("expected RFC3339Nano to mis-order; test premise changed")
	}
}

func TestNormalizeStoredTimestampsRewritesLegacyValues(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if _, err := st.db.ExecContext(ctx, `INSERT INTO events(id,run_id,ts,level,message,fields_json) VALUES('e1','r','2026-01-01T00:00:00.1+02:00','INFO','m','{}'),('e2','r','not-a-time','INFO','m','{}')`); err != nil {
		t.Fatal(err)
	}
	if err := normalizeStoredTimestamps(ctx, st.db); err != nil {
		t.Fatal(err)
	}
	var ts1, ts2 string
	_ = st.db.QueryRowContext(ctx, `SELECT ts FROM events WHERE id='e1'`).Scan(&ts1)
	_ = st.db.QueryRowContext(ctx, `SELECT ts FROM events WHERE id='e2'`).Scan(&ts2)
	if ts1 != "2025-12-31T22:00:00.100000000Z" || ts2 != "not-a-time" {
		t.Fatalf("ts1=%s ts2=%s", ts1, ts2)
	}
}
