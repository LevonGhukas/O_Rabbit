package db

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

func TestFindByNameReturnsNewestAndUsesIndex(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if err := st.CreateConnection(ctx, Connection{ID: "c1", Name: "src", Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: []byte("x")}); err != nil {
		t.Fatalf("create connection: %v", err)
	}
	if got, err := st.FindConnectionByName(ctx, "src"); err != nil || got.ID != "c1" {
		t.Fatalf("FindConnectionByName = %q, %v; want c1", got.ID, err)
	}
	for _, id := range []string{"j-old", "j-new"} {
		if err := st.CreateJob(ctx, Job{ID: id, Name: "orders", SourceConnectionID: "c1", TargetConnectionID: "c1", WriteMode: "append", OptionsJSON: []byte(`{}`)}); err != nil {
			t.Fatalf("create job %s: %v", id, err)
		}
	}
	if got, err := st.FindJobByName(ctx, "orders"); err != nil || got.ID != "j-new" {
		t.Fatalf("FindJobByName = %q, %v; want newest j-new", got.ID, err)
	}
	if _, err := st.FindJobByName(ctx, "missing"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("missing job err=%v, want sql.ErrNoRows", err)
	}

	for table, index := range map[string]string{"connections": "idx_connections_name", "jobs": "idx_jobs_name_created"} {
		var plan strings.Builder
		rows, err := st.db.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT id FROM `+table+` WHERE name=? ORDER BY created_at DESC, id DESC LIMIT 1`, "x")
		if err != nil {
			t.Fatalf("explain %s: %v", table, err)
		}
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatalf("scan plan: %v", err)
			}
			plan.WriteString(detail + "\n")
		}
		_ = rows.Close()
		if !strings.Contains(plan.String(), index) {
			t.Fatalf("%s lookup does not use %s:\n%s", table, index, plan.String())
		}
	}
}
