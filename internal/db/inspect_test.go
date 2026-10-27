package db

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestInspectDatabaseReportsARestorableCopy(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "master.sqlite")
	st, err := Open(ctx, Config{Path: path}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	key := rotationKey(t, 1)
	seedSealedValues(t, st, key)
	if _, err := st.AcquireLeadership(ctx, "master-test", time.Minute, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	r, err := InspectDatabase(ctx, path, key, now)
	if err != nil {
		t.Fatal(err)
	}
	if problems := r.Problems(time.Minute); len(problems) != 0 {
		t.Fatalf("problems = %v, report %+v", problems, r)
	}
	if r.SchemaVersion != r.SchemaVersionKnown || r.SchemaVersion == 0 {
		t.Fatalf("schema %d, known %d", r.SchemaVersion, r.SchemaVersionKnown)
	}
	if !r.KeyChecked || r.SealedValues != 3 || r.SealedValuesUnreadable != 0 {
		t.Fatalf("key check = %+v", r)
	}
	if r.LeadershipEpoch != 1 || r.AgeSeconds == nil || *r.AgeSeconds > 60 {
		t.Fatalf("leadership = epoch %d age %v", r.LeadershipEpoch, r.AgeSeconds)
	}
	// The copied lease of a master that never released it is still held.
	if r.LeaseRemainingSeconds <= 0 || r.LeaseRemainingSeconds > 60 {
		t.Fatalf("lease remaining = %v, want within the 1m lease", r.LeaseRemainingSeconds)
	}
	if r.RunsByStatus["SUCCEEDED"] != 1 {
		t.Fatalf("runs = %v", r.RunsByStatus)
	}

	// The inspection must not migrate or otherwise write the copy.
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("InspectDatabase changed the database file")
	}

	t.Run("wrong master key", func(t *testing.T) {
		r, err := InspectDatabase(ctx, path, rotationKey(t, 2), now)
		if err != nil {
			t.Fatal(err)
		}
		if r.SealedValuesUnreadable != 3 || !hasProblem(r.Problems(0), "do not decrypt") {
			t.Fatalf("report %+v problems %v", r, r.Problems(0))
		}
	})
	t.Run("stale copy", func(t *testing.T) {
		r, err := InspectDatabase(ctx, path, key, now.Add(5*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if !hasProblem(r.Problems(time.Minute), "old, more than") {
			t.Fatalf("problems %v, want a staleness problem", r.Problems(time.Minute))
		}
		if len(r.Problems(0)) != 0 {
			t.Fatalf("maxAge 0 must skip the staleness check: %v", r.Problems(0))
		}
		if r.LeaseRemainingSeconds != 0 {
			t.Fatalf("an expired lease has %vs remaining, want 0", r.LeaseRemainingSeconds)
		}
	})
	t.Run("not a master database", func(t *testing.T) {
		other := filepath.Join(t.TempDir(), "other.sqlite")
		if err := os.WriteFile(other, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := InspectDatabase(ctx, other, key, now); err == nil {
			t.Fatal("expected an error for a file without the master schema")
		}
	})
}

func hasProblem(problems []string, substr string) bool {
	for _, p := range problems {
		if strings.Contains(p, substr) {
			return true
		}
	}
	return false
}
