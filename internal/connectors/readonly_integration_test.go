package connectors

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestReadOnlySessionsRefuseWrites runs against real databases when their DSNs
// are set, for example:
//
//	ORABBIT_IT_POSTGRES_DSN=postgres://postgres:pw@localhost:5432/postgres?sslmode=disable
//	ORABBIT_IT_MYSQL_DSN=root:pw@tcp(localhost:3306)/mysql
//	ORABBIT_IT_MARIADB_DSN=root:pw@tcp(localhost:3307)/mysql
//	ORABBIT_IT_CLICKHOUSE_DSN=clickhouse://default:@localhost:9000/default
func TestReadOnlySessionsRefuseWrites(t *testing.T) {
	// With ORABBIT_IT_POSTGRES_SNAPSHOT_TABLE (a table with a bigint "id"
	// column), also check that consistent-snapshot reads work read-only.
	if dsn, table := os.Getenv("ORABBIT_IT_POSTGRES_DSN"), os.Getenv("ORABBIT_IT_POSTGRES_SNAPSHOT_TABLE"); dsn != "" && table != "" {
		t.Run("postgres snapshot", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			p, err := OpenPostgres(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			snapshot, err := p.ExportSnapshot(ctx)
			if err != nil {
				t.Fatalf("export snapshot in a read-only session: %v", err)
			}
			rows, _, _, _, err := p.QueryCursor(ctx, CursorQuery{Table: table, CursorColumn: "id", CursorDomain: CursorDomainInt64, SnapshotContext: snapshot})
			if err != nil {
				t.Fatalf("read at snapshot in a read-only session: %v", err)
			}
			defer rows.Close()
			n := 0
			for rows.Next() {
				n++
			}
			if err := rows.Err(); err != nil || n == 0 {
				t.Fatalf("snapshot read rows=%d err=%v", n, err)
			}
		})
	}
	cases := []struct {
		env  string
		open func(context.Context, string) (interface{ Close() error }, func(context.Context, string) error, error)
	}{
		{"ORABBIT_IT_POSTGRES_DSN", func(ctx context.Context, dsn string) (interface{ Close() error }, func(context.Context, string) error, error) {
			p, err := OpenPostgres(ctx, dsn)
			if err != nil {
				return nil, nil, err
			}
			return p, func(ctx context.Context, q string) error { _, err := p.db.ExecContext(ctx, q); return err }, nil
		}},
		{"ORABBIT_IT_MYSQL_DSN", func(ctx context.Context, dsn string) (interface{ Close() error }, func(context.Context, string) error, error) {
			m, err := OpenMySQL(ctx, dsn)
			if err != nil {
				return nil, nil, err
			}
			return m, func(ctx context.Context, q string) error { _, err := m.db.ExecContext(ctx, q); return err }, nil
		}},
		{"ORABBIT_IT_MARIADB_DSN", func(ctx context.Context, dsn string) (interface{ Close() error }, func(context.Context, string) error, error) {
			m, err := OpenMariaDB(ctx, dsn)
			if err != nil {
				return nil, nil, err
			}
			return m, func(ctx context.Context, q string) error { _, err := m.db.ExecContext(ctx, q); return err }, nil
		}},
		{"ORABBIT_IT_CLICKHOUSE_DSN", func(ctx context.Context, dsn string) (interface{ Close() error }, func(context.Context, string) error, error) {
			c, err := OpenClickHouse(ctx, dsn)
			if err != nil {
				return nil, nil, err
			}
			return c, func(ctx context.Context, q string) error { _, err := c.db.ExecContext(ctx, q); return err }, nil
		}},
	}
	ran := false
	for _, tc := range cases {
		dsn := os.Getenv(tc.env)
		if dsn == "" {
			continue
		}
		ran = true
		t.Run(tc.env, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			conn, exec, err := tc.open(ctx, dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer conn.Close()
			if err := exec(ctx, "SELECT 1"); err != nil {
				t.Fatalf("reads must work: %v", err)
			}
			if err := exec(ctx, "CREATE TABLE orabbit_readonly_probe (id INT) ENGINE = Memory"); err == nil && tc.env == "ORABBIT_IT_CLICKHOUSE_DSN" {
				t.Fatal("clickhouse session must refuse DDL")
			}
			if tc.env != "ORABBIT_IT_CLICKHOUSE_DSN" {
				if err := exec(ctx, "CREATE TABLE orabbit_readonly_probe (id INT)"); err == nil {
					t.Fatal("session must refuse writes")
				} else {
					t.Logf("write refused as expected: %v", err)
				}
			}
		})
	}
	if !ran {
		t.Skip("set ORABBIT_IT_*_DSN to run against real databases")
	}
}
