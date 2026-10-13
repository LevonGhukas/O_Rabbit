package connectors

import (
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func TestSourceSessionsAreReadOnly(t *testing.T) {
	pg, err := readOnlyPostgresConfig("postgres://u:p@db:5432/app?sslmode=disable")
	if err != nil || pg.RuntimeParams["default_transaction_read_only"] != "on" || pg.Database != "app" {
		t.Fatalf("postgres config=%+v err=%v", pg, err)
	}

	for variable, dsn := range map[string]string{"transaction_read_only": "u:p@tcp(db:3306)/app?parseTime=true", "tx_read_only": "u:p@tcp(db:3306)/app"} {
		ro, err := readOnlyMySQLDSN(dsn, variable)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := mysqldriver.ParseDSN(ro)
		if err != nil || cfg.Params[variable] != "1" || cfg.DBName != "app" {
			t.Fatalf("mysql dsn=%q params=%v err=%v", ro, cfg.Params, err)
		}
	}

	ch, err := readOnlyClickHouseOptions("clickhouse://u:p@db:9000/app?dial_timeout=5s")
	if err != nil || ch.Settings["readonly"] != 2 || ch.Auth.Database != "app" {
		t.Fatalf("clickhouse options=%+v err=%v", ch, err)
	}
}

func TestValidateWhereClause(t *testing.T) {
	for _, ok := range []string{
		"status = 'active'",
		"(region IN ('eu', 'us')) AND deleted_at IS NULL",
		"note <> 'it''s -- fine; (really'",
		`"Order Date" > DATE '2026-01-01'`,
	} {
		if err := ValidateWhereClause(ok); err != nil {
			t.Fatalf("valid clause %q rejected: %v", ok, err)
		}
	}
	for bad, want := range map[string]string{
		"1=1; DROP TABLE users":           "';'",
		"1=1 --":                          "comments",
		"1=1 /* hide */":                  "comments",
		"1=1) OR (1=1":                    "unbalanced",
		"(1=1":                            "unbalanced",
		"id IN (SELECT id FROM x) INTO y": `"INTO"`,
		"name = 'unterminated":            "unterminated",
	} {
		if err := ValidateWhereClause(bad); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("clause %q: err=%v, want %q", bad, err, want)
		}
	}
}
