package connectors

import (
	"database/sql"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// Source connections are opened read-only where the database supports a
// session-level read-only mode, as defense in depth behind the query checks.
// It is not a sandbox: a read-only session still runs side-effecting
// functions that do not write data (pg_terminate_backend, dblink), and some
// engines have no such mode. Source connections must use a read-only database
// user; see "Source database access" in the README.

// openReadOnlyPostgres opens a pool whose sessions default to read-only
// transactions (default_transaction_read_only=on).
func openReadOnlyPostgres(dsn string) (*sql.DB, error) {
	cfg, err := readOnlyPostgresConfig(dsn)
	if err != nil {
		return nil, err
	}
	return stdlib.OpenDB(*cfg), nil
}

func readOnlyPostgresConfig(dsn string) (*pgx.ConnConfig, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["default_transaction_read_only"] = "on"
	return cfg, nil
}

// readOnlyMySQLDSN adds a session variable that makes every transaction on
// the connection read-only. MySQL uses transaction_read_only; MariaDB before
// 11.1 only knows tx_read_only.
func readOnlyMySQLDSN(dsn, variable string) (string, error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params[variable] = "1"
	return cfg.FormatDSN(), nil
}

// openReadOnlyClickHouse opens a pool with readonly=2: data and DDL writes are
// refused while per-query settings stay allowed for the driver.
func openReadOnlyClickHouse(dsn string) (*sql.DB, error) {
	opts, err := readOnlyClickHouseOptions(dsn)
	if err != nil {
		return nil, err
	}
	return clickhouse.OpenDB(opts), nil
}

func readOnlyClickHouseOptions(dsn string) (*clickhouse.Options, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse dsn: %w", err)
	}
	if opts.Settings == nil {
		opts.Settings = clickhouse.Settings{}
	}
	opts.Settings["readonly"] = 2
	return opts, nil
}
