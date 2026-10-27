package connectors

import (
	"context"
	"fmt"
	"strings"
)

// CountRange implementations. Each counts the rows the matching QueryCursor
// selects: same source expression, same predicate, same arguments. They never
// apply SnapshotContext or ORDER BY.

func (p *Postgres) CountRange(ctx context.Context, q CursorQuery) (int64, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCount(ctx, p.db, "postgres", q)
	}
	rng, err := postgresRange(q)
	if err != nil {
		return 0, err
	}
	return queryRangeCount(ctx, p.db, countFrom(countExpr, rng.Table, rng), rng.Args)
}

func (m *MySQL) CountRange(ctx context.Context, q CursorQuery) (int64, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCount(ctx, m.db, "mysql", q)
	}
	rng, err := mysqlRange(q)
	if err != nil {
		return 0, err
	}
	return queryRangeCount(ctx, m.db, countFrom(countExpr, rng.Table, rng), rng.Args)
}

func (m *MariaDB) CountRange(ctx context.Context, q CursorQuery) (int64, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCount(ctx, m.db, "mariadb", q)
	}
	rng, err := mariadbRange(q)
	if err != nil {
		return 0, err
	}
	return queryRangeCount(ctx, m.db, countFrom(countExpr, rng.Table, rng), rng.Args)
}

func (m *MSSQL) CountRange(ctx context.Context, q CursorQuery) (int64, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCount(ctx, m.db, "mssql", q)
	}
	rng, err := mssqlRange(q)
	if err != nil {
		return 0, err
	}
	return queryRangeCount(ctx, m.db, countFrom(mssqlCountExpr, rng.Table+" WITH (NOLOCK)", rng), rng.Args)
}

func (o *Oracle) CountRange(ctx context.Context, q CursorQuery) (int64, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCount(ctx, o.db, "oracle", q)
	}
	rng, err := oracleRange(q)
	if err != nil {
		return 0, err
	}
	return queryRangeCount(ctx, o.db, countFrom(countExpr, rng.Table, rng), rng.Args)
}

func (c *ClickHouse) CountRange(ctx context.Context, q CursorQuery) (int64, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCount(ctx, c.db, "clickhouse", q)
	}
	rng, err := clickHouseRange(q)
	if err != nil {
		return 0, err
	}
	return queryRangeCount(ctx, c.db, countFrom(clickHouseCountExpr, rng.Table, rng), rng.Args)
}

func (t *Trino) CountRange(ctx context.Context, q CursorQuery) (int64, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCount(ctx, t.db, "trino", q)
	}
	rng, err := trinoRange(q)
	if err != nil {
		return 0, err
	}
	return queryRangeCount(ctx, t.db, countFrom(countExpr, rng.Table, rng), rng.Args)
}

// CountRange delegates to the wrapped reader when it can count ranges.
func (w *errorWrappedTableReader) CountRange(ctx context.Context, q CursorQuery) (int64, error) {
	counter, ok := w.inner.(RangeCounter)
	if !ok {
		return 0, fmt.Errorf("range counting is not supported for this source")
	}
	n, err := counter.CountRange(ctx, q)
	return n, ClassifyConnectorError(err)
}
