package connectors

import (
	"context"
	"fmt"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// DecimalColumnStats describes the values one decimal column holds.
type DecimalColumnStats struct {
	Column string
	// Values is the number of non-null values read.
	Values int64
	// IntegerDigits and Scale are the most digits any value needs before and
	// after the decimal point.
	IntegerDigits int
	Scale         int
}

// DecimalColumnScanner is implemented by sources whose decimal columns
// carry no declared precision or scale, so a fitting decimal(p,s) can only be
// chosen from the values themselves.
type DecimalColumnScanner interface {
	ScanDecimalColumns(ctx context.Context, table, query string, columns []string) (map[string]DecimalColumnStats, error)
}

// ScanDecimalColumns reads the given decimal columns once and reports, per
// column, how many integer and fractional digits its values need. In table
// mode only these columns are selected; in query mode the query is run and
// only these columns are read from its rows.
func (c *Cassandra) ScanDecimalColumns(ctx context.Context, table, query string, columns []string) (map[string]DecimalColumnStats, error) {
	if len(columns) == 0 {
		return map[string]DecimalColumnStats{}, nil
	}
	cql := strings.TrimSpace(query)
	if cql != "" {
		if err := validateCassandraCQLQuery(cql); err != nil {
			return nil, err
		}
	} else {
		keyspace, tableName, err := splitCassandraTableIdent(c.keyspace, table)
		if err != nil {
			return nil, err
		}
		quoted := make([]string, len(columns))
		for i, col := range columns {
			q, err := quoteCassandraIdent(col)
			if err != nil {
				return nil, err
			}
			quoted[i] = q
		}
		qk, err := quoteCassandraIdent(keyspace)
		if err != nil {
			return nil, err
		}
		qt, err := quoteCassandraIdent(tableName)
		if err != nil {
			return nil, err
		}
		cql = fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(quoted, ", "), qk, qt)
	}

	iter := c.session.Query(cql).WithContext(ctx).Iter()
	scanner := newCassandraRowScanner(iter.Columns())
	stats := make(map[string]DecimalColumnStats, len(columns))
	for _, col := range columns {
		stats[col] = DecimalColumnStats{Column: col}
	}
	for {
		row, ok := scanner.scan(iter)
		if !ok {
			break
		}
		for _, col := range columns {
			value := normalizeCassandraValue(row[col])
			if value == nil {
				continue
			}
			text, isText := value.(string)
			intDigits, scale, parsed := typesystem.DecimalDigits(text)
			if !isText || !parsed {
				_ = iter.Close()
				return nil, fmt.Errorf("cassandra decimal scan: column %s: unexpected value %T", col, value)
			}
			s := stats[col]
			s.Values++
			s.IntegerDigits = max(s.IntegerDigits, intDigits)
			s.Scale = max(s.Scale, scale)
			stats[col] = s
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("cassandra decimal scan: %w", err)
	}
	return stats, nil
}
