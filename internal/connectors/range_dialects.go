package connectors

// Per-dialect resolution of the table, cursor column and predicate shared by
// each driver's QueryCursor and CountRange.

func postgresRange(q CursorQuery) (rangeSQL, error) {
	qt, err := quotePostgresMultipartIdent(q.Table)
	if err != nil {
		return rangeSQL{}, err
	}
	qc, err := quotePostgresMultipartIdent(q.CursorColumn)
	if err != nil {
		return rangeSQL{}, err
	}
	return newRangeSQL(q, qt, qc, true, dollarPlaceholder)
}

func mysqlRange(q CursorQuery) (rangeSQL, error) {
	qt, err := quoteMySQLMultipartIdent(q.Table)
	if err != nil {
		return rangeSQL{}, err
	}
	qc, err := quoteMySQLMultipartIdent(q.CursorColumn)
	if err != nil {
		return rangeSQL{}, err
	}
	return newRangeSQL(q, qt, qc, true, questionPlaceholder)
}

func mariadbRange(q CursorQuery) (rangeSQL, error) {
	qt, err := quoteMySQLMultipartIdent(q.Table)
	if err != nil {
		return rangeSQL{}, err
	}
	qc, err := quoteMySQLMultipartIdent(q.CursorColumn)
	if err != nil {
		return rangeSQL{}, err
	}
	return newRangeSQL(q, qt, qc, true, questionPlaceholder)
}

func mssqlRange(q CursorQuery) (rangeSQL, error) {
	qt, err := quoteMSSQLMultipartIdent(q.Table)
	if err != nil {
		return rangeSQL{}, err
	}
	qc, err := quoteMSSQLMultipartIdent(q.CursorColumn)
	if err != nil {
		return rangeSQL{}, err
	}
	return newRangeSQL(q, qt, qc, true, mssqlPlaceholder)
}

func clickHouseRange(q CursorQuery) (rangeSQL, error) {
	qt, err := quoteClickHouseMultipartIdent(q.Table)
	if err != nil {
		return rangeSQL{}, err
	}
	qc, err := quoteClickHouseMultipartIdent(q.CursorColumn)
	if err != nil {
		return rangeSQL{}, err
	}
	return newRangeSQL(q, qt, qc, true, questionPlaceholder)
}

func trinoRange(q CursorQuery) (rangeSQL, error) {
	qt, err := quoteTrinoMultipartIdent(q.Table)
	if err != nil {
		return rangeSQL{}, err
	}
	qc, err := quoteTrinoMultipartIdent(q.CursorColumn)
	if err != nil {
		return rangeSQL{}, err
	}
	return newRangeSQL(q, qt, qc, true, questionPlaceholder)
}

func oracleRange(q CursorQuery) (rangeSQL, error) {
	qt, err := quoteOracleMultipartIdent(q.Table)
	if err != nil {
		return rangeSQL{}, err
	}
	qc, err := quoteOracleCursorIdent(q.CursorColumn)
	if err != nil {
		return rangeSQL{}, err
	}
	return newRangeSQL(q, qt, qc, true, oraclePlaceholder)
}
