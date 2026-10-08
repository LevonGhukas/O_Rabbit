package planner

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

type cursorValidator interface {
	ValidateCursorColumn(ctx context.Context, table, cursorColumn string) (connectors.CursorColumnValidation, error)
}

type tableDescriber interface {
	DescribeTable(ctx context.Context, table string) ([]string, []*sql.ColumnType, error)
}

type cursorStatDiscoverer interface {
	DiscoverCursorStats(ctx context.Context, table, cursorColumn string, domain connectors.CursorDomain) (connectors.CursorStats, error)
}

// discoverCursorStats handles source statistics discovery for ordered-cursor capable engines.
func discoverCursorStats(ctx context.Context, r cursorStatDiscoverer, sourceEngine, table, cursorCol string, domain connectors.CursorDomain) (connectors.CursorStats, error) {
	stats, err := r.DiscoverCursorStats(ctx, table, cursorCol, domain)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return connectors.CursorStats{}, fmt.Errorf("ordered-cursor stats query timed out (engine=%s table=%s cursor_column=%s deadline=%s): %w", sourceEngine, table, cursorCol, ctxDeadlineRFC3339(ctx), err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return connectors.CursorStats{}, fmt.Errorf("ordered-cursor stats query was canceled (engine=%s table=%s cursor_column=%s): %w", sourceEngine, table, cursorCol, err)
		}
		return connectors.CursorStats{}, fmt.Errorf("ordered-cursor stats query failed (engine=%s table=%s cursor_column=%s): %w", sourceEngine, table, cursorCol, err)
	}
	return stats, nil
}

func discoverQueryCursorStats(ctx context.Context, r connectors.SourceQueryReader, sourceEngine, query, cursorCol string, domain connectors.CursorDomain, queryHash string) (connectors.CursorStats, error) {
	stats, err := r.DiscoverQueryCursorStats(ctx, query, cursorCol, domain)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return connectors.CursorStats{}, fmt.Errorf("query-mode stats query timed out (engine=%s query_hash=%s cursor_column=%s deadline=%s): %w", sourceEngine, queryHash, cursorCol, ctxDeadlineRFC3339(ctx), err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return connectors.CursorStats{}, fmt.Errorf("query-mode stats query was canceled (engine=%s query_hash=%s cursor_column=%s): %w", sourceEngine, queryHash, cursorCol, err)
		}
		return connectors.CursorStats{}, fmt.Errorf("query-mode stats query failed (engine=%s query_hash=%s cursor_column=%s): %w", sourceEngine, queryHash, cursorCol, err)
	}
	return stats, nil
}

func validateCursorColumn(ctx context.Context, r cursorValidator, sourceEngine, table, cursorCol string) (connectors.CursorColumnValidation, error) {
	cv, err := r.ValidateCursorColumn(ctx, table, cursorCol)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return connectors.CursorColumnValidation{}, fmt.Errorf("ordered-cursor column validation timed out (engine=%s table=%s cursor_column=%s deadline=%s): %w", sourceEngine, table, cursorCol, ctxDeadlineRFC3339(ctx), err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return connectors.CursorColumnValidation{}, fmt.Errorf("ordered-cursor column validation was canceled (engine=%s table=%s cursor_column=%s): %w", sourceEngine, table, cursorCol, err)
		}
		return connectors.CursorColumnValidation{}, fmt.Errorf("ordered-cursor column validation failed (engine=%s table=%s cursor_column=%s): %w", sourceEngine, table, cursorCol, err)
	}
	return cv, nil
}

func validateQueryCursorColumn(ctx context.Context, r connectors.SourceQueryReader, sourceEngine, query, cursorCol, queryHash string) (connectors.CursorColumnValidation, error) {
	cv, err := r.ValidateQueryCursorColumn(ctx, query, cursorCol)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return connectors.CursorColumnValidation{}, fmt.Errorf("query-mode column validation timed out (engine=%s query_hash=%s cursor_column=%s deadline=%s): %w", sourceEngine, queryHash, cursorCol, ctxDeadlineRFC3339(ctx), err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return connectors.CursorColumnValidation{}, fmt.Errorf("query-mode column validation was canceled (engine=%s query_hash=%s cursor_column=%s): %w", sourceEngine, queryHash, cursorCol, err)
		}
		return connectors.CursorColumnValidation{}, fmt.Errorf("query-mode column validation failed (engine=%s query_hash=%s cursor_column=%s): %w", sourceEngine, queryHash, cursorCol, err)
	}
	return cv, nil
}

func openCursorReader(ctx context.Context, st *db.Store, k crypto.Key, connID, sourceEngine string) (connectors.TableReader, error) {
	src, err := st.GetConnection(ctx, connID)
	if err != nil {
		return nil, err
	}
	sec, err := crypto.Decrypt(k, src.SecretEncBlob, []byte(src.ID))
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(sec, &m); err != nil {
		return nil, err
	}
	dsn, _ := m["dsn"].(string)
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("source connection secret missing dsn")
	}
	r, err := connectors.OpenCursorReader(ctx, sourceEngine, dsn)
	if err != nil {
		return nil, fmt.Errorf("open source reader (engine=%s): %w", sourceEngine, err)
	}
	return r, nil
}

func openDocumentReader(ctx context.Context, st *db.Store, k crypto.Key, connID, sourceEngine string) (connectors.DocumentReader, error) {
	src, err := st.GetConnection(ctx, connID)
	if err != nil {
		return nil, err
	}
	sec, err := crypto.Decrypt(k, src.SecretEncBlob, []byte(src.ID))
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(sec, &m); err != nil {
		return nil, err
	}
	dsn, _ := m["dsn"].(string)
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("source connection secret missing dsn")
	}
	r, err := connectors.OpenDocumentReader(ctx, sourceEngine, dsn)
	if err != nil {
		return nil, fmt.Errorf("open source reader (engine=%s): %w", sourceEngine, err)
	}
	return r, nil
}

func ctxDeadlineRFC3339(ctx context.Context) string {
	if d, ok := ctx.Deadline(); ok {
		return d.UTC().Format(time.RFC3339Nano)
	}
	return "none"
}
