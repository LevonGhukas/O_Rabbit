package connectors

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

type MSSQLCDC struct {
	db       *sql.DB
	events   chan recordEvent
	closeCtx context.Context
	cancel   context.CancelFunc
}

func OpenMSSQLCDC(ctx context.Context, dsn string) (*MSSQLCDC, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	cCtx, cancel := context.WithCancel(context.Background())
	return &MSSQLCDC{
		db:       db,
		events:   make(chan recordEvent, 1024),
		closeCtx: cCtx,
		cancel:   cancel,
	}, nil
}

func (c *MSSQLCDC) Close() error {
	c.cancel()
	return c.db.Close()
}

func (c *MSSQLCDC) StartStream(ctx context.Context, table string, startPosition string) error {
	// Parse capture instance name from table (e.g. dbo.users -> dbo_users)
	parts := strings.Split(table, ".")
	var captureInstance string
	if len(parts) == 2 {
		captureInstance = fmt.Sprintf("%s_%s", parts[0], parts[1])
	} else {
		captureInstance = fmt.Sprintf("dbo_%s", table) // Default schema
	}

	go c.pollLoop(captureInstance, startPosition)
	return nil
}

func (c *MSSQLCDC) pollLoop(captureInstance string, startPosition string) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var currentLsn []byte

	// Resolve initial LSN
	if startPosition != "" && strings.HasPrefix(startPosition, "0x") {
		// Convert hex string back to varbinary for SQL Server
		var err error
		currentLsn, err = c.getLsnFromHex(startPosition)
		if err != nil {
			c.sendError(fmt.Errorf("invalid start position %q: %w", startPosition, err))
			return
		}
	} else {
		var err error
		currentLsn, err = c.getMinLsn(captureInstance)
		if err != nil {
			c.sendError(fmt.Errorf("failed to get min lsn for %s: %w", captureInstance, err))
			return
		}
	}

	for {
		select {
		case <-c.closeCtx.Done():
			return
		case <-ticker.C:
			// 1. Get max LSN
			maxLsn, err := c.getMaxLsn()
			if err != nil {
				c.sendError(fmt.Errorf("failed to get max lsn: %w", err))
				return
			}
			if len(maxLsn) == 0 {
				continue
			}

			// 2. Check if currentLsn is within bounds
			if currentLsn != nil && compareLsn(currentLsn, maxLsn) > 0 {
				continue // We are ahead of max LSN, wait.
			}

			// 3. Query changes
			nextLsn, err := c.pollChanges(captureInstance, currentLsn, maxLsn)
			if err != nil {
				c.sendError(err)
				return
			}

			// 4. Update LSN for next iteration
			if nextLsn != nil {
				currentLsn = nextLsn
			}
		}
	}
}

func (c *MSSQLCDC) pollChanges(captureInstance string, fromLsn, toLsn []byte) ([]byte, error) {
	query := fmt.Sprintf(`
		SELECT 
			sys.fn_varbintohexstr(__$start_lsn) as lsn_hex,
			__$operation as operation,
			* 
		FROM cdc.fn_cdc_get_all_changes_%s(@p1, @p2, 'all')
		ORDER BY __$start_lsn ASC, __$seqval ASC
	`, captureInstance)

	rows, err := c.db.QueryContext(c.closeCtx, query, sql.Named("p1", fromLsn), sql.Named("p2", toLsn))
	if err != nil {
		// If the LSN provided is invalid (e.g. truncated), fn_cdc_get_all_changes throws an error.
		return nil, fmt.Errorf("cdc query failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		// Read all columns dynamically
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range cols {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		var lsnHex string
		var operation int
		rowData := make(map[string]interface{})

		for i, col := range cols {
			val := values[i]
			if val != nil {
				// Convert byte arrays to string if they represent strings in SQL Server,
				// or let JSON serialization handle base64 encoding.
				if b, ok := val.([]byte); ok {
					val = string(b)
				}
			}

			if col == "lsn_hex" {
				lsnHex = val.(string)
			} else if col == "operation" {
				if v, ok := val.(int64); ok {
					operation = int(v)
				}
			} else if !strings.HasPrefix(col, "__$") {
				rowData[col] = val
			}
		}

		// Map CDC operation to our standard action
		var action string
		switch operation {
		case 1:
			action = "DELETE"
		case 2:
			action = "INSERT"
		case 4:
			action = "UPDATE"
		case 3:
			continue // Skip UPDATE (before image), we only emit the after image (4)
		default:
			continue
		}

		// Emit the record
		tableName := strings.Replace(captureInstance, "_", ".", 1)
		record := CDCRecord{
			Action: action,
			Table:  tableName,
			Data:   rowData,
		}

		payload, err := json.Marshal(record)
		if err == nil {
			select {
			case c.events <- recordEvent{Payload: payload, Position: lsnHex}:
			case <-c.closeCtx.Done():
				return nil, c.closeCtx.Err()
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Calculate the next start LSN
	return c.incrementLsn(toLsn)
}

func (c *MSSQLCDC) getMinLsn(captureInstance string) ([]byte, error) {
	var lsn []byte
	query := "SELECT sys.fn_cdc_get_min_lsn(@p1)"
	err := c.db.QueryRowContext(c.closeCtx, query, sql.Named("p1", captureInstance)).Scan(&lsn)
	return lsn, err
}

func (c *MSSQLCDC) getMaxLsn() ([]byte, error) {
	var lsn []byte
	query := "SELECT sys.fn_cdc_get_max_lsn()"
	err := c.db.QueryRowContext(c.closeCtx, query).Scan(&lsn)
	return lsn, err
}

func (c *MSSQLCDC) incrementLsn(lsn []byte) ([]byte, error) {
	var nextLsn []byte
	query := "SELECT sys.fn_cdc_increment_lsn(@p1)"
	err := c.db.QueryRowContext(c.closeCtx, query, sql.Named("p1", lsn)).Scan(&nextLsn)
	return nextLsn, err
}

func (c *MSSQLCDC) getLsnFromHex(hexStr string) ([]byte, error) {
	var lsn []byte
	query := "SELECT CONVERT(varbinary(10), @p1, 1)" // Style 1 converts '0x...' to varbinary
	err := c.db.QueryRowContext(c.closeCtx, query, sql.Named("p1", hexStr)).Scan(&lsn)
	return lsn, err
}

func (c *MSSQLCDC) sendError(err error) {
	select {
	case c.events <- recordEvent{Error: err}:
	case <-c.closeCtx.Done():
	}
}

func (c *MSSQLCDC) NextRecord(ctx context.Context) ([]byte, string, error) {
	select {
	case <-ctx.Done():
		return nil, "", ctx.Err()
	case <-c.closeCtx.Done():
		return nil, "", fmt.Errorf("stream closed")
	case ev := <-c.events:
		if ev.Error != nil {
			return nil, "", ev.Error
		}
		return ev.Payload, ev.Position, nil
	}
}

func compareLsn(a, b []byte) int {
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	for i := 0; i < len(a); i++ {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}
	return 0
}
