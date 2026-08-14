package connectors

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqlDriver "github.com/go-sql-driver/mysql"
)

// CDCStreamReader represents a stream reader that tails logical changes.
type CDCStreamReader interface {
	Close() error
	// StartStream starts capturing changes for a given table from a known position.
	StartStream(ctx context.Context, table string, startPosition string) error
	// NextRecord blocks until a new record is available and returns the raw JSON payload and the new cursor position.
	NextRecord(ctx context.Context) ([]byte, string, error)
}

// CDCRecord represents a unified change event structure.
type CDCRecord struct {
	Action string         `json:"action"` // INSERT, UPDATE, DELETE
	Table  string         `json:"table"`
	Data   map[string]any `json:"data"`
}

type MySQLCDC struct {
	dsn      string
	canal    *canal.Canal
	events   chan recordEvent
	closeCtx context.Context
	cancel   context.CancelFunc
}

type recordEvent struct {
	Payload  []byte
	Position string
	Error    error
}

func OpenMySQLCDC(ctx context.Context, dsn string) (*MySQLCDC, error) {
	cCtx, cancel := context.WithCancel(context.Background())
	return &MySQLCDC{
		dsn:      dsn,
		events:   make(chan recordEvent, 1024),
		closeCtx: cCtx,
		cancel:   cancel,
	}, nil
}

func (c *MySQLCDC) Close() error {
	c.cancel()
	if c.canal != nil {
		c.canal.Close()
	}
	return nil
}

func (c *MySQLCDC) StartStream(ctx context.Context, table string, startPosition string) error {
	cfg, err := parseDSNToCanalConfig(c.dsn)
	if err != nil {
		return err
	}

	cfg.Dump.ExecutionPath = "" // Disable mysqldump for pure CDC

	if table != "" {
		parts := strings.Split(table, ".")
		if len(parts) == 2 {
			cfg.IncludeTableRegex = []string{fmt.Sprintf("^%s\\.%s$", parts[0], parts[1])}
		} else {
			cfg.IncludeTableRegex = []string{fmt.Sprintf("^.*\\.%s$", table)}
		}
	}

	cnl, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("create canal: %w", err)
	}
	c.canal = cnl

	handler := &mysqlEventHandler{
		canal:    c.canal,
		events:   c.events,
		closeCtx: c.closeCtx,
	}
	c.canal.SetEventHandler(handler)

	var startPos mysql.Position
	if startPosition != "" {
		posParts := strings.Split(startPosition, ":")
		if len(posParts) == 2 {
			p, err := strconv.ParseUint(posParts[1], 10, 32)
			if err == nil {
				startPos = mysql.Position{
					Name: posParts[0],
					Pos:  uint32(p),
				}
			}
		}
	}

	go func() {
		var err error
		if startPos.Name != "" {
			err = c.canal.RunFrom(startPos)
		} else {
			err = c.canal.Run()
		}
		if err != nil && err != context.Canceled && !strings.Contains(err.Error(), "canal is closed") {
			select {
			case c.events <- recordEvent{Error: err}:
			case <-c.closeCtx.Done():
			}
		}
	}()

	return nil
}

func (c *MySQLCDC) NextRecord(ctx context.Context) ([]byte, string, error) {
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

func parseDSNToCanalConfig(dsn string) (*canal.Config, error) {
	parsed, err := mysqlDriver.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse mysql dsn: %w", err)
	}

	cfg := canal.NewDefaultConfig()
	cfg.Addr = parsed.Addr
	cfg.User = parsed.User
	cfg.Password = parsed.Passwd

	// Default to MySQL server ID if not set to avoid conflicts. In prod, this should be configurable.
	cfg.ServerID = 1001
	return cfg, nil
}

// mysqlEventHandler implements canal.DummyEventHandler to capture binlog rows.
type mysqlEventHandler struct {
	canal.DummyEventHandler
	canal    *canal.Canal
	events   chan recordEvent
	closeCtx context.Context
}

func (h *mysqlEventHandler) OnRow(e *canal.RowsEvent) error {
	select {
	case <-h.closeCtx.Done():
		return h.closeCtx.Err()
	default:
	}

	// We only process DML (INSERT, UPDATE, DELETE)
	var action string
	switch e.Action {
	case canal.InsertAction:
		action = "INSERT"
	case canal.UpdateAction:
		action = "UPDATE"
	case canal.DeleteAction:
		action = "DELETE"
	default:
		return nil
	}

	tableName := fmt.Sprintf("%s.%s", e.Table.Schema, e.Table.Name)
	syncedPos := h.canal.SyncedPosition()
	pos := fmt.Sprintf("%s:%d", syncedPos.Name, e.Header.LogPos)

	// In go-mysql, Rows[0] is typically the row data.
	// For UPDATE, Rows[0] is before, Rows[1] is after.
	var rowIndex int
	if action == "UPDATE" && len(e.Rows) > 1 {
		rowIndex = 1
	}

	if rowIndex < len(e.Rows) {
		rowData := make(map[string]any)
		for i, col := range e.Table.Columns {
			rowData[col.Name] = e.Rows[rowIndex][i]
		}

		record := CDCRecord{
			Action: action,
			Table:  tableName,
			Data:   rowData,
		}

		payload, err := json.Marshal(record)
		if err == nil {
			select {
			case h.events <- recordEvent{Payload: payload, Position: pos}:
			case <-h.closeCtx.Done():
				return h.closeCtx.Err()
			}
		}
	}

	return nil
}

func (h *mysqlEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	// Not strictly required for raw tailing, but useful for checkpointing if the queue is empty.
	return nil
}
