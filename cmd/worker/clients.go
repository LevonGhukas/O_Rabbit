package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// clientCache reuses expensive clients within a single worker process.
//
// The cache is intentionally small (last-config wins) because a worker is
// typically bound to one job configuration at a time.
type clientCache struct {
	mu sync.Mutex

	sqlEngine string
	sqlDSN    string
	sqlReader connectors.TableReader

	flightSQLDSN string
	flightSQL    *connectors.FlightSQL

	mongoEngine    string
	mongoDSN       string
	mongoDocReader connectors.DocumentReader

	s3Key string
	s3    *s3io.Uploader
}

func (c *clientCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sqlReader != nil {
		_ = c.sqlReader.Close()
		c.sqlReader = nil
		c.sqlEngine = ""
		c.sqlDSN = ""
	}
	if c.flightSQL != nil {
		_ = c.flightSQL.Close()
		c.flightSQL = nil
		c.flightSQLDSN = ""
	}
	if c.mongoDocReader != nil {
		_ = c.mongoDocReader.Close()
		c.mongoDocReader = nil
		c.mongoEngine = ""
		c.mongoDSN = ""
	}

	// s3io.Uploader currently has no Close; drop reference.
	c.s3 = nil
	c.s3Key = ""
}

func (c *clientCache) SQLReader(ctx context.Context, engine, dsn string) (connectors.TableReader, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	engine = connectors.NormalizeSourceEngine(engine)
	dsn = strings.TrimSpace(dsn)
	if engine == "" {
		return nil, 0, fmt.Errorf("empty source engine")
	}
	if dsn == "" {
		return nil, 0, fmt.Errorf("empty dsn")
	}

	if c.sqlReader != nil && c.sqlEngine == engine && c.sqlDSN == dsn {
		return c.sqlReader, 0, nil
	}

	if c.sqlReader != nil {
		_ = c.sqlReader.Close()
		c.sqlReader = nil
		c.sqlEngine = ""
		c.sqlDSN = ""
	}

	start := time.Now()
	r, err := connectors.OpenIntRangeReader(ctx, engine, dsn)
	if err != nil {
		return nil, time.Since(start).Milliseconds(), err
	}

	c.sqlReader = r
	c.sqlEngine = engine
	c.sqlDSN = dsn
	return r, time.Since(start).Milliseconds(), nil
}

func (c *clientCache) FlightSQL(ctx context.Context, dsn string) (*connectors.FlightSQL, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, 0, fmt.Errorf("empty dsn")
	}

	if c.flightSQL != nil && c.flightSQLDSN == dsn {
		return c.flightSQL, 0, nil
	}

	if c.flightSQL != nil {
		_ = c.flightSQL.Close()
		c.flightSQL = nil
		c.flightSQLDSN = ""
	}
	if c.mongoDocReader != nil {
		_ = c.mongoDocReader.Close()
		c.mongoDocReader = nil
		c.mongoEngine = ""
		c.mongoDSN = ""
	}

	start := time.Now()
	conn, err := connectors.OpenFlightSQL(ctx, dsn)
	if err != nil {
		return nil, time.Since(start).Milliseconds(), err
	}

	c.flightSQL = conn
	c.flightSQLDSN = dsn
	return conn, time.Since(start).Milliseconds(), nil
}

func (c *clientCache) DocumentReader(ctx context.Context, engine, dsn string) (connectors.DocumentReader, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	engine = connectors.NormalizeSourceEngine(engine)
	dsn = strings.TrimSpace(dsn)
	if engine == "" {
		return nil, 0, fmt.Errorf("empty source engine")
	}
	if dsn == "" {
		return nil, 0, fmt.Errorf("empty dsn")
	}

	if c.mongoDocReader != nil && c.mongoEngine == engine && c.mongoDSN == dsn {
		return c.mongoDocReader, 0, nil
	}

	if c.mongoDocReader != nil {
		_ = c.mongoDocReader.Close()
		c.mongoDocReader = nil
		c.mongoEngine = ""
		c.mongoDSN = ""
	}

	start := time.Now()
	r, err := connectors.OpenDocumentReader(ctx, engine, dsn)
	if err != nil {
		return nil, time.Since(start).Milliseconds(), err
	}

	c.mongoDocReader = r
	c.mongoEngine = engine
	c.mongoDSN = dsn
	return r, time.Since(start).Milliseconds(), nil
}

func (c *clientCache) S3(ctx context.Context, cfg s3io.Config) (*s3io.Uploader, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Avoid putting secrets into the cache key; AccessKeyID is enough to detect config changes.
	key := fmt.Sprintf("%s|%s|%s|%t|%s", strings.TrimSpace(cfg.Endpoint), strings.TrimSpace(cfg.Region), strings.TrimSpace(cfg.Bucket), cfg.ForcePathStyle, strings.TrimSpace(cfg.AccessKeyID))

	if c.s3 != nil && c.s3Key == key {
		return c.s3, 0, nil
	}

	start := time.Now()
	u, err := s3io.New(ctx, cfg)
	if err != nil {
		return nil, time.Since(start).Milliseconds(), err
	}

	c.s3 = u
	c.s3Key = key
	return u, time.Since(start).Milliseconds(), nil
}
