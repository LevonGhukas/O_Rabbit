package icebergreg

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

type datasetState struct {
	Bucket            string   `json:"bucket"`
	Prefix            string   `json:"prefix"`
	LastCommittedRun  string   `json:"last_committed_run_id"`
	CommittedAt       string   `json:"committed_at"`
	MaxHWMValue       string   `json:"max_hwm_value"`
	MaxPart           int      `json:"max_part"`
	NextPart          int      `json:"next_part"`
	LastRunObjects    []string `json:"last_run_objects"`
	LastRunObjectsV2  []string `json:"last_run_objects_v2"`
	LastCommittedKeys []string `json:"last_committed_objects"`
}

type commitCheckpoint struct {
	RunID       string
	CommittedAt string
}

type iceState struct {
	LastInsertedPart int             `json:"last_inserted_part"`
	LastRunID        string          `json:"last_run_id,omitempty"`
	UpdatedAt        string          `json:"updated_at"`
	CatalogReceipt   json.RawMessage `json:"catalog_receipt"`
}

type icebergObj struct {
	key   string
	part  int
	rows  int64
	bytes int64
}

func collectCommittedKeys(ctx context.Context, u *s3io.Uploader, basePrefix string, ds datasetState, objStats map[string]struct{ rows, bytes int64 }) ([]string, error) {
	keys := []string{}
	if commitKeys, err := u.ListKeys(ctx, basePrefix+"/_commits/"); err == nil && len(commitKeys) != 0 {
		type commitManifest struct {
			Objects   []string         `json:"objects"`
			ObjectsV2 []map[string]any `json:"objects_v2"`
		}
		for _, ck := range commitKeys {
			b, ok, err := u.GetObjectBytes(ctx, ck)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			var cm commitManifest
			if err := json.Unmarshal(b, &cm); err != nil {
				continue
			}
			keys = append(keys, cm.Objects...)
			for _, ov := range cm.ObjectsV2 {
				key, _ := ov["key"].(string)
				key = strings.TrimSpace(key)
				if key == "" {
					continue
				}
				var rows int64
				var bytes int64
				if v, ok := ov["rows"].(float64); ok {
					rows = int64(v)
				}
				if v, ok := ov["bytes"].(float64); ok {
					bytes = int64(v)
				}
				if rows != 0 || bytes != 0 {
					objStats[key] = struct{ rows, bytes int64 }{rows: rows, bytes: bytes}
				}
			}
		}
	}
	if len(keys) == 0 {
		keys = ds.LastRunObjects
		if len(keys) == 0 {
			keys = ds.LastRunObjectsV2
		}
		if len(keys) == 0 {
			keys = ds.LastCommittedKeys
		}
	}
	if len(keys) != 0 {
		return keys, nil
	}

	listed, err := u.ListKeys(ctx, basePrefix+"/part-")
	if err != nil {
		return nil, fmt.Errorf("list parquet keys: %w", err)
	}
	return listed, nil
}

func waitForObject(ctx context.Context, u *s3io.Uploader, bucket, key, stateKey string) error {
	deadline := time.Now().Add(1 * time.Second)
	for {
		if _, err := u.Head(ctx, key); err == nil {
			return nil
		} else if !isNotFoundErr(err) {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("dataset state references missing object: s3://%s/%s (state %s)", bucket, key, stateKey)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (s datasetState) commitCheckpoint() commitCheckpoint {
	return commitCheckpoint{
		RunID:       strings.TrimSpace(s.LastCommittedRun),
		CommittedAt: strings.TrimSpace(s.CommittedAt),
	}
}

func nextIceState(prev iceState, cp commitCheckpoint, fallbackRunID string, lastInsertedPart int) iceState {
	runID := strings.TrimSpace(cp.RunID)
	if runID == "" {
		runID = strings.TrimSpace(fallbackRunID)
	}
	updatedAt := strings.TrimSpace(cp.CommittedAt)
	if updatedAt == "" {
		updatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	prev.LastInsertedPart = lastInsertedPart
	prev.LastRunID = runID
	prev.UpdatedAt = updatedAt
	return prev
}

func parsePartNum(key string) (int, bool) {
	i := strings.LastIndex(key, "/part-")
	if i < 0 {
		return 0, false
	}
	s := key[i+len("/part-"):]
	if !strings.HasSuffix(s, ".parquet") {
		return 0, false
	}
	s = strings.TrimSuffix(s, ".parquet")
	if s == "" {
		return 0, false
	}
	if dash := strings.IndexByte(s, '-'); dash >= 0 {
		suffix := s[dash+1:]
		if suffix == "" {
			return 0, false
		}
		for _, r := range suffix {
			if r < '0' || r > '9' {
				return 0, false
			}
		}
		s = s[:dash]
		if s == "" {
			return 0, false
		}
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int(r-'0')
	}
	return n, true
}

func parsePartFileIndex(key string) (int, bool) {
	i := strings.LastIndex(key, "/part-")
	if i < 0 {
		return 0, false
	}
	s := key[i+len("/part-"):]
	if !strings.HasSuffix(s, ".parquet") {
		return 0, false
	}
	s = strings.TrimSuffix(s, ".parquet")
	if s == "" {
		return 0, false
	}
	if dash := strings.IndexByte(s, '-'); dash >= 0 {
		s = s[dash+1:]
		if s == "" {
			return 0, false
		}
		n := 0
		for _, r := range s {
			if r < '0' || r > '9' {
				return 0, false
			}
			n = n*10 + int(r-'0')
		}
		return n, true
	}
	return 0, true
}

func isNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	le := strings.ToLower(err.Error())
	return strings.Contains(le, "notfound") ||
		strings.Contains(le, "no such key") ||
		strings.Contains(le, "nosuchkey") ||
		strings.Contains(le, "not found") ||
		strings.Contains(le, "status code: 404")
}

func loadDatasetState(ctx context.Context, u *s3io.Uploader, stateKey string, wantRunID string) (datasetState, error) {
	deadline := time.Now().Add(2 * time.Second)
	backoff := 25 * time.Millisecond
	var lastErr error

	for {
		b, ok, err := u.GetObjectBytes(ctx, stateKey)
		if err != nil {
			return datasetState{}, err
		}
		if ok {
			var ds datasetState
			if err := json.Unmarshal(b, &ds); err != nil {
				return datasetState{}, fmt.Errorf("parse dataset state: %w", err)
			}
			if strings.TrimSpace(ds.LastCommittedRun) == "" || strings.EqualFold(ds.LastCommittedRun, wantRunID) {
				return ds, nil
			}
			lastErr = fmt.Errorf("dataset state last_committed_run_id=%s (want %s)", ds.LastCommittedRun, wantRunID)
		} else {
			lastErr = fmt.Errorf("missing dataset state: %s", stateKey)
		}

		if time.Now().After(deadline) {
			return datasetState{}, lastErr
		}
		select {
		case <-ctx.Done():
			return datasetState{}, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 300*time.Millisecond {
			backoff *= 2
			if backoff > 300*time.Millisecond {
				backoff = 300 * time.Millisecond
			}
		}
	}
}
