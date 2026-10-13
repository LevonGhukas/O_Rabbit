package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
)

type WorkerInstance struct {
	BootID        string `json:"boot_id"`
	WorkerID      string `json:"worker_id"`
	Hostname      string `json:"hostname"`
	PID           int    `json:"pid"`
	Version       string `json:"version"`
	Status        string `json:"status"`
	StartedAt     string `json:"started_at"`
	LastHeartbeat string `json:"last_heartbeat"`
}

type Worker struct {
	ID            string           `json:"id"`
	Addr          string           `json:"addr"`
	Status        string           `json:"status"`
	LastHeartbeat string           `json:"last_heartbeat"`
	Capabilities  json.RawMessage  `json:"capabilities_json"`
	Instances     []WorkerInstance `json:"instances"`
}

func (s *Store) UpdateWorkerHeartbeat(ctx context.Context, bootID, workerID, addr, capabilities, hostname, version string, pid int) error {
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		now := nowUTC()

		_, err = tx.ExecContext(ctx, `
			INSERT INTO workers(id, addr, status, last_heartbeat, capabilities_json)
			VALUES (?, ?, 'ACTIVE', ?, ?)
			ON CONFLICT(id) DO UPDATE SET
				addr=COALESCE(NULLIF(excluded.addr, ''), workers.addr),
				status='ACTIVE',
				last_heartbeat=excluded.last_heartbeat,
				capabilities_json=COALESCE(NULLIF(excluded.capabilities_json, ''), workers.capabilities_json);`,
			workerID, addr, now, capabilities,
		)
		if err != nil {
			return err
		}

		if bootID != "" {
			_, err = tx.ExecContext(ctx, `
				INSERT INTO worker_instances(boot_id, worker_id, hostname, pid, version, status, started_at, last_heartbeat)
				VALUES (?, ?, ?, ?, ?, 'ACTIVE', ?, ?)
				ON CONFLICT(boot_id) DO UPDATE SET
					status='ACTIVE',
					last_heartbeat=excluded.last_heartbeat
				WHERE worker_instances.worker_id=excluded.worker_id;`,
				bootID, workerID, hostname, pid, version, now, now,
			)
			if err != nil {
				return err
			}
		}

		return tx.Commit()
	})
	return err
}

func (s *Store) loadWorkerInstances(ctx context.Context, workers []Worker) error {
	if len(workers) == 0 {
		return nil
	}
	// Fetch all instances and group by worker
	rows, err := s.rdb.QueryContext(ctx, `SELECT boot_id, worker_id, hostname, pid, version, status, started_at, last_heartbeat FROM worker_instances ORDER BY last_heartbeat DESC`)
	if err != nil {
		return err
	}
	defer rows.Close()

	byWorker := make(map[string][]WorkerInstance)
	for rows.Next() {
		var inst WorkerInstance
		if err := rows.Scan(&inst.BootID, &inst.WorkerID, &inst.Hostname, &inst.PID, &inst.Version, &inst.Status, &inst.StartedAt, &inst.LastHeartbeat); err != nil {
			return err
		}
		byWorker[inst.WorkerID] = append(byWorker[inst.WorkerID], inst)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for i := range workers {
		workers[i].Instances = byWorker[workers[i].ID]
	}
	return nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.rdb.QueryContext(ctx, `SELECT id, addr, status, last_heartbeat, capabilities_json FROM workers ORDER BY last_heartbeat DESC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		var w Worker
		var capStr string
		if err := rows.Scan(&w.ID, &w.Addr, &w.Status, &w.LastHeartbeat, &capStr); err != nil {
			return nil, err
		}
		w.Capabilities = json.RawMessage(capStr)
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := s.loadWorkerInstances(ctx, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) ListWorkersActive(ctx context.Context, activeSince string) ([]Worker, error) {
	if strings.TrimSpace(activeSince) == "" {
		return s.ListWorkers(ctx)
	}
	rows, err := s.rdb.QueryContext(ctx, `SELECT id, addr, status, last_heartbeat, capabilities_json FROM workers WHERE last_heartbeat >= ? ORDER BY last_heartbeat DESC;`, normalizeTimestamp(activeSince))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		var w Worker
		var capStr string
		if err := rows.Scan(&w.ID, &w.Addr, &w.Status, &w.LastHeartbeat, &capStr); err != nil {
			return nil, err
		}
		w.Capabilities = json.RawMessage(capStr)
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := s.loadWorkerInstances(ctx, out); err != nil {
		return nil, err
	}
	return out, nil
}

// TouchWorkerHeartbeat updates only liveness fields, preserving addr/capabilities.
func (s *Store) TouchWorkerHeartbeat(ctx context.Context, bootID, workerID string) error {
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		now := nowUTC()

		_, err = tx.ExecContext(ctx, `
			INSERT INTO workers(id, addr, status, last_heartbeat, capabilities_json)
			VALUES (?, '', 'ACTIVE', ?, '{}')
			ON CONFLICT(id) DO UPDATE SET
				status='ACTIVE',
				last_heartbeat=excluded.last_heartbeat;`, workerID, now)
		if err != nil {
			return err
		}

		if bootID != "" {
			_, err = tx.ExecContext(ctx, `
				UPDATE worker_instances SET status='ACTIVE', last_heartbeat=? WHERE boot_id=? AND worker_id=?;`,
				now, bootID, workerID)
			if err != nil {
				return err
			}
		}

		return tx.Commit()
	})
	return err
}
