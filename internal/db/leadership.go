package db

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var (
	ErrLeadershipHeld = errors.New("master leadership lease is held")
	ErrNotLeader      = errors.New("master is not the current leader")
)

type Leadership struct {
	InstanceID       string `json:"instance_id"`
	State            string `json:"leadership_state"`
	Epoch            int64  `json:"leadership_epoch"`
	LeaseDeadlineMS  int64  `json:"lease_deadline_unix_ms"`
	LastRenewalMS    int64  `json:"last_successful_renewal_unix_ms"`
	DatabaseIdentity string `json:"database_path_identity,omitempty"`
	Ready            bool   `json:"ready"`
}

type LeaderLease struct {
	InstanceID, LeaseDeadline string
	Epoch                     int64
	LeaseDeadlineMS           int64
}

func NewMasterInstanceID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "master-" + hex.EncodeToString(b[:]), nil
}

func sqliteNowMS() string {
	return `CAST(unixepoch('subsec')*1000 AS INTEGER)`
}

func (s *Store) AcquireLeadership(ctx context.Context, instanceID string, lease time.Duration, metadata map[string]any) (LeaderLease, error) {
	if strings.TrimSpace(instanceID) == "" || lease <= 0 {
		return LeaderLease{}, errors.New("instance id and positive leadership lease are required")
	}
	body, _ := json.Marshal(metadata)
	leaseMS := lease.Milliseconds()
	query := fmt.Sprintf(`UPDATE master_leadership SET instance_id=?,epoch=epoch+1,status='ACTIVE',lease_deadline_ms=%s+?,acquired_at_ms=%s,renewed_at_ms=%s,released_at_ms=0,metadata_json=? WHERE leadership_name='master' AND (status<>'ACTIVE' OR lease_deadline_ms<=%s) RETURNING epoch,lease_deadline_ms`, sqliteNowMS(), sqliteNowMS(), sqliteNowMS(), sqliteNowMS())
	var out LeaderLease
	out.InstanceID = instanceID
	if err := s.db.QueryRowContext(ctx, query, instanceID, leaseMS, string(body)).Scan(&out.Epoch, &out.LeaseDeadlineMS); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			var heldBy string
			var deadline int64
			_ = s.db.QueryRowContext(ctx, `SELECT instance_id,lease_deadline_ms FROM master_leadership WHERE leadership_name='master'`).Scan(&heldBy, &deadline)
			return LeaderLease{}, fmt.Errorf("%w by %s until %d", ErrLeadershipHeld, heldBy, deadline)
		}
		return LeaderLease{}, err
	}
	out.LeaseDeadline = time.UnixMilli(out.LeaseDeadlineMS).UTC().Format(time.RFC3339Nano)
	event := "MASTER_LEADERSHIP_ACQUIRED"
	if out.Epoch > 1 {
		event = "MASTER_TAKEOVER"
	}
	_, _ = s.db.ExecContext(ctx, `INSERT OR IGNORE INTO master_leadership_history(id,instance_id,epoch,event_type,occurred_at_ms,lease_deadline_ms,metadata_json) VALUES(?,?,?,?,`+sqliteNowMS()+`,?,?)`,
		fmt.Sprintf("leadership-%s-%d", event, out.Epoch), instanceID, out.Epoch, event, out.LeaseDeadlineMS, string(body))
	return out, nil
}

func (s *Store) RenewLeadership(ctx context.Context, instanceID string, epoch int64, lease time.Duration) (int64, error) {
	query := fmt.Sprintf(`UPDATE master_leadership SET lease_deadline_ms=%s+?,renewed_at_ms=%s WHERE leadership_name='master' AND status='ACTIVE' AND instance_id=? AND epoch=? AND lease_deadline_ms>%s RETURNING lease_deadline_ms`, sqliteNowMS(), sqliteNowMS(), sqliteNowMS())
	var deadline int64
	if err := s.db.QueryRowContext(ctx, query, lease.Milliseconds(), instanceID, epoch).Scan(&deadline); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrNotLeader
		}
		return 0, err
	}
	return deadline, nil
}

func (s *Store) ReleaseLeadership(ctx context.Context, instanceID string, epoch int64) (bool, error) {
	query := fmt.Sprintf(`UPDATE master_leadership SET status='RELEASED',lease_deadline_ms=0,released_at_ms=%s WHERE leadership_name='master' AND status='ACTIVE' AND instance_id=? AND epoch=?`, sqliteNowMS())
	res, err := s.db.ExecContext(ctx, query, instanceID, epoch)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n == 1 {
		_, _ = s.db.ExecContext(ctx, `INSERT OR IGNORE INTO master_leadership_history(id,instance_id,epoch,event_type,occurred_at_ms) VALUES(?,?,?,'MASTER_LEADERSHIP_RELEASED',`+sqliteNowMS()+`)`, fmt.Sprintf("leadership-release-%d", epoch), instanceID, epoch)
	}
	return n == 1, nil
}

func (s *Store) AssertLeadership(ctx context.Context, instanceID string, epoch int64) error {
	query := fmt.Sprintf(`SELECT 1 FROM master_leadership WHERE leadership_name='master' AND status='ACTIVE' AND instance_id=? AND epoch=? AND lease_deadline_ms>%s`, sqliteNowMS())
	var one int
	if err := s.db.QueryRowContext(ctx, query, instanceID, epoch).Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotLeader
		}
		return err
	}
	return nil
}

func (s *Store) RecordLeadershipEvent(ctx context.Context, instanceID string, epoch int64, eventType string, metadata map[string]any) {
	body, _ := json.Marshal(metadata)
	id := fmt.Sprintf("leadership-%s-%s-%d", strings.ToLower(eventType), instanceID, epoch)
	_, _ = s.db.ExecContext(ctx, `INSERT OR IGNORE INTO master_leadership_history(id,instance_id,epoch,event_type,occurred_at_ms,metadata_json) VALUES(?,?,?,?,`+sqliteNowMS()+`,?)`, id, instanceID, epoch, eventType, string(body))
}

var fencedTables = []string{
	"connections", "jobs", "runs", "tasks", "events", "hwm", "workers", "audit_log",
	"servers", "server_credentials", "command_executions", "deployments", "config_versions",
	"task_attempts", "task_artifacts", "iceberg_registrations", "iceberg_registration_attempts",
	"iceberg_reconciliation_attempts",
	"multipart_uploads",
	"canceled_object_candidates", "canceled_object_cleanup_attempts",
}

func (s *Store) ActivateLeadershipFence(ctx context.Context, instanceID string, epoch int64) error {
	if err := s.AssertLeadership(ctx, instanceID, epoch); err != nil {
		return err
	}
	quotedID := strings.ReplaceAll(instanceID, "'", "''")
	for _, table := range fencedTables {
		for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			name := strings.ToLower("leader_fence_" + table + "_" + operation)
			stmt := fmt.Sprintf(`CREATE TEMP TRIGGER IF NOT EXISTS %s BEFORE %s ON main.%s BEGIN SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM master_leadership WHERE leadership_name='master' AND status='ACTIVE' AND instance_id='%s' AND epoch=%d AND lease_deadline_ms>%s) THEN RAISE(ABORT,'STALE_MASTER_MUTATION_REJECTED') END; END`, name, operation, table, quotedID, epoch, sqliteNowMS())
			if _, err := s.db.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
	}
	return nil
}

type LeadershipController struct {
	store      *Store
	instanceID string
	epoch      int64
	lease      time.Duration
	interval   time.Duration
	identity   string
	cancel     context.CancelFunc
	done       chan struct{}
	workCtx    context.Context
	mu         sync.RWMutex
	status     Leadership
}

func NewLeadershipController(store *Store, lease LeaderLease, duration, interval time.Duration, identity string) (*LeadershipController, error) {
	if interval <= 0 || duration <= interval {
		return nil, errors.New("leadership renewal interval must be positive and shorter than lease duration")
	}
	c := &LeadershipController{store: store, instanceID: lease.InstanceID, epoch: lease.Epoch, lease: duration, interval: interval, identity: identity, done: make(chan struct{})}
	c.status = Leadership{InstanceID: lease.InstanceID, State: "STARTING", Epoch: lease.Epoch, LeaseDeadlineMS: lease.LeaseDeadlineMS, DatabaseIdentity: identity}
	return c, nil
}

func (c *LeadershipController) SetReady(ready bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.status.Ready = ready
	if ready {
		c.status.State = "LEADER"
	}
}

func (c *LeadershipController) Status() Leadership {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

func (c *LeadershipController) Assert(ctx context.Context) error {
	st := c.Status()
	if !st.Ready || st.State != "LEADER" {
		return ErrNotLeader
	}
	return c.store.AssertLeadership(ctx, c.instanceID, c.epoch)
}

func (c *LeadershipController) Start(parent context.Context) context.Context {
	ctx, cancel := context.WithCancel(parent)
	c.cancel = cancel
	c.mu.Lock()
	c.workCtx = ctx
	c.mu.Unlock()
	go func() {
		defer close(c.done)
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				deadline, err := c.store.RenewLeadership(ctx, c.instanceID, c.epoch, c.lease)
				if err != nil {
					c.mu.Lock()
					c.status.State, c.status.Ready = "LOST", false
					c.mu.Unlock()
					c.store.RecordLeadershipEvent(context.Background(), c.instanceID, c.epoch, "MASTER_LEADERSHIP_LOST", map[string]any{"reason": "renewal_failed"})
					cancel()
					return
				}
				c.mu.Lock()
				c.status.LeaseDeadlineMS = deadline
				c.status.LastRenewalMS = time.Now().UnixMilli()
				c.mu.Unlock()
			}
		}
	}()
	return ctx
}

func (c *LeadershipController) WorkContext() context.Context {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.workCtx
}

func (c *LeadershipController) Stop(ctx context.Context) {
	c.SetReady(false)
	c.mu.Lock()
	c.status.State = "SHUTTING_DOWN"
	c.mu.Unlock()
	if c.cancel != nil {
		c.cancel()
		<-c.done
	}
	_, _ = c.store.ReleaseLeadership(ctx, c.instanceID, c.epoch)
}
