package workerworkspace

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type ErrorClass string

const (
	ErrTempRootUnsafe        ErrorClass = "TEMP_ROOT_UNSAFE"
	ErrTempRootUnavailable   ErrorClass = "TEMP_ROOT_UNAVAILABLE"
	ErrWorkspaceCreateFailed ErrorClass = "TEMP_WORKSPACE_CREATE_FAILED"
	ErrMarkerInvalid         ErrorClass = "TEMP_MARKER_INVALID"
	ErrLockActive            ErrorClass = "TEMP_LOCK_ACTIVE"
	ErrLockUnsupported       ErrorClass = "TEMP_LOCK_UNSUPPORTED"
	ErrStatusUnavailable     ErrorClass = "TEMP_STATUS_UNAVAILABLE"
	ErrWorkspaceAmbiguous    ErrorClass = "TEMP_WORKSPACE_AMBIGUOUS"
	ErrRenameFailed          ErrorClass = "TEMP_RENAME_FAILED"
	ErrDeleteFailed          ErrorClass = "TEMP_DELETE_FAILED"
	ErrPathEscape            ErrorClass = "TEMP_PATH_ESCAPE"
	ErrSymlinkConflict       ErrorClass = "TEMP_SYMLINK_CONFLICT"
	ErrCapacityLow           ErrorClass = "TEMP_CAPACITY_LOW"
	ErrCleanupExhausted      ErrorClass = "TEMP_CLEANUP_EXHAUSTED"
)

var ErrRootLockHeld = errors.New("worker temp root scavenger lock is held")

type ClassifiedError struct {
	Class ErrorClass
	Err   error
}

func (e *ClassifiedError) Error() string { return string(e.Class) + ": " + e.Err.Error() }
func (e *ClassifiedError) Unwrap() error { return e.Err }

type Config struct {
	Root                string
	RepositoryRoot      string
	UnlockedGrace       time.Duration
	MaxOfflineRetention time.Duration
	MaxEntries          int
	MaxBytes            int64
	MinFreeBytes        uint64
	MaxManagedBytes     int64
	DryRun              bool
}

type Marker struct {
	FormatVersion    int    `json:"format_version"`
	Application      string `json:"application"`
	RunID            string `json:"run_id"`
	TaskID           string `json:"task_id"`
	AttemptID        string `json:"attempt_id"`
	AttemptNumber    int32  `json:"attempt_number"`
	WorkerID         string `json:"worker_id"`
	WorkerInstanceID string `json:"worker_instance_id"`
	CreatedAt        string `json:"created_at"`
	LastActivityAt   string `json:"last_activity_at"`
	State            string `json:"state"`
}

type Status struct {
	ManagedTempRoot           string         `json:"managed_temp_root"`
	ManagedBytes              int64          `json:"managed_bytes"`
	WorkspaceCount            int            `json:"workspace_count"`
	ActiveWorkspaceCount      int            `json:"active_workspace_count"`
	QuarantinedWorkspaceCount int            `json:"quarantined_workspace_count"`
	TrashPendingCount         int            `json:"trash_pending_count"`
	LastScanAt                string         `json:"last_scan_at,omitempty"`
	LastScanResult            string         `json:"last_scan_result,omitempty"`
	BytesReclaimed            int64          `json:"bytes_reclaimed"`
	CleanupFailures           int            `json:"cleanup_failures"`
	DiskFreeBytes             uint64         `json:"disk_free_bytes"`
	CapacityReady             bool           `json:"capacity_ready"`
	Classifications           map[string]int `json:"classifications,omitempty"`
}

type AttemptStatus string

const (
	AttemptActive     AttemptStatus = "ACTIVE"
	AttemptExpired    AttemptStatus = "EXPIRED"
	AttemptSuperseded AttemptStatus = "SUPERSEDED"
	AttemptCanceled   AttemptStatus = "CANCELED"
	AttemptFailed     AttemptStatus = "FAILED"
	AttemptSucceeded  AttemptStatus = "SUCCEEDED"
	AttemptUnknown    AttemptStatus = "UNKNOWN"
)

type AttemptStatusResolver interface {
	ResolveAttemptStatus(context.Context, Marker) (AttemptStatus, error)
}

type Manager struct {
	cfg        Config
	root       string
	workspaces string
	trash      string
	quarantine string
	state      string
	rootLock   *os.File
	mu         sync.Mutex
	status     Status
	now        func() time.Time
	resolver   AttemptStatusResolver
}

type Workspace struct {
	manager *Manager
	Path    string
	Marker  Marker
	lock    *os.File
	closed  bool
}

func DefaultRoot() string { return filepath.Join(os.TempDir(), "orabbit-worker") }

func (m *Manager) SetAttemptStatusResolver(resolver AttemptStatusResolver) { m.resolver = resolver }

func Open(cfg Config) (*Manager, error) {
	if cfg.UnlockedGrace <= 0 {
		cfg.UnlockedGrace = 30 * time.Minute
	}
	if cfg.MaxOfflineRetention <= cfg.UnlockedGrace {
		cfg.MaxOfflineRetention = 7 * 24 * time.Hour
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 100
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 10 << 30
	}
	root, err := validateRoot(cfg.Root, cfg.RepositoryRoot)
	if err != nil {
		return nil, err
	}
	m := &Manager{cfg: cfg, root: root, now: time.Now}
	m.workspaces = filepath.Join(root, "workspaces")
	m.trash = filepath.Join(root, "trash")
	m.quarantine = filepath.Join(root, "quarantine")
	m.state = filepath.Join(root, "worker-state")
	for _, dir := range []string{root, m.workspaces, m.trash, m.quarantine, m.state} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, &ClassifiedError{ErrTempRootUnavailable, err}
		}
		if err := rejectSymlink(dir); err != nil {
			return nil, err
		}
	}
	lockPath := filepath.Join(m.state, "scavenger.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, &ClassifiedError{ErrTempRootUnavailable, err}
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrRootLockHeld
		}
		return nil, &ClassifiedError{ErrLockUnsupported, err}
	}
	m.rootLock = lock
	m.status.ManagedTempRoot = root
	m.status.CapacityReady = true
	return m, nil
}

func validateRoot(raw, repositoryRoot string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		raw = DefaultRoot()
	}
	abs, err := filepath.Abs(raw)
	if err != nil {
		return "", &ClassifiedError{ErrTempRootUnsafe, err}
	}
	abs = filepath.Clean(abs)
	if abs == string(filepath.Separator) {
		return "", &ClassifiedError{ErrTempRootUnsafe, errors.New("root filesystem is not a valid worker temp root")}
	}
	home, _ := os.UserHomeDir()
	if samePath(abs, home) {
		return "", &ClassifiedError{ErrTempRootUnsafe, errors.New("home directory is not a valid worker temp root")}
	}
	if repositoryRoot != "" {
		repo, _ := filepath.Abs(repositoryRoot)
		if samePath(abs, repo) || pathWithin(repo, abs) {
			return "", &ClassifiedError{ErrTempRootUnsafe, errors.New("repository paths are not valid worker temp roots")}
		}
	}
	if info, err := os.Lstat(abs); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", &ClassifiedError{ErrTempRootUnsafe, errors.New("worker temp root must be a real directory")}
		}
		resolved, err := filepath.EvalSymlinks(abs)
		if err != nil {
			return "", &ClassifiedError{ErrTempRootUnsafe, errors.New("worker temp root cannot be resolved")}
		}
		abs = resolved
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", &ClassifiedError{ErrTempRootUnavailable, err}
	} else {
		parent := filepath.Dir(abs)
		resolved, err := filepath.EvalSymlinks(parent)
		if err != nil {
			return "", &ClassifiedError{ErrTempRootUnsafe, errors.New("worker temp parent cannot be resolved")}
		}
		abs = filepath.Join(resolved, filepath.Base(abs))
	}
	return abs, nil
}

func pathWithin(root, target string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(target))
	return err == nil && rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func samePath(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	aa, _ := filepath.Abs(a)
	bb, _ := filepath.Abs(b)
	return filepath.Clean(aa) == filepath.Clean(bb)
}

func rejectSymlink(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return &ClassifiedError{ErrTempRootUnavailable, err}
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return &ClassifiedError{ErrSymlinkConflict, fmt.Errorf("%s is a symlink", filepath.Base(path))}
	}
	return nil
}

func RandomInstanceID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func safeComponent(prefix, value string) string {
	sum := sha256.Sum256([]byte(value))
	return prefix + "-" + hex.EncodeToString(sum[:12])
}

func (m *Manager) Create(runID, taskID, attemptID string, attemptNumber int32, workerID, instanceID string) (*Workspace, error) {
	if m == nil || strings.TrimSpace(runID) == "" || strings.TrimSpace(taskID) == "" || strings.TrimSpace(attemptID) == "" {
		return nil, &ClassifiedError{ErrWorkspaceCreateFailed, errors.New("run, task, and attempt identities are required")}
	}
	path := filepath.Join(m.workspaces, safeComponent("run", runID), safeComponent("task", taskID), safeComponent("attempt", attemptID))
	if err := ensureBelow(m.workspaces, path); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, &ClassifiedError{ErrWorkspaceCreateFailed, err}
	}
	lockPath := filepath.Join(path, ".workspace.lock")
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, &ClassifiedError{ErrWorkspaceCreateFailed, err}
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, &ClassifiedError{ErrLockActive, err}
	}
	now := m.now().UTC().Format(time.RFC3339Nano)
	marker := Marker{1, "orabbit", runID, taskID, attemptID, attemptNumber, workerID, instanceID, now, now, "ACTIVE"}
	existing, readErr := readMarker(path)
	if readErr == nil && !sameMarkerIdentity(existing, marker) {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
		return nil, &ClassifiedError{ErrMarkerInvalid, errors.New("conflicting workspace marker")}
	}
	if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
		return nil, readErr
	}
	if err := writeMarker(path, marker); err != nil {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
		return nil, err
	}
	return &Workspace{manager: m, Path: path, Marker: marker, lock: lock}, nil
}

func sameMarkerIdentity(a, b Marker) bool {
	return a.Application == "orabbit" && a.FormatVersion == 1 && a.RunID == b.RunID && a.TaskID == b.TaskID && a.AttemptID == b.AttemptID && a.AttemptNumber == b.AttemptNumber
}

func writeMarker(path string, marker Marker) error {
	body, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	tmp, err := os.OpenFile(filepath.Join(path, ".workspace.json.tmp"), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return &ClassifiedError{ErrWorkspaceCreateFailed, err}
	}
	if _, err = tmp.Write(body); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), filepath.Join(path, ".workspace.json"))
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return &ClassifiedError{ErrWorkspaceCreateFailed, err}
	}
	return nil
}

func readMarker(path string) (Marker, error) {
	var marker Marker
	file := filepath.Join(path, ".workspace.json")
	info, err := os.Lstat(file)
	if err != nil {
		return marker, err
	}
	if !info.Mode().IsRegular() {
		return marker, &ClassifiedError{ErrMarkerInvalid, errors.New("marker is not a regular file")}
	}
	body, err := os.ReadFile(file)
	if err != nil {
		return marker, err
	}
	if err := json.Unmarshal(body, &marker); err != nil || marker.FormatVersion != 1 || marker.Application != "orabbit" || marker.RunID == "" || marker.TaskID == "" || marker.AttemptID == "" {
		return marker, &ClassifiedError{ErrMarkerInvalid, errors.New("invalid workspace marker")}
	}
	return marker, nil
}

func (w *Workspace) SetState(state string) error {
	if w == nil || w.closed {
		return errors.New("workspace is closed")
	}
	w.Marker.State = state
	w.Marker.LastActivityAt = w.manager.now().UTC().Format(time.RFC3339Nano)
	return writeMarker(w.Path, w.Marker)
}

func (w *Workspace) Cleanup(state string) error {
	if w == nil || w.closed {
		return nil
	}
	_ = w.SetState(state)
	_ = syscall.Flock(int(w.lock.Fd()), syscall.LOCK_UN)
	_ = w.lock.Close()
	w.lock = nil
	w.closed = true
	return w.manager.moveAndDelete(w.Path, "graceful")
}

func (w *Workspace) AbandonForTest() error {
	if w == nil || w.closed {
		return nil
	}
	_ = syscall.Flock(int(w.lock.Fd()), syscall.LOCK_UN)
	err := w.lock.Close()
	w.lock = nil
	w.closed = true
	return err
}

func (m *Manager) moveAndDelete(path, reason string) error {
	if err := ensureBelow(m.workspaces, path); err != nil {
		return err
	}
	leaf := filepath.Base(path) + "-" + safeComponent(reason, path) + "-" + fmt.Sprint(m.now().UnixNano())
	target := filepath.Join(m.trash, leaf)
	if err := ensureBelow(m.trash, target); err != nil {
		return err
	}
	if m.cfg.DryRun {
		return nil
	}
	if err := os.Rename(path, target); err != nil {
		return &ClassifiedError{ErrRenameFailed, err}
	}
	size, _ := treeSize(target, m.cfg.MaxBytes)
	if err := safeRemoveTree(m.root, target); err != nil {
		return &ClassifiedError{ErrDeleteFailed, err}
	}
	m.mu.Lock()
	m.status.BytesReclaimed += size
	m.mu.Unlock()
	return nil
}

func ensureBelow(root, target string) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(rootAbs, targetAbs)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return &ClassifiedError{ErrPathEscape, errors.New("path is not a child of managed root")}
	}
	return nil
}

func safeRemoveTree(root, target string) error {
	if err := ensureBelow(root, target); err != nil {
		return err
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return err
	}
	rootStat, ok := rootInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return &ClassifiedError{ErrLockUnsupported, errors.New("filesystem identity unavailable")}
	}
	var remove func(string) error
	remove = func(path string) error {
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Dev != rootStat.Dev {
			return &ClassifiedError{ErrWorkspaceAmbiguous, errors.New("filesystem boundary crossing rejected")}
		}
		mode := info.Mode()
		switch {
		case mode&os.ModeSymlink != 0:
			return os.Remove(path)
		case mode.IsRegular():
			return os.Remove(path)
		case mode.IsDir():
			entries, err := os.ReadDir(path)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				if err := remove(filepath.Join(path, entry.Name())); err != nil {
					return err
				}
			}
			return os.Remove(path)
		default:
			return &ClassifiedError{ErrWorkspaceAmbiguous, fmt.Errorf("unsupported file type %s", mode)}
		}
	}
	return remove(target)
}

func treeSize(path string, limit int64) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if entry.Type().IsRegular() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
			if limit > 0 && total > limit {
				return io.EOF
			}
		}
		return nil
	})
	if errors.Is(err, io.EOF) {
		return total, nil
	}
	return total, err
}

func tryWorkspaceLock(path string) (*os.File, bool, error) {
	lock, err := os.OpenFile(filepath.Join(path, ".workspace.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, false, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, true, nil
		}
		return nil, false, &ClassifiedError{ErrLockUnsupported, err}
	}
	return lock, false, nil
}

func (m *Manager) Scan(ctx context.Context) (Status, error) {
	now := m.now()
	m.mu.Lock()
	m.status.BytesReclaimed = 0
	m.status.WorkspaceCount = 0
	m.status.ActiveWorkspaceCount = 0
	m.status.TrashPendingCount = 0
	m.status.QuarantinedWorkspaceCount = 0
	m.status.Classifications = map[string]int{}
	m.mu.Unlock()
	result := "ok"
	failures := 0
	processed := 0
	for _, base := range []string{m.trash, m.workspaces} {
		entries, err := os.ReadDir(base)
		if err != nil {
			return m.Status(), err
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
		for _, entry := range entries {
			m.mu.Lock()
			reclaimed := m.status.BytesReclaimed
			m.mu.Unlock()
			if ctx.Err() != nil || processed >= m.cfg.MaxEntries || reclaimed >= m.cfg.MaxBytes {
				result = "bounded"
				break
			}
			processed++
			path := filepath.Join(base, entry.Name())
			if base == m.trash {
				m.mu.Lock()
				m.status.TrashPendingCount++
				m.mu.Unlock()
				if !entry.IsDir() {
					_ = m.quarantinePath(path, "trash-unowned")
					continue
				}
				if _, err := readMarker(path); err != nil {
					_ = m.quarantinePath(path, "trash-marker-invalid")
					continue
				}
				if !m.cfg.DryRun {
					size, sizeErr := treeSize(path, m.cfg.MaxBytes-reclaimed)
					if sizeErr != nil || size > m.cfg.MaxBytes-reclaimed {
						result = "bounded"
						continue
					}
					if err := safeRemoveTree(m.root, path); err != nil {
						failures++
						_ = m.quarantinePath(path, "trash-delete-unsafe")
					} else {
						m.mu.Lock()
						m.status.BytesReclaimed += size
						m.mu.Unlock()
					}
				}
				continue
			}
			m.scanWorkspaceTree(ctx, path, now, &processed, &failures)
		}
	}
	if entries, err := os.ReadDir(m.quarantine); err == nil {
		m.mu.Lock()
		m.status.QuarantinedWorkspaceCount = len(entries)
		m.mu.Unlock()
	}
	managed, _ := treeSize(m.root, 0)
	free, capacityReady := m.capacity(managed)
	m.mu.Lock()
	m.status.ManagedBytes = managed
	m.status.DiskFreeBytes = free
	m.status.CapacityReady = capacityReady
	m.status.LastScanAt = now.UTC().Format(time.RFC3339Nano)
	m.status.LastScanResult = result
	m.status.CleanupFailures += failures
	status := m.status
	m.mu.Unlock()
	_ = m.writeStatus(status)
	return status, nil
}

func (m *Manager) scanWorkspaceTree(ctx context.Context, path string, now time.Time, processed, failures *int) {
	if ctx.Err() != nil || *processed >= m.cfg.MaxEntries {
		return
	}
	info, err := os.Lstat(path)
	if err != nil {
		return
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		m.classify("UNOWNED")
		_ = m.quarantinePath(path, "unowned")
		return
	}
	marker, markerErr := readMarker(path)
	if markerErr == nil {
		expected := filepath.Join(m.workspaces, safeComponent("run", marker.RunID), safeComponent("task", marker.TaskID), safeComponent("attempt", marker.AttemptID))
		if !samePath(path, expected) {
			m.classify("IDENTITY_CONFLICT")
			_ = m.quarantinePath(path, "identity-conflict")
			return
		}
		m.mu.Lock()
		m.status.WorkspaceCount++
		m.mu.Unlock()
		lock, active, lockErr := tryWorkspaceLock(path)
		if active {
			m.classify("ACTIVE_LOCKED")
			m.mu.Lock()
			m.status.ActiveWorkspaceCount++
			m.mu.Unlock()
			return
		}
		if lockErr != nil {
			m.classify("MALFORMED")
			_ = m.quarantinePath(path, "lock-unsupported")
			return
		}
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
		last, err := time.Parse(time.RFC3339Nano, marker.LastActivityAt)
		if err != nil {
			m.classify("MALFORMED")
			_ = m.quarantinePath(path, "marker-time")
			return
		}
		age := now.Sub(last)
		if age < m.cfg.UnlockedGrace {
			m.classify("RECENT_UNLOCKED")
			return
		}
		status := AttemptUnknown
		if m.resolver != nil {
			resolved, err := m.resolver.ResolveAttemptStatus(ctx, marker)
			if err != nil {
				m.classify("MASTER_UNAVAILABLE")
			} else {
				status = resolved
			}
		} else {
			m.classify("MASTER_UNAVAILABLE")
		}
		switch status {
		case AttemptActive:
			m.classify("MASTER_SAYS_ACTIVE")
			return
		case AttemptExpired, AttemptSuperseded, AttemptCanceled, AttemptFailed, AttemptSucceeded:
			m.classify("MASTER_SAYS_TERMINAL")
		case AttemptUnknown:
			if age < m.cfg.MaxOfflineRetention {
				return
			}
			m.classify("STALE_CONFIRMED")
		default:
			m.classify("MASTER_UNAVAILABLE")
			if age < m.cfg.MaxOfflineRetention {
				return
			}
		}
		m.mu.Lock()
		reclaimed := m.status.BytesReclaimed
		m.mu.Unlock()
		remaining := m.cfg.MaxBytes - reclaimed
		size, sizeErr := treeSize(path, remaining)
		if sizeErr != nil || remaining <= 0 || size > remaining {
			return
		}
		if err := m.moveAndDelete(path, "offline-retention"); err != nil {
			*failures++
		}
		return
	}
	if _, err := os.Lstat(filepath.Join(path, ".workspace.json")); err == nil {
		m.classify("MALFORMED")
		_ = m.quarantinePath(path, "marker-invalid")
		return
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return
	}
	if len(entries) == 0 {
		base := filepath.Base(path)
		if strings.HasPrefix(base, "run-") || strings.HasPrefix(base, "task-") {
			_ = os.Remove(path)
		} else {
			m.classify("UNOWNED")
			_ = m.quarantinePath(path, "empty-unowned")
		}
		return
	}
	for _, child := range entries {
		if *processed >= m.cfg.MaxEntries {
			return
		}
		*processed++
		m.scanWorkspaceTree(ctx, filepath.Join(path, child.Name()), now, processed, failures)
	}
}

func (m *Manager) classify(classification string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.status.Classifications == nil {
		m.status.Classifications = map[string]int{}
	}
	m.status.Classifications[classification]++
}

func (m *Manager) quarantinePath(path, reason string) error {
	if err := ensureBelow(m.root, path); err != nil {
		return err
	}
	target := filepath.Join(m.quarantine, filepath.Base(path)+"-"+safeComponent(reason, path))
	if samePath(filepath.Dir(path), m.quarantine) || m.cfg.DryRun {
		return nil
	}
	if err := os.Rename(path, target); err != nil {
		return err
	}
	m.mu.Lock()
	m.status.QuarantinedWorkspaceCount++
	m.mu.Unlock()
	return nil
}

func (m *Manager) capacity(managed int64) (uint64, bool) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(m.root, &stat); err != nil {
		return 0, false
	}
	free := uint64(stat.Bavail) * uint64(stat.Bsize)
	ready := free >= m.cfg.MinFreeBytes
	if m.cfg.MaxManagedBytes > 0 && managed >= m.cfg.MaxManagedBytes {
		ready = false
	}
	return free, ready
}

func (m *Manager) CapacityReady() (Status, error) {
	managed, err := treeSize(m.root, 0)
	if err != nil {
		return m.Status(), err
	}
	free, ready := m.capacity(managed)
	m.mu.Lock()
	m.status.ManagedBytes, m.status.DiskFreeBytes, m.status.CapacityReady = managed, free, ready
	status := m.status
	m.mu.Unlock()
	_ = m.writeStatus(status)
	if !ready {
		return status, &ClassifiedError{ErrCapacityLow, errors.New("managed temp capacity threshold reached")}
	}
	return status, nil
}

func (m *Manager) writeStatus(status Status) error {
	body, err := json.Marshal(status)
	if err != nil {
		return err
	}
	tmp := filepath.Join(m.state, "status.json.tmp")
	final := filepath.Join(m.state, "status.json")
	if err := os.WriteFile(tmp, body, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}

func (m *Manager) Status() Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.status
}

func (m *Manager) Root() string { return m.root }

func (m *Manager) Close() error {
	if m == nil || m.rootLock == nil {
		return nil
	}
	err := syscall.Flock(int(m.rootLock.Fd()), syscall.LOCK_UN)
	closeErr := m.rootLock.Close()
	m.rootLock = nil
	if err != nil {
		return err
	}
	return closeErr
}
