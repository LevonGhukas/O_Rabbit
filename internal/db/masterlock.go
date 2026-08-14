package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

var ErrMasterLockHeld = errors.New("master lock is already held")

type MasterProcessLock struct {
	file         *os.File
	databaseFile *os.File
	DatabasePath string
	Identity     string
}

func CanonicalDatabasePath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("sqlite database path is empty")
	}
	if path == ":memory:" || strings.Contains(path, "mode=memory") {
		return "", errors.New("in-memory sqlite requires explicit test leadership mode")
	}
	if strings.HasPrefix(path, "file:") {
		return "", errors.New("sqlite URI paths are unsupported for master leadership; use a filesystem path")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if _, err := os.Lstat(absolute); err == nil {
		resolved, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			return "", fmt.Errorf("resolve sqlite file: %w", err)
		}
		info, err := os.Stat(resolved)
		if err != nil {
			return "", fmt.Errorf("stat sqlite file: %w", err)
		}
		if !info.Mode().IsRegular() {
			return "", fmt.Errorf("sqlite database path is not a regular file")
		}
		return resolved, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect sqlite file: %w", err)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", fmt.Errorf("resolve sqlite parent directory: %w", err)
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

func AcquireMasterProcessLock(databasePath, instanceID string) (*MasterProcessLock, error) {
	canonical, err := CanonicalDatabasePath(databasePath)
	if err != nil {
		return nil, err
	}
	databaseFile, err := os.OpenFile(canonical, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database %s: %w", filepath.Base(canonical), err)
	}
	if err := syscall.Flock(int(databaseFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = databaseFile.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w for database %s", ErrMasterLockHeld, filepath.Base(canonical))
		}
		return nil, fmt.Errorf("acquire sqlite file lock: %w", err)
	}
	info, err := databaseFile.Stat()
	if err != nil {
		_ = syscall.Flock(int(databaseFile.Fd()), syscall.LOCK_UN)
		_ = databaseFile.Close()
		return nil, fmt.Errorf("stat sqlite database: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		_ = syscall.Flock(int(databaseFile.Fd()), syscall.LOCK_UN)
		_ = databaseFile.Close()
		return nil, errors.New("sqlite database file identity is unavailable")
	}
	identity := fmt.Sprintf("sqlite-%d-%d", uint64(stat.Dev), uint64(stat.Ino))
	lockPath := canonical + ".master.lock"
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		_ = syscall.Flock(int(databaseFile.Fd()), syscall.LOCK_UN)
		_ = databaseFile.Close()
		return nil, fmt.Errorf("open master lock %s: %w", filepath.Base(lockPath), err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		_ = syscall.Flock(int(databaseFile.Fd()), syscall.LOCK_UN)
		_ = databaseFile.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w for database %s", ErrMasterLockHeld, filepath.Base(canonical))
		}
		return nil, fmt.Errorf("acquire master lock: %w", err)
	}
	_ = f.Truncate(0)
	_, _ = f.WriteString(fmt.Sprintf("pid=%d instance_id=%s\n", os.Getpid(), instanceID))
	_ = f.Sync()
	return &MasterProcessLock{file: f, databaseFile: databaseFile, DatabasePath: canonical, Identity: identity}, nil
}

func (l *MasterProcessLock) Close() error {
	if l == nil {
		return nil
	}
	var errs []error
	if l.file != nil {
		if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
			errs = append(errs, err)
		}
		if err := l.file.Close(); err != nil {
			errs = append(errs, err)
		}
		l.file = nil
	}
	if l.databaseFile != nil {
		if err := syscall.Flock(int(l.databaseFile.Fd()), syscall.LOCK_UN); err != nil {
			errs = append(errs, err)
		}
		if err := l.databaseFile.Close(); err != nil {
			errs = append(errs, err)
		}
		l.databaseFile = nil
	}
	return errors.Join(errs...)
}
