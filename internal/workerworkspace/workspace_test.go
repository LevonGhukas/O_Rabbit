package workerworkspace

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type fixedAttemptResolver struct {
	status AttemptStatus
	err    error
}

func (r fixedAttemptResolver) ResolveAttemptStatus(context.Context, Marker) (AttemptStatus, error) {
	return r.status, r.err
}

func testManager(t *testing.T, mutate func(*Config)) *Manager {
	t.Helper()
	base := t.TempDir()
	cfg := Config{
		Root:                filepath.Join(base, "orabbit-managed"),
		RepositoryRoot:      filepath.Join(base, "repo"),
		UnlockedGrace:       time.Minute,
		MaxOfflineRetention: 2 * time.Minute,
		MaxEntries:          100,
		MaxBytes:            1 << 20,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	manager, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	return manager
}

func TestManagedRootValidationAndExclusiveScavenger(t *testing.T) {
	home, _ := os.UserHomeDir()
	repo := t.TempDir()
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	link := filepath.Join(t.TempDir(), "root-link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	for name, root := range map[string]string{"filesystem": "/", "home": home, "repository": repo, "repository-child": filepath.Join(repo, "tmp"), "regular-file": file, "symlink": link} {
		t.Run(name, func(t *testing.T) {
			manager, err := Open(Config{Root: root, RepositoryRoot: repo})
			if manager != nil {
				_ = manager.Close()
			}
			if err == nil {
				t.Fatalf("unsafe root %q accepted", root)
			}
		})
	}
	manager := testManager(t, nil)
	info, err := os.Stat(manager.Root())
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("root mode=%v err=%v", info.Mode(), err)
	}
	second, err := Open(manager.cfg)
	if second != nil {
		_ = second.Close()
	}
	if !errors.Is(err, ErrRootLockHeld) {
		t.Fatalf("second root lock err=%v", err)
	}
}

func TestWorkspaceMarkerIsolationLockAndGracefulCleanup(t *testing.T) {
	manager := testManager(t, nil)
	workspace, err := manager.Create("../run/../../escape", "task/one", "attempt/one", 2, "worker", "instance")
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureBelow(manager.workspaces, workspace.Path); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(workspace.Path, ".workspace.json"))
	if err != nil {
		t.Fatal(err)
	}
	var marker Marker
	if err := json.Unmarshal(body, &marker); err != nil || marker.AttemptID != "attempt/one" || marker.State != "ACTIVE" {
		t.Fatalf("marker=%+v err=%v", marker, err)
	}
	text := string(body)
	if strings.Contains(text, "fencing") || strings.Contains(text, "secret") {
		t.Fatalf("marker contains secret material: %s", text)
	}
	second, err := manager.Create("../run/../../escape", "task/one", "attempt/one", 2, "worker", "other-instance")
	if second != nil {
		_ = second.AbandonForTest()
	}
	if err == nil {
		t.Fatal("active workspace lock was reacquired")
	}
	if err := os.WriteFile(filepath.Join(workspace.Path, "part.parquet"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Cleanup("COMPLETED"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(workspace.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace remains after cleanup: %v", err)
	}
}

func TestScavengerPreservesLockedAndRecentThenDeletesOld(t *testing.T) {
	manager := testManager(t, nil)
	base := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	manager.now = func() time.Time { return base }
	workspace, err := manager.Create("run", "task", "attempt", 1, "worker", "instance")
	if err != nil {
		t.Fatal(err)
	}
	manager.now = func() time.Time { return base.Add(24 * time.Hour) }
	status, err := manager.Scan(context.Background())
	if err != nil || status.ActiveWorkspaceCount != 1 {
		t.Fatalf("locked status=%+v err=%v", status, err)
	}
	if err := workspace.AbandonForTest(); err != nil {
		t.Fatal(err)
	}
	manager.now = func() time.Time { return base.Add(30 * time.Second) }
	if _, err := manager.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(workspace.Path); err != nil {
		t.Fatalf("recent workspace removed: %v", err)
	}
	manager.now = func() time.Time { return base.Add(3 * time.Minute) }
	if _, err := manager.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(workspace.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale workspace not removed: %v", err)
	}
}

func TestMasterAttemptClassificationControlsCleanup(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  AttemptStatus
		deleted bool
		class   string
	}{
		{name: "active", status: AttemptActive, class: "MASTER_SAYS_ACTIVE"},
		{name: "expired", status: AttemptExpired, deleted: true, class: "MASTER_SAYS_TERMINAL"},
		{name: "canceled", status: AttemptCanceled, deleted: true, class: "MASTER_SAYS_TERMINAL"},
		{name: "unknown", status: AttemptUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := testManager(t, nil)
			base := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
			manager.now = func() time.Time { return base }
			manager.SetAttemptStatusResolver(fixedAttemptResolver{status: tc.status})
			workspace, err := manager.Create("run", "task", "attempt-"+tc.name, 1, "worker", "instance")
			if err != nil {
				t.Fatal(err)
			}
			_ = workspace.AbandonForTest()
			manager.now = func() time.Time { return base.Add(90 * time.Second) }
			status, err := manager.Scan(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_, statErr := os.Stat(workspace.Path)
			if tc.deleted && !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("terminal workspace remains: %v", statErr)
			}
			if !tc.deleted && statErr != nil {
				t.Fatalf("workspace unexpectedly removed: %v", statErr)
			}
			if tc.class != "" && status.Classifications[tc.class] == 0 {
				t.Fatalf("classifications=%v", status.Classifications)
			}
		})
	}
}

func TestSafeDeletionDoesNotFollowSymlinks(t *testing.T) {
	manager := testManager(t, nil)
	workspace, err := manager.Create("run", "task", "attempt", 1, "worker", "instance")
	if err != nil {
		t.Fatal(err)
	}
	external := filepath.Join(t.TempDir(), "external")
	if err := os.MkdirAll(external, 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(external, "sentinel")
	if err := os.WriteFile(sentinel, []byte("preserve"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(workspace.Path, "outside")); err != nil {
		t.Fatal(err)
	}
	if err := workspace.Cleanup("FAILED"); err != nil {
		t.Fatal(err)
	}
	if body, err := os.ReadFile(sentinel); err != nil || string(body) != "preserve" {
		t.Fatalf("external sentinel changed body=%q err=%v", body, err)
	}
}

func TestMalformedAndIdentityConflictAreQuarantined(t *testing.T) {
	manager := testManager(t, nil)
	malformed := filepath.Join(manager.workspaces, "malformed")
	if err := os.MkdirAll(malformed, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(malformed, ".workspace.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(manager.quarantine)
	if err != nil || len(entries) == 0 {
		t.Fatalf("quarantine entries=%d err=%v", len(entries), err)
	}
}

func TestUnexpectedFIFOIsNeverTraversedOrDeletedAsRegularData(t *testing.T) {
	manager := testManager(t, nil)
	workspace, err := manager.Create("run", "task", "fifo-attempt", 1, "worker", "instance")
	if err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(workspace.Path, "unexpected.fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("fifo unsupported: %v", err)
	}
	if err := workspace.Cleanup("FAILED"); err == nil {
		t.Fatal("unsupported FIFO was silently deleted")
	}
	if _, err := manager.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(manager.quarantine)
	if err != nil || len(entries) == 0 {
		t.Fatalf("unsafe trash not quarantined entries=%d err=%v", len(entries), err)
	}
}

func TestTrashRecoveryDryRunBoundsAndCapacity(t *testing.T) {
	manager := testManager(t, func(cfg *Config) {
		cfg.MaxEntries = 1
		cfg.MinFreeBytes = ^uint64(0)
	})
	trash := filepath.Join(manager.trash, "recognized")
	if err := os.MkdirAll(trash, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(trash, "part"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeMarker(trash, Marker{FormatVersion: 1, Application: "orabbit", RunID: "run", TaskID: "task", AttemptID: "attempt", AttemptNumber: 1, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), LastActivityAt: time.Now().UTC().Format(time.RFC3339Nano), State: "CLEANUP_PENDING"}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(manager.trash, "second"), 0o700); err != nil {
		t.Fatal(err)
	}
	status, err := manager.Scan(context.Background())
	if err != nil || status.LastScanResult != "bounded" {
		t.Fatalf("status=%+v err=%v", status, err)
	}
	if _, err := os.Stat(trash); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("trash not resumed: %v", err)
	}
	if capacity, err := manager.CapacityReady(); err == nil || capacity.CapacityReady {
		t.Fatalf("capacity=%+v err=%v", capacity, err)
	}
	statusBody, err := os.ReadFile(filepath.Join(manager.state, "status.json"))
	if err != nil || strings.Contains(string(statusBody), "secret") || !strings.Contains(string(statusBody), `"capacity_ready":false`) {
		t.Fatalf("status=%s err=%v", statusBody, err)
	}

	dry := testManager(t, func(cfg *Config) { cfg.DryRun = true })
	workspace, err := dry.Create("run", "task", "attempt", 1, "worker", "instance")
	if err != nil {
		t.Fatal(err)
	}
	if err := workspace.Cleanup("COMPLETED"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(workspace.Path); err != nil {
		t.Fatalf("dry-run removed workspace: %v", err)
	}
}

func TestScavengerHonorsByteLimit(t *testing.T) {
	manager := testManager(t, func(cfg *Config) { cfg.MaxBytes = 4 })
	base := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	manager.now = func() time.Time { return base }
	workspace, err := manager.Create("run", "task", "large-attempt", 1, "worker", "instance")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace.Path, "large.parquet"), []byte("larger-than-limit"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := workspace.AbandonForTest(); err != nil {
		t.Fatal(err)
	}
	manager.now = func() time.Time { return base.Add(3 * time.Minute) }
	if _, err := manager.Scan(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(workspace.Path); err != nil {
		t.Fatalf("oversized workspace deleted: %v", err)
	}
}
