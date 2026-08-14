package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMasterProcessLockCanonicalSingleton(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "master.sqlite")
	first, err := AcquireMasterProcessLock(dbPath, "one")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireMasterProcessLock(filepath.Join(dir, ".", "master.sqlite"), "two"); !errors.Is(err, ErrMasterLockHeld) {
		t.Fatalf("second lock err=%v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := AcquireMasterProcessLock(dbPath, "two")
	if err != nil {
		t.Fatalf("stale lock file should be reusable: %v", err)
	}
	defer second.Close()
	if _, err := os.Stat(dbPath + ".master.lock"); err != nil {
		t.Fatal(err)
	}
	if _, err := CanonicalDatabasePath(":memory:"); err == nil {
		t.Fatal("in-memory database silently accepted")
	}
}

func TestMasterProcessLockResolvesSymlinkedParent(t *testing.T) {
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	if err := os.Mkdir(realDir, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(realDir, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	a, err := CanonicalDatabasePath(filepath.Join(realDir, "master.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := CanonicalDatabasePath(filepath.Join(link, "master.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("canonical paths differ: %q %q", a, b)
	}
}

func TestMasterProcessLockDatabaseFileIdentityAliases(t *testing.T) {
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	if err := os.Mkdir(realDir, 0o700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(realDir, "master.sqlite")
	if err := os.WriteFile(dbPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	parentLink := filepath.Join(root, "parent-link")
	finalLink := filepath.Join(root, "final-link.sqlite")
	hardLink := filepath.Join(root, "hard-link.sqlite")
	if err := os.Symlink(realDir, parentLink); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := os.Symlink(dbPath, finalLink); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := os.Link(dbPath, hardLink); err != nil {
		t.Skipf("hard link unavailable: %v", err)
	}
	previousDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previousDir) })

	aliases := []string{
		dbPath,
		filepath.Join("real", "master.sqlite"),
		filepath.Join(parentLink, "master.sqlite"),
		finalLink,
		hardLink,
	}
	first, err := AcquireMasterProcessLock(aliases[0], "first")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	for _, alias := range aliases[1:] {
		if _, err := AcquireMasterProcessLock(alias, "alias"); !errors.Is(err, ErrMasterLockHeld) {
			t.Fatalf("alias %q acquired independent lock: %v", alias, err)
		}
	}
	expectedDBPath, expectedErr := filepath.EvalSymlinks(dbPath)
	if canonical, err := CanonicalDatabasePath(finalLink); err != nil || expectedErr != nil || canonical != expectedDBPath {
		t.Fatalf("final symlink canonical=%q err=%v want=%q", canonical, err, expectedDBPath)
	}
}

func TestMasterProcessLockMissingDatabaseUsesStableIdentity(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "missing.sqlite")
	canonical, err := CanonicalDatabasePath(dbPath)
	expectedParent, parentErr := filepath.EvalSymlinks(filepath.Dir(dbPath))
	expected := filepath.Join(expectedParent, filepath.Base(dbPath))
	if err != nil || parentErr != nil || canonical != expected {
		t.Fatalf("canonical=%q err=%v", canonical, err)
	}
	first, err := AcquireMasterProcessLock(dbPath, "first")
	if err != nil {
		t.Fatal(err)
	}
	identity := first.Identity
	if identity == "" {
		t.Fatal("missing database did not receive file identity")
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("database was not created before locking: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := AcquireMasterProcessLock(dbPath, "second")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if second.Identity != identity {
		t.Fatalf("identity changed: %q -> %q", identity, second.Identity)
	}
}

func openSharedLeadershipStores(t *testing.T) (*Store, *Store) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "leadership.sqlite")
	a, err := Open(context.Background(), Config{Path: path}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Open(context.Background(), Config{Path: path}, nil)
	if err != nil {
		a.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		a.Close()
		b.Close()
	})
	return a, b
}

func TestDurableLeadershipAcquisitionTakeoverAndFencing(t *testing.T) {
	ctx := context.Background()
	a, b := openSharedLeadershipStores(t)
	first, err := a.AcquireLeadership(ctx, "first", time.Minute, nil)
	if err != nil || first.Epoch != 1 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	if _, err := b.AcquireLeadership(ctx, "second", time.Minute, nil); !errors.Is(err, ErrLeadershipHeld) {
		t.Fatalf("second acquire err=%v", err)
	}
	if _, err := a.RenewLeadership(ctx, "first", first.Epoch, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := a.RenewLeadership(ctx, "wrong", first.Epoch, time.Minute); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("wrong renew err=%v", err)
	}
	if _, err := b.db.ExecContext(ctx, `UPDATE master_leadership SET lease_deadline_ms=0 WHERE leadership_name='master'`); err != nil {
		t.Fatal(err)
	}
	second, err := b.AcquireLeadership(ctx, "second", time.Minute, nil)
	if err != nil || second.Epoch != first.Epoch+1 {
		t.Fatalf("takeover=%+v err=%v", second, err)
	}
	if _, err := a.RenewLeadership(ctx, "first", first.Epoch, time.Minute); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("stale renew err=%v", err)
	}
	if released, err := a.ReleaseLeadership(ctx, "first", first.Epoch); err != nil || released {
		t.Fatalf("stale release=%v err=%v", released, err)
	}
}

func TestConcurrentDurableLeadershipHasOneWinner(t *testing.T) {
	ctx := context.Background()
	a, b := openSharedLeadershipStores(t)
	var wg sync.WaitGroup
	var successes int
	var mu sync.Mutex
	for i, st := range []*Store{a, b} {
		wg.Add(1)
		go func(i int, st *Store) {
			defer wg.Done()
			if _, err := st.AcquireLeadership(ctx, "instance-"+string(rune('a'+i)), time.Minute, nil); err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}(i, st)
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("leadership successes=%d", successes)
	}
}

func TestSQLiteMutationFenceRejectsStaleEpoch(t *testing.T) {
	ctx := context.Background()
	a, b := openSharedLeadershipStores(t)
	first, _ := a.AcquireLeadership(ctx, "first", time.Minute, nil)
	if err := a.ActivateLeadershipFence(ctx, "first", first.Epoch); err != nil {
		t.Fatal(err)
	}
	if err := a.InsertEvent(ctx, Event{ID: "before", TS: nowUTC(), Level: "INFO", Message: "current"}); err != nil {
		t.Fatal(err)
	}
	_, _ = b.db.ExecContext(ctx, `UPDATE master_leadership SET lease_deadline_ms=0 WHERE leadership_name='master'`)
	second, err := b.AcquireLeadership(ctx, "second", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.ActivateLeadershipFence(ctx, "second", second.Epoch); err != nil {
		t.Fatal(err)
	}
	err = a.InsertEvent(ctx, Event{ID: "after", TS: nowUTC(), Level: "INFO", Message: "stale"})
	if err == nil || !strings.Contains(err.Error(), "STALE_MASTER_MUTATION_REJECTED") {
		t.Fatalf("stale mutation err=%v", err)
	}
	if err := b.InsertEvent(ctx, Event{ID: "new", TS: nowUTC(), Level: "INFO", Message: "leader"}); err != nil {
		t.Fatal(err)
	}
}

func TestLeadershipControllerFailsClosedAndCancelsWork(t *testing.T) {
	ctx := context.Background()
	a, b := openSharedLeadershipStores(t)
	first, err := a.AcquireLeadership(ctx, "first", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewLeadershipController(a, first, time.Minute, 5*time.Millisecond, "db")
	if err != nil {
		t.Fatal(err)
	}
	leaderCtx := controller.Start(ctx)
	controller.SetReady(true)
	_, _ = b.db.ExecContext(ctx, `UPDATE master_leadership SET lease_deadline_ms=0 WHERE leadership_name='master'`)
	if _, err := b.AcquireLeadership(ctx, "second", time.Minute, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-leaderCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("leader work context was not canceled")
	}
	status := controller.Status()
	if status.Ready || status.State != "LOST" {
		t.Fatalf("status=%+v", status)
	}
	controller.Stop(ctx)
}
