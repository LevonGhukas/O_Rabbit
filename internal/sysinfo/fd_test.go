package sysinfo

import "testing"

func TestFileDescriptors(t *testing.T) {
	used, limit, ok := FileDescriptors()

	if !ok {
		// Not supported on this platform, that's fine
		return
	}

	if limit == 0 {
		t.Fatal("expected file descriptor limit to be > 0")
	}

	if used > limit {
		t.Fatalf("used file descriptors %d exceeds limit %d", used, limit)
	}
}
