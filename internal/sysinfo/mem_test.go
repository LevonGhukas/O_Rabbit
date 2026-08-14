package sysinfo

import "testing"

func TestTotalMemoryBytes(t *testing.T) {
	bytes, ok := TotalMemoryBytes()

	if !ok {
		t.Fatal("expected memory detection to be available")
	}

	if bytes == 0 {
		t.Fatal("expected non-zero memory size")
	}
}

func TestTotalMemoryBytesReasonable(t *testing.T) {
	bytes, ok := TotalMemoryBytes()

	if !ok {
		t.Fatal("expected memory detection to be available")
	}

	// Any normal machine should have at least 1 MiB of RAM.
	// This mainly protects against returning garbage/zero values.
	const minMemory = 1024 * 1024

	if bytes < minMemory {
		t.Fatalf("memory too small: %d bytes", bytes)
	}
}

func TestAvailableMemoryBytes(t *testing.T) {
	bytes, ok := AvailableMemoryBytes()

	// Some platforms might not support this, in which case it should return ok=false
	if !ok {
		return
	}

	if bytes == 0 {
		// It's technically possible but very unlikely to have exactly 0 bytes available
		// unless under severe pressure, but for a test environment it should be > 0.
		t.Log("available memory is 0, which is suspicious but possible")
	}

	total, tok := TotalMemoryBytes()
	if tok && bytes > total {
		t.Fatalf("available memory %d exceeds total memory %d", bytes, total)
	}
}
