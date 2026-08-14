package sysinfo

// TotalMemoryBytes returns total physical memory (best-effort).
// ok=false means the platform implementation is unavailable.
func TotalMemoryBytes() (bytes uint64, ok bool) {
	return totalMemoryBytes()
}

// AvailableMemoryBytes returns available physical memory (best-effort).
// ok=false means the platform implementation is unavailable.
func AvailableMemoryBytes() (bytes uint64, ok bool) {
	return availableMemoryBytes()
}
