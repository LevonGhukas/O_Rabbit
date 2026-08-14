//go:build !darwin && !linux && !windows

package sysinfo

func totalMemoryBytes() (uint64, bool) {
	return 0, false
}

func availableMemoryBytes() (uint64, bool) {
	return 0, false
}
