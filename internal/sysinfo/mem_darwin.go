//go:build darwin

package sysinfo

import "golang.org/x/sys/unix"

func totalMemoryBytes() (uint64, bool) {
	v, err := unix.SysctlUint64("hw.memsize")
	if err != nil || v == 0 {
		return 0, false
	}
	return v, true
}

func availableMemoryBytes() (uint64, bool) {
	// Not easily available on macOS without cgo or complex mach host calls.
	return 0, false
}
