//go:build linux || darwin

package sysinfo

import (
	"os"

	"golang.org/x/sys/unix"
)

func fileDescriptors() (used, limit uint64, ok bool) {
	var rlimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rlimit); err != nil {
		return 0, 0, false
	}
	limit = rlimit.Cur

	// To count open FDs, the most robust cross-unix way is to read /proc/self/fd on linux or /dev/fd on darwin
	path := "/proc/self/fd"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		path = "/dev/fd"
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return 0, limit, true // Returning limit is still useful
	}
	return uint64(len(entries)), limit, true
}
