//go:build windows

package sysinfo

func fileDescriptors() (used, limit uint64, ok bool) {
	return 0, 0, false
}
