//go:build linux

package sysinfo

import (
	"os"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

func totalMemoryBytes() (uint64, bool) {
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0, false
	}
	// Totalram is in units of info.Unit bytes.
	unit := uint64(info.Unit)
	if unit == 0 {
		unit = 1
	}
	v := uint64(info.Totalram) * unit
	if v == 0 {
		return 0, false
	}
	return v, true
}

func availableMemoryBytes() (uint64, bool) {
	// First try cgroups v2
	if b, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		maxStr := strings.TrimSpace(string(b))
		if maxStr != "max" {
			if max, err := strconv.ParseUint(maxStr, 10, 64); err == nil {
				if currB, err := os.ReadFile("/sys/fs/cgroup/memory.current"); err == nil {
					if curr, err := strconv.ParseUint(strings.TrimSpace(string(currB)), 10, 64); err == nil {
						if max > curr {
							return max - curr, true
						}
						return 0, true
					}
				}
			}
		}
	}
	// Try cgroups v1
	if b, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		maxStr := strings.TrimSpace(string(b))
		if max, err := strconv.ParseUint(maxStr, 10, 64); err == nil && max < 9000000000000000000 {
			if currB, err := os.ReadFile("/sys/fs/cgroup/memory/memory.usage_in_bytes"); err == nil {
				if curr, err := strconv.ParseUint(strings.TrimSpace(string(currB)), 10, 64); err == nil {
					if max > curr {
						return max - curr, true
					}
					return 0, true
				}
			}
		}
	}

	// Fallback to MemAvailable in /proc/meminfo
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		lines := strings.Split(string(b), "\n")
		for _, line := range lines {
			if strings.HasPrefix(line, "MemAvailable:") {
				parts := strings.Fields(line)
				if len(parts) >= 2 {
					if kb, err := strconv.ParseUint(parts[1], 10, 64); err == nil {
						return kb * 1024, true
					}
				}
			}
		}
	}

	// Ultimate fallback to sysinfo Freeram
	var info unix.Sysinfo_t
	if err := unix.Sysinfo(&info); err != nil {
		return 0, false
	}
	unit := uint64(info.Unit)
	if unit == 0 {
		unit = 1
	}
	v := uint64(info.Freeram) * unit
	return v, true
}
