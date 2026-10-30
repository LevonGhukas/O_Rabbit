package main

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// memoryLimitBytes reports the memory this worker may use: the container's
// cgroup limit when one is set, otherwise the host's total memory. It returns
// 0 when neither can be read (for example on macOS), so callers can omit it.
func memoryLimitBytes() int64 {
	host := readMemTotal("/proc/meminfo")
	limit := readCgroupLimit("/sys/fs/cgroup/memory.max") // cgroup v2
	if limit == 0 {
		limit = readCgroupLimit("/sys/fs/cgroup/memory/memory.limit_in_bytes") // cgroup v1
	}
	return effectiveMemory(host, limit)
}

// effectiveMemory picks the smaller known value; 0 means unknown.
func effectiveMemory(host, limit int64) int64 {
	switch {
	case limit > 0 && (host == 0 || limit < host):
		return limit
	default:
		return host
	}
}

func readMemTotal(path string) int64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	return parseMemTotal(bufio.NewScanner(f))
}

// parseMemTotal reads "MemTotal:  16303488 kB" from /proc/meminfo.
func parseMemTotal(sc *bufio.Scanner) int64 {
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) >= 2 && fields[0] == "MemTotal:" {
			kb, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil || kb <= 0 {
				return 0
			}
			return kb * 1024
		}
	}
	return 0
}

func readCgroupLimit(path string) int64 {
	raw, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	return parseCgroupLimit(string(raw))
}

// noCgroupLimitV1 is the sentinel cgroup v1 reports for "unlimited" (close to max int64, page aligned).
const noCgroupLimitV1 = int64(1) << 62

// parseCgroupLimit returns the limit in bytes, or 0 for "max" (v2), the
// v1 unlimited sentinel, or anything unreadable.
func parseCgroupLimit(raw string) int64 {
	v := strings.TrimSpace(raw)
	if v == "" || v == "max" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 || n >= noCgroupLimitV1 {
		return 0
	}
	return n
}
