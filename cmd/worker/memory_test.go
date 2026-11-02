package main

import (
	"bufio"
	"strings"
	"testing"
)

func TestParseMemTotal(t *testing.T) {
	sc := bufio.NewScanner(strings.NewReader("MemFree:  100 kB\nMemTotal:       16303488 kB\n"))
	if got, want := parseMemTotal(sc), int64(16303488*1024); got != want {
		t.Fatalf("parseMemTotal = %d, want %d", got, want)
	}
	if got := parseMemTotal(bufio.NewScanner(strings.NewReader("garbage"))); got != 0 {
		t.Fatalf("parseMemTotal(garbage) = %d, want 0", got)
	}
}

func TestParseCgroupLimit(t *testing.T) {
	cases := map[string]int64{
		"max\n":               0,          // cgroup v2 unlimited
		"9223372036854771712": 0,          // cgroup v1 unlimited sentinel
		"2147483648\n":        2147483648, // 2 GiB limit
		"":                    0,
		"not-a-number":        0,
	}
	for raw, want := range cases {
		if got := parseCgroupLimit(raw); got != want {
			t.Errorf("parseCgroupLimit(%q) = %d, want %d", raw, got, want)
		}
	}
}

func TestEffectiveMemory(t *testing.T) {
	cases := []struct{ host, limit, want int64 }{
		{host: 16 << 30, limit: 2 << 30, want: 2 << 30}, // container limit below host
		{host: 16 << 30, limit: 0, want: 16 << 30},      // no limit
		{host: 2 << 30, limit: 16 << 30, want: 2 << 30}, // limit above host is meaningless
		{host: 0, limit: 1 << 30, want: 1 << 30},        // host unknown
		{host: 0, limit: 0, want: 0},                    // nothing known
	}
	for _, c := range cases {
		if got := effectiveMemory(c.host, c.limit); got != c.want {
			t.Errorf("effectiveMemory(%d, %d) = %d, want %d", c.host, c.limit, got, c.want)
		}
	}
}
