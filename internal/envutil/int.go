package envutil

import (
	"strconv"
	"strings"
)

// ParsePositiveInt parses a trimmed positive integer string.
func ParsePositiveInt(raw string) (int, bool) {
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}
