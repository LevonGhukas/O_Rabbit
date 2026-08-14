package envutil

import "strings"

// ParseBoolEnv parses a bool-like string without applying defaults.
func ParseBoolEnv(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "y", "yes", "on":
		return true, true
	case "0", "false", "n", "no", "off":
		return false, true
	default:
		return false, false
	}
}
