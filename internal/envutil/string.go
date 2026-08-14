package envutil

import (
	"os"
	"strings"
)

// EnvOrDefault returns a trimmed env value or the provided default.
func EnvOrDefault(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}
