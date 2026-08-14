package common

import (
	"fmt"
	"path"
	"regexp"
	"strings"
)

var shellUnsafeSingleQuote = regexp.MustCompile(`'`)

func ShellQuote(value string) string {
	if value == "" {
		return "''"
	}
	return "'" + shellUnsafeSingleQuote.ReplaceAllString(value, `'"'"'`) + "'"
}

func ResolveUnderProject(projectDir string, relativePath string) (string, error) {
	base := strings.TrimSpace(projectDir)
	if base == "" {
		return "", fmt.Errorf("missing project_dir")
	}
	base = path.Clean(base)
	if !path.IsAbs(base) {
		return "", fmt.Errorf("project_dir must be an absolute path")
	}

	rel := strings.TrimSpace(relativePath)
	if rel == "" {
		return "", fmt.Errorf("missing relative path")
	}
	if strings.HasPrefix(rel, "/") {
		return "", fmt.Errorf("relative path must not be absolute")
	}
	cleanRel := path.Clean(rel)
	if cleanRel == "." || cleanRel == ".." || strings.HasPrefix(cleanRel, "../") {
		return "", fmt.Errorf("path traversal is not allowed")
	}

	resolved := path.Join(base, cleanRel)
	if resolved != base && !strings.HasPrefix(resolved, base+"/") {
		return "", fmt.Errorf("resolved path escapes project_dir")
	}
	return resolved, nil
}
