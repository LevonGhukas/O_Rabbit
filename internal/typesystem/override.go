package typesystem

import (
	"regexp"
	"strings"
)

// SourceTypeKeyword is a column override placeholder meaning "keep the type
// inferred from the source". It lets callers change only nullability, e.g.
// "source" (NOT NULL) or "nullable<source>" (NULL allowed).
const SourceTypeKeyword = "source"

var sourceOverridePattern = regexp.MustCompile(`(?i)^\s*(?:(nullable)\s*[<(]\s*source\s*[>)]|source)\s*$`)

// IsSourceOverride reports whether raw is a source-type placeholder and, if so,
// whether it requests a nullable column.
func IsSourceOverride(raw string) (isSource bool, nullable bool) {
	m := sourceOverridePattern.FindStringSubmatch(raw)
	if m == nil {
		return false, false
	}
	return true, strings.TrimSpace(m[1]) != ""
}

// ValidateOverride checks the syntax of a user column override.
func ValidateOverride(raw string) error {
	if ok, _ := IsSourceOverride(raw); ok {
		return nil
	}
	_, err := ParseType(raw)
	return err
}

// ResolveOverride turns a user column override into a logical type, resolving
// the source-type placeholder against the column's inferred source type.
func ResolveOverride(raw string, source LogicalType) (LogicalType, error) {
	if ok, nullable := IsSourceOverride(raw); ok {
		source.Nullable = nullable
		return source, nil
	}
	return ParseType(strings.TrimSpace(raw))
}
