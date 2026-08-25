package connectors

import (
	"fmt"
	"strings"
)

// identifierRenderer renders raw identifier parts only at query generation.
// It deliberately has no SQL parsing responsibility: legacy dotted values are
// accepted solely at the current string-based boundary until structured table
// references are available in the service contract.
type identifierRenderer struct {
	openQuote  string
	closeQuote string
}

func (r identifierRenderer) part(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("empty identifier")
	}
	return r.openQuote + strings.ReplaceAll(raw, r.closeQuote, r.closeQuote+r.closeQuote) + r.closeQuote, nil
}

func (r identifierRenderer) qualified(parts ...string) (string, error) {
	if len(parts) == 0 {
		return "", fmt.Errorf("empty qualified identifier")
	}
	rendered := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted, err := r.part(part)
		if err != nil {
			return "", err
		}
		rendered = append(rendered, quoted)
	}
	return strings.Join(rendered, "."), nil
}

// legacyQualified preserves the existing dot-delimited table contract. It
// recognises previously rendered identifier parts, but never treats a select
// column as qualified; a selected column is always one literal identifier.
func (r identifierRenderer) legacyQualified(raw string) (string, error) {
	parts, err := splitLegacyIdentifier(raw, r.openQuote, r.closeQuote)
	if err != nil {
		return "", err
	}
	return r.qualified(parts...)
}

func splitLegacyIdentifier(raw, openQuote, closeQuote string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty identifier")
	}
	var parts []string
	var current strings.Builder
	inQuote := false
	for i := 0; i < len(raw); i++ {
		if inQuote {
			if strings.HasPrefix(raw[i:], closeQuote) {
				if strings.HasPrefix(raw[i+len(closeQuote):], closeQuote) {
					current.WriteString(closeQuote)
					i += len(closeQuote)
					continue
				}
				inQuote = false
				i += len(closeQuote) - 1
				continue
			}
			current.WriteByte(raw[i])
			continue
		}
		if current.Len() == 0 && strings.HasPrefix(raw[i:], openQuote) {
			inQuote = true
			i += len(openQuote) - 1
			continue
		}
		if !inQuote && raw[i] == '.' {
			part := strings.TrimSpace(current.String())
			if part == "" {
				return nil, fmt.Errorf("empty identifier part")
			}
			parts = append(parts, part)
			current.Reset()
			continue
		}
		current.WriteByte(raw[i])
	}
	if inQuote {
		return nil, fmt.Errorf("unterminated quoted identifier")
	}
	part := strings.TrimSpace(current.String())
	if part == "" {
		return nil, fmt.Errorf("empty identifier part")
	}
	return append(parts, part), nil
}

func renderSelectColumns(renderer identifierRenderer, columns []string) (string, error) {
	if len(columns) == 0 {
		return "*", nil
	}
	rendered := make([]string, 0, len(columns))
	for _, column := range columns {
		column = strings.TrimSpace(column)
		if column == "" {
			continue
		}
		quoted, err := renderer.part(column)
		if err != nil {
			return "", err
		}
		rendered = append(rendered, quoted)
	}
	if len(rendered) == 0 {
		return "*", nil
	}
	return strings.Join(rendered, ", "), nil
}

var (
	postgresIdentifierRenderer   = identifierRenderer{openQuote: `"`, closeQuote: `"`}
	mysqlIdentifierRenderer      = identifierRenderer{openQuote: "`", closeQuote: "`"}
	mssqlIdentifierRenderer      = identifierRenderer{openQuote: "[", closeQuote: "]"}
	clickHouseIdentifierRenderer = identifierRenderer{openQuote: "`", closeQuote: "`"}
	trinoIdentifierRenderer      = identifierRenderer{openQuote: `"`, closeQuote: `"`}
	oracleIdentifierRenderer     = identifierRenderer{openQuote: `"`, closeQuote: `"`}
	cassandraIdentifierRenderer  = identifierRenderer{openQuote: `"`, closeQuote: `"`}
)
