package connectors

import (
	"fmt"
	"strings"
)

// identifierDialect contains the small amount of syntax needed to render SQL
// identifiers safely. Callers must choose whether an input is one literal
// identifier or a qualified reference; dots are never inferred by the single
// identifier renderer.
type identifierDialect struct {
	open, close       string
	escape            func(string) string
	normalizeUnquoted func(string) string
}

func doubleQuoteDialect() identifierDialect {
	return identifierDialect{open: `"`, close: `"`, escape: func(s string) string { return strings.ReplaceAll(s, `"`, `""`) }}
}

func backtickDialect() identifierDialect {
	return identifierDialect{open: "`", close: "`", escape: func(s string) string { return strings.ReplaceAll(s, "`", "``") }}
}

func bracketDialect() identifierDialect {
	return identifierDialect{open: "[", close: "]", escape: func(s string) string { return strings.ReplaceAll(s, "]", "]]") }}
}

func quoteIdentifierPart(raw string, dialect identifierDialect) (string, error) {
	if raw == "" || strings.ContainsRune(raw, '\x00') {
		return "", fmt.Errorf("invalid identifier")
	}
	return dialect.open + dialect.escape(raw) + dialect.close, nil
}

// quoteQualifiedIdentifier parses only the qualification separators. Quoted
// input parts retain dots and escaped delimiters as literal content.
func quoteQualifiedIdentifier(raw string, dialect identifierDialect) (string, error) {
	parts, err := parseQualifiedIdentifier(raw, dialect)
	if err != nil {
		return "", err
	}
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		name := part.name
		if !part.quoted && dialect.normalizeUnquoted != nil {
			name = dialect.normalizeUnquoted(name)
		}
		q, err := quoteIdentifierPart(name, dialect)
		if err != nil {
			return "", err
		}
		quoted = append(quoted, q)
	}
	return strings.Join(quoted, "."), nil
}

type identifierPart struct {
	name   string
	quoted bool
}

func parseQualifiedIdentifier(raw string, dialect identifierDialect) ([]identifierPart, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty identifier")
	}
	parts := make([]identifierPart, 0, 3)
	for pos := 0; pos < len(raw); {
		for pos < len(raw) && (raw[pos] == ' ' || raw[pos] == '\t' || raw[pos] == '\n' || raw[pos] == '\r') {
			pos++
		}
		if pos == len(raw) {
			return nil, fmt.Errorf("empty identifier part")
		}
		part := identifierPart{}
		if strings.HasPrefix(raw[pos:], dialect.open) {
			part.quoted = true
			pos += len(dialect.open)
			var b strings.Builder
			for {
				if pos >= len(raw) {
					return nil, fmt.Errorf("unterminated quoted identifier")
				}
				if strings.HasPrefix(raw[pos:], dialect.close) {
					next := pos + len(dialect.close)
					if strings.HasPrefix(raw[next:], dialect.close) {
						b.WriteString(dialect.close)
						pos = next + len(dialect.close)
						continue
					}
					pos = next
					break
				}
				b.WriteByte(raw[pos])
				pos++
			}
			part.name = b.String()
		} else {
			start := pos
			for pos < len(raw) && raw[pos] != '.' {
				pos++
			}
			part.name = strings.TrimSpace(raw[start:pos])
		}
		if part.name == "" || strings.ContainsRune(part.name, '\x00') {
			return nil, fmt.Errorf("invalid identifier")
		}
		parts = append(parts, part)
		for pos < len(raw) && (raw[pos] == ' ' || raw[pos] == '\t' || raw[pos] == '\n' || raw[pos] == '\r') {
			pos++
		}
		if pos == len(raw) {
			break
		}
		if raw[pos] != '.' {
			return nil, fmt.Errorf("invalid qualified identifier")
		}
		pos++
	}
	return parts, nil
}
