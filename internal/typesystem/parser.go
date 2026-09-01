package typesystem

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// ParseType parses ORabbit's canonical target-type syntax and the supported
// legacy ClickHouse-style compatibility spellings into logical semantics.
// Unlike source inference, an unknown explicit target type is always an error.
func ParseType(input string) (LogicalType, error) {
	p := typeParser{input: input}
	p.skipSpace()
	if p.eof() {
		return LogicalType{}, p.errorf("missing type")
	}
	t, err := p.parseType()
	if err != nil {
		return LogicalType{}, err
	}
	p.skipSpace()
	if !p.eof() {
		return LogicalType{}, p.errorf("unexpected trailing input %q", p.input[p.pos:])
	}
	if err := t.Validate(); err != nil {
		return LogicalType{}, fmt.Errorf("invalid type %q: %w", input, err)
	}
	return t, nil
}

type typeParser struct {
	input string
	pos   int
}

func (p *typeParser) parseType() (LogicalType, error) {
	p.skipSpace()
	identifier, err := p.identifier()
	if err != nil {
		return LogicalType{}, err
	}
	name := strings.ToLower(identifier)

	switch name {
	case "array":
		return p.parseArray()
	case "nullable":
		return p.parseNullable()
	case "lowcardinality":
		return p.parseLowCardinality()
	case "decimal", "numeric", "number":
		return p.parseDecimal()
	case "money":
		return Decimal(19, 4), nil
	case "smallmoney":
		return Decimal(10, 4), nil
	case "timestamp_tz":
		return p.parseTimestampTZ()
	case "datetime", "datetime64", "timestamp":
		return p.parseLegacyTimestamp(name)
	case "time64":
		if err := p.consumeOptionalPrecision(); err != nil {
			return LogicalType{}, err
		}
		return LogicalType{Kind: KindTime}, nil
	}

	if kind, ok := primitiveKind(name); ok {
		return LogicalType{Kind: kind}, nil
	}
	return LogicalType{}, p.errorf("unsupported target type %q", identifier)
}

func primitiveKind(name string) (Kind, bool) {
	switch name {
	case "string", "text", "varchar", "nvarchar", "char", "nchar", "xml":
		return KindString, true
	case "bool", "boolean":
		return KindBool, true
	case "int8":
		return KindInt8, true
	case "int16":
		return KindInt16, true
	case "int32":
		return KindInt32, true
	case "int64":
		return KindInt64, true
	case "uint8":
		return KindUInt8, true
	case "uint16":
		return KindUInt16, true
	case "uint32":
		return KindUInt32, true
	case "uint64":
		return KindUInt64, true
	case "float32":
		return KindFloat32, true
	case "float64":
		return KindFloat64, true
	case "date", "date32":
		return KindDate, true
	case "time":
		return KindTime, true
	case "uuid", "uniqueidentifier":
		return KindUUID, true
	case "binary", "bytea", "blob", "varbinary", "image", "rowversion":
		return KindBinary, true
	case "json":
		return KindJSON, true
	}
	return KindUnknown, false
}

func (p *typeParser) parseArray() (LogicalType, error) {
	open, close, err := p.openContainer()
	if err != nil {
		return LogicalType{}, err
	}
	_ = open
	p.skipSpace()
	if p.peek(close) {
		return LogicalType{}, p.errorf("missing array element type")
	}
	element, err := p.parseType()
	if err != nil {
		return LogicalType{}, err
	}
	if err := p.closeContainer(close); err != nil {
		return LogicalType{}, err
	}
	return ArrayOf(element), nil
}

func (p *typeParser) parseNullable() (LogicalType, error) {
	_, close, err := p.openContainer()
	if err != nil {
		return LogicalType{}, err
	}
	p.skipSpace()
	if p.peek(close) {
		return LogicalType{}, p.errorf("missing nullable type")
	}
	t, err := p.parseType()
	if err != nil {
		return LogicalType{}, err
	}
	if err := p.closeContainer(close); err != nil {
		return LogicalType{}, err
	}
	t.Nullable = true
	return t, nil
}

func (p *typeParser) parseLowCardinality() (LogicalType, error) {
	_, close, err := p.openContainer()
	if err != nil {
		return LogicalType{}, err
	}
	t, err := p.parseType()
	if err != nil {
		return LogicalType{}, err
	}
	if err := p.closeContainer(close); err != nil {
		return LogicalType{}, err
	}
	return t, nil
}

func (p *typeParser) parseDecimal() (LogicalType, error) {
	p.skipSpace()
	if !p.consume('(') {
		return LogicalType{}, p.errorf("decimal requires precision and scale")
	}
	precision, err := p.int32("decimal precision")
	if err != nil {
		return LogicalType{}, err
	}
	p.skipSpace()
	if !p.consume(',') {
		return LogicalType{}, p.errorf("decimal requires precision and scale")
	}
	scale, err := p.int32("decimal scale")
	if err != nil {
		return LogicalType{}, err
	}
	p.skipSpace()
	if !p.consume(')') {
		return LogicalType{}, p.errorf("expected ')' after decimal scale")
	}
	t := Decimal(precision, scale)
	if err := t.Validate(); err != nil {
		return LogicalType{}, fmt.Errorf("invalid type %q: %w", p.input, err)
	}
	return t, nil
}

func (p *typeParser) parseTimestampTZ() (LogicalType, error) {
	t := LogicalType{Kind: KindTimestampTZ}
	p.skipSpace()
	if !p.consume('[') {
		return t, nil
	}
	start := p.pos
	for !p.eof() && p.input[p.pos] != ']' {
		p.pos++
	}
	if p.eof() {
		return LogicalType{}, p.errorf("unterminated timestamp timezone")
	}
	t.Timezone = strings.TrimSpace(p.input[start:p.pos])
	if t.Timezone == "" {
		return LogicalType{}, p.errorf("timestamp timezone is empty")
	}
	p.pos++
	return t, nil
}

func (p *typeParser) parseLegacyTimestamp(name string) (LogicalType, error) {
	p.skipSpace()
	if !p.peek('(') {
		return LogicalType{Kind: KindTimestamp}, nil
	}
	raw, err := p.parenthesizedContent()
	if err != nil {
		return LogicalType{}, err
	}
	parts := splitArguments(raw)
	var timezone string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return LogicalType{}, p.errorf("empty timestamp argument")
		}
		if parsedTimezone, ok := quotedText(part); ok {
			if timezone != "" {
				return LogicalType{}, p.errorf("multiple timestamp timezones")
			}
			timezone = parsedTimezone
			continue
		}
		if _, err := strconv.ParseInt(part, 10, 32); err != nil {
			return LogicalType{}, p.errorf("unsupported %s argument %q", name, part)
		}
	}
	if timezone != "" {
		return LogicalType{Kind: KindTimestampTZ, Timezone: timezone}, nil
	}
	return LogicalType{Kind: KindTimestamp}, nil
}

func (p *typeParser) consumeOptionalPrecision() error {
	p.skipSpace()
	if !p.peek('(') {
		return nil
	}
	raw, err := p.parenthesizedContent()
	if err != nil {
		return err
	}
	if _, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 32); err != nil {
		return p.errorf("unsupported time precision %q", raw)
	}
	return nil
}

func (p *typeParser) openContainer() (byte, byte, error) {
	p.skipSpace()
	if p.consume('<') {
		return '<', '>', nil
	}
	if p.consume('(') {
		return '(', ')', nil
	}
	return 0, 0, p.errorf("expected '<' or '('")
}

func (p *typeParser) closeContainer(close byte) error {
	p.skipSpace()
	if !p.consume(close) {
		return p.errorf("expected %q", string(close))
	}
	return nil
}

func (p *typeParser) parenthesizedContent() (string, error) {
	p.skipSpace()
	if !p.consume('(') {
		return "", p.errorf("expected '('")
	}
	start, depth := p.pos, 1
	var quote byte
	for !p.eof() {
		current := p.input[p.pos]
		p.pos++
		if quote != 0 {
			if current == '\\' && !p.eof() {
				p.pos++
			} else if current == quote {
				quote = 0
			}
			continue
		}
		if current == '\'' || current == '"' {
			quote = current
			continue
		}
		switch current {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return p.input[start : p.pos-1], nil
			}
		}
	}
	return "", p.errorf("unterminated parenthesized type")
}

func (p *typeParser) identifier() (string, error) {
	p.skipSpace()
	start := p.pos
	for !p.eof() {
		r := rune(p.input[p.pos])
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			p.pos++
			continue
		}
		break
	}
	if start == p.pos {
		return "", p.errorf("expected type name")
	}
	return p.input[start:p.pos], nil
}

func (p *typeParser) int32(label string) (int32, error) {
	p.skipSpace()
	start := p.pos
	if p.peek('-') || p.peek('+') {
		p.pos++
	}
	for !p.eof() && p.input[p.pos] >= '0' && p.input[p.pos] <= '9' {
		p.pos++
	}
	if start == p.pos || (p.pos == start+1 && (p.input[start] == '-' || p.input[start] == '+')) {
		return 0, p.errorf("expected %s", label)
	}
	value, err := strconv.ParseInt(p.input[start:p.pos], 10, 32)
	if err != nil {
		return 0, p.errorf("invalid %s", label)
	}
	return int32(value), nil
}

func (p *typeParser) skipSpace() {
	for !p.eof() && unicode.IsSpace(rune(p.input[p.pos])) {
		p.pos++
	}
}

func (p *typeParser) consume(expected byte) bool {
	if p.peek(expected) {
		p.pos++
		return true
	}
	return false
}

func (p *typeParser) peek(expected byte) bool { return !p.eof() && p.input[p.pos] == expected }
func (p *typeParser) eof() bool               { return p.pos >= len(p.input) }

func (p *typeParser) errorf(format string, args ...any) error {
	return fmt.Errorf("invalid type %q at position %d: %s", p.input, p.pos, fmt.Sprintf(format, args...))
}

func splitArguments(raw string) []string {
	parts := make([]string, 0, 2)
	start, depth := 0, 0
	var quote byte
	for index := 0; index < len(raw); index++ {
		current := raw[index]
		if quote != 0 {
			if current == '\\' {
				index++
			} else if current == quote {
				quote = 0
			}
			continue
		}
		switch current {
		case '\'', '"':
			quote = current
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, raw[start:index])
				start = index + 1
			}
		}
	}
	parts = append(parts, raw[start:])
	return parts
}

func quotedText(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if len(value) < 2 || (value[0] != '\'' && value[0] != '"') || value[len(value)-1] != value[0] {
		return "", false
	}
	text := strings.TrimSpace(value[1 : len(value)-1])
	return text, text != ""
}
