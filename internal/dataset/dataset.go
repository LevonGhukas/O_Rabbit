package dataset

import "strings"

// Prefix returns the dataset prefix used for Parquet parts and _state.json.
//
// Layout (default):
//
//	<engine>/<table>
//
// Example:
//
//	mssql/Orders
//
// If targetPrefix is non-empty, it is used as-is (trimmed).
func Prefix(targetPrefix, engine, table string) string {
	p := CanonicalPrefix(targetPrefix)
	if p != "" {
		return p
	}

	eng := strings.TrimSpace(engine)
	if eng == "" {
		eng = "db"
	}
	name := TableName(table)
	if name == "" {
		name = "table"
	}
	return eng + "/" + name
}

// TableName converts identifiers like "SalesDB.dbo.Orders" or "[SalesDB].[dbo].[Orders]" into a stable dataset name.
//
// We keep all identifier segments to avoid collisions across schemas/databases.
func TableName(table string) string {
	parts := strings.Split(table, ".")
	segs := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		p = strings.TrimPrefix(p, "[")
		p = strings.TrimSuffix(p, "]")
		p = strings.Trim(p, "`\"'")
		if p == "" {
			continue
		}

		var b strings.Builder
		for _, r := range p {
			switch {
			case r >= 'a' && r <= 'z':
				b.WriteRune(r)
			case r >= 'A' && r <= 'Z':
				b.WriteRune(r)
			case r >= '0' && r <= '9':
				b.WriteRune(r)
			case r == '_' || r == '-':
				b.WriteRune(r)
			default:
				b.WriteByte('_')
			}
		}
		seg := strings.Trim(b.String(), "_")
		if seg == "" {
			continue
		}
		segs = append(segs, seg)
	}
	return strings.Join(segs, "__")
}

// IcebergTableFromStoragePrefix converts a discovered warehouse/table prefix
// into an Iceberg identifier. If discovery points at the Iceberg metadata
// directory itself, only a final path segment named exactly "metadata" is
// removed. Object keys under metadata/ are intentionally not special-cased here;
// callers should pass discovered table/common prefixes, not Iceberg files.
func IcebergTableFromStoragePrefix(prefix string) (string, bool) {
	p := CanonicalPrefix(prefix)
	if p == "" || !strings.Contains(p, "/") {
		return "", false
	}

	parts := strings.Split(p, "/")
	if len(parts) >= 2 && parts[len(parts)-2] == "metadata" && isIcebergMetadataFileSegment(parts[len(parts)-1]) {
		return "", false
	}
	if len(parts) > 0 && parts[len(parts)-1] == "metadata" {
		parts = parts[:len(parts)-1]
	}
	if len(parts) == 0 {
		return "", false
	}

	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		clean = append(clean, part)
	}
	if len(clean) == 0 {
		return "", false
	}
	return strings.Join(clean, "."), true
}

func isIcebergMetadataFileSegment(segment string) bool {
	segment = strings.TrimSpace(segment)
	return strings.HasSuffix(segment, ".json") ||
		strings.HasSuffix(segment, ".avro") ||
		segment == "version-hint.text"
}

// CanonicalPrefix normalizes a dataset prefix into a stable path-like form.
func CanonicalPrefix(prefix string) string {
	p := strings.TrimSpace(prefix)
	p = strings.Trim(p, "/")
	for strings.Contains(p, "//") {
		p = strings.ReplaceAll(p, "//", "/")
	}
	return p
}

// StorageKey builds a stable identity for a physical dataset location.
func StorageKey(endpoint, bucket, prefix string) string {
	e := strings.TrimSpace(endpoint)
	if e == "" {
		e = "http://localhost:9000"
	}
	b := strings.TrimSpace(bucket)
	p := CanonicalPrefix(prefix)
	return e + "|" + b + "|" + p
}
