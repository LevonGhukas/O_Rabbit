// Package icebergpart is the partition contract between the master and its
// workers for partitioned Iceberg tables.
//
// The master sends a table's partition spec to every worker task. A worker
// splits its rows with the same Iceberg transforms the catalog uses (the
// iceberg-go implementation), writes one partition tuple per Parquet file and
// records that tuple in the file's footer. The master then registers the
// worker's files as Iceberg data files, reading only their footers: it never
// rewrites rows, so the files Iceberg references are the files whose checksums
// were verified at commit.
package icebergpart

import (
	"encoding/json"
	"fmt"
	"strings"

	iceberg "github.com/apache/iceberg-go"
)

// FooterKey is the Parquet key-value metadata key that holds a file's Contract.
const FooterKey = "orabbit.iceberg.partition"

// contractVersion is bumped when the footer format changes incompatibly.
const contractVersion = 1

// Field is one partition field. The JSON names match the iceberg registration
// option partition_spec, so the master can send that option unchanged.
type Field struct {
	Source    string `json:"source"`
	Name      string `json:"name,omitempty"`
	Transform string `json:"transform,omitempty"`
}

// Spec is an ordered partition spec.
type Spec []Field

// Normalize returns a copy with trimmed names and the default transform,
// matching how the registration options are normalized.
func (s Spec) Normalize() Spec {
	out := make(Spec, len(s))
	for i, f := range s {
		out[i] = Field{
			Source:    strings.TrimSpace(f.Source),
			Name:      strings.TrimSpace(f.Name),
			Transform: strings.ToLower(strings.TrimSpace(f.Transform)),
		}
		if out[i].Transform == "" {
			out[i].Transform = "identity"
		}
	}
	return out
}

// Equal reports whether two specs partition rows identically. Field names are
// ignored: they label the partition columns but do not change the values.
func (s Spec) Equal(other Spec) bool {
	a, b := s.Normalize(), other.Normalize()
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Source != b[i].Source || !sameTransform(a[i].Transform, b[i].Transform) {
			return false
		}
	}
	return true
}

func sameTransform(a, b string) bool {
	ta, errA := iceberg.ParseTransform(a)
	tb, errB := iceberg.ParseTransform(b)
	if errA != nil || errB != nil {
		return a == b
	}
	return ta.Equals(tb)
}

// String renders the spec for error messages, for example "bucket[16](id), day(ts)".
func (s Spec) String() string {
	parts := make([]string, len(s))
	for i, f := range s.Normalize() {
		parts[i] = fmt.Sprintf("%s(%s)", f.Transform, f.Source)
	}
	return strings.Join(parts, ", ")
}

// ParseSpecJSON decodes the spec a worker receives with its task. An empty
// string means the target is not a partitioned Iceberg table.
func ParseSpecJSON(raw string) (Spec, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var spec Spec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		return nil, fmt.Errorf("decode iceberg partition spec: %w", err)
	}
	spec = spec.Normalize()
	for _, f := range spec {
		if f.Source == "" {
			return nil, fmt.Errorf("iceberg partition spec has a field without a source column")
		}
		if _, err := iceberg.ParseTransform(f.Transform); err != nil {
			return nil, fmt.Errorf("iceberg partition spec field %q: %w", f.Source, err)
		}
	}
	return spec, nil
}

// Value is one partition value: the transform result in Iceberg's single-value
// binary serialization, tagged with its Iceberg type. A nil *Value is a null
// partition value.
type Value struct {
	Type string `json:"type"`
	Data []byte `json:"data"`
}

// Contract is stored in the footer of every file a worker writes for a
// partitioned table. Values are in Spec order.
type Contract struct {
	Version int      `json:"version"`
	Spec    Spec     `json:"spec"`
	Values  []*Value `json:"values"`
}

// Marshal encodes the contract for the Parquet footer.
func (c Contract) Marshal() (string, error) {
	c.Version = contractVersion
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// ParseContract decodes and checks a footer contract.
func ParseContract(raw string) (Contract, error) {
	var c Contract
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return Contract{}, fmt.Errorf("decode partition contract: %w", err)
	}
	if c.Version != contractVersion {
		return Contract{}, fmt.Errorf("unsupported partition contract version %d", c.Version)
	}
	if len(c.Values) != len(c.Spec) {
		return Contract{}, fmt.Errorf("partition contract has %d values for %d fields", len(c.Values), len(c.Spec))
	}
	return c, nil
}

// encodeLiteral serializes a transform result for the footer.
func encodeLiteral(lit iceberg.Literal) (*Value, error) {
	if lit == nil {
		return nil, nil
	}
	data, err := lit.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("serialize partition value %s: %w", lit, err)
	}
	return &Value{Type: lit.Type().Type(), Data: data}, nil
}

// Literal decodes the value and converts it to want, the result type of the
// partition field in the table. The conversion covers lossless widenings such
// as int to long when the table's column type is wider than the file's.
func (v *Value) Literal(want iceberg.Type) (iceberg.Literal, error) {
	if v == nil {
		return nil, nil
	}
	typ, err := parseType(v.Type)
	if err != nil {
		return nil, err
	}
	lit, err := iceberg.LiteralFromBytes(typ, v.Data)
	if err != nil {
		return nil, fmt.Errorf("decode partition value of type %s: %w", v.Type, err)
	}
	if want == nil || lit.Type().Equals(want) {
		return lit, nil
	}
	converted, err := lit.To(want)
	if err != nil {
		return nil, fmt.Errorf("convert partition value %s from %s to %s: %w", lit, v.Type, want, err)
	}
	return converted, nil
}

// parseType parses an Iceberg type name such as "long" or "decimal(9, 2)".
// iceberg-go only exposes its type parser through JSON decoding of a field.
func parseType(name string) (iceberg.Type, error) {
	raw, err := json.Marshal(map[string]any{"id": 1, "name": "v", "type": name, "required": false})
	if err != nil {
		return nil, err
	}
	var field iceberg.NestedField
	if err := json.Unmarshal(raw, &field); err != nil {
		return nil, fmt.Errorf("unknown partition value type %q: %w", name, err)
	}
	return field.Type, nil
}
