package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	FormatVersion        = 1
	VerificationPortable = "PORTABLE_FULL_SHA256"
	VerificationProvider = "PROVIDER_SHA256_AND_PORTABLE_FULL_SHA256"
	VerificationVerified = "VERIFIED"
	streamBufferSize     = 64 * 1024
)

type Record struct {
	ObjectKey          string `json:"object_key"`
	ByteSize           int64  `json:"byte_size"`
	SHA256             string `json:"sha256"`
	RowCount           int64  `json:"row_count"`
	SchemaFingerprint  string `json:"schema_fingerprint"`
	RunID              string `json:"run_id"`
	TaskID             string `json:"task_id"`
	AttemptID          string `json:"attempt_id"`
	AttemptNumber      int    `json:"attempt_number"`
	FileIndex          int    `json:"file_index"`
	FormatVersion      int    `json:"format_version"`
	VerificationMethod string `json:"verification_method"`
	VerificationStatus string `json:"verification_status"`
	VerifiedAt         string `json:"verified_at,omitempty"`
	MaxHWM             string `json:"max_hwm,omitempty"`
}

func (r Record) Validate() error {
	if strings.TrimSpace(r.ObjectKey) == "" || strings.TrimSpace(r.RunID) == "" || strings.TrimSpace(r.TaskID) == "" || strings.TrimSpace(r.AttemptID) == "" {
		return fmt.Errorf("artifact identity is incomplete")
	}
	if r.ByteSize <= 0 || r.RowCount < 0 || r.AttemptNumber <= 0 || r.FileIndex < 0 {
		return fmt.Errorf("artifact numeric fields are invalid")
	}
	if len(r.SHA256) != 64 || len(r.SchemaFingerprint) != 64 {
		return fmt.Errorf("artifact digests must be lowercase SHA-256 hex")
	}
	if _, err := hex.DecodeString(r.SHA256); err != nil || strings.ToLower(r.SHA256) != r.SHA256 {
		return fmt.Errorf("invalid artifact sha256")
	}
	if _, err := hex.DecodeString(r.SchemaFingerprint); err != nil || strings.ToLower(r.SchemaFingerprint) != r.SchemaFingerprint {
		return fmt.Errorf("invalid schema fingerprint")
	}
	if r.FormatVersion != FormatVersion || (r.VerificationMethod != VerificationPortable && r.VerificationMethod != VerificationProvider) || r.VerificationStatus != VerificationVerified {
		return fmt.Errorf("artifact verification is insufficient")
	}
	return nil
}

type LocalInfo struct {
	ByteSize          int64
	SHA256            string
	RowCount          int64
	SchemaFingerprint string
}

func ValidateLocalParquet(ctx context.Context, path string, expectedRows int64, expectedSchema *arrow.Schema) (LocalInfo, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return LocalInfo{}, &Failure{Classification: FailureLocalParquetInvalid, Err: err}
	}
	if fi.Size() <= 0 {
		return LocalInfo{}, &Failure{Classification: FailureLocalSizeMismatch, Err: fmt.Errorf("local parquet is empty")}
	}
	f, err := os.Open(path)
	if err != nil {
		return LocalInfo{}, err
	}
	digest, size, err := StreamSHA256(ctx, f)
	_ = f.Close()
	if err != nil {
		class := FailureLocalParquetInvalid
		if ctx.Err() != nil {
			class = FailureVerificationCanceled
		}
		return LocalInfo{}, &Failure{Classification: class, Err: err}
	}
	if size != fi.Size() {
		return LocalInfo{}, &Failure{Classification: FailureLocalSizeMismatch, Err: fmt.Errorf("hashed size=%d stat size=%d", size, fi.Size())}
	}
	pr, err := file.OpenParquetFile(path, false)
	if err != nil {
		return LocalInfo{}, &Failure{Classification: FailureLocalParquetInvalid, Err: fmt.Errorf("invalid parquet footer: %w", err)}
	}
	defer pr.Close()
	rows := pr.NumRows()
	if rows != expectedRows {
		return LocalInfo{}, &Failure{Classification: FailureLocalRowCountMismatch, Err: fmt.Errorf("parquet row count mismatch: physical=%d expected=%d", rows, expectedRows)}
	}
	actualSchema, err := pqarrow.FromParquet(pr.MetaData().Schema, &pqarrow.ArrowReadProperties{}, pr.MetaData().KeyValueMetadata())
	if err != nil {
		return LocalInfo{}, fmt.Errorf("read parquet schema: %w", err)
	}
	actualFP, err := SchemaFingerprint(actualSchema)
	if err != nil {
		return LocalInfo{}, err
	}
	expectedFP, err := SchemaFingerprint(expectedSchema)
	if err != nil {
		return LocalInfo{}, err
	}
	if actualFP != expectedFP {
		return LocalInfo{}, &Failure{Classification: FailureLocalSchemaMismatch, Err: fmt.Errorf("parquet schema fingerprint mismatch: actual=%s expected=%s", actualFP, expectedFP)}
	}
	return LocalInfo{ByteSize: size, SHA256: digest, RowCount: rows, SchemaFingerprint: actualFP}, nil
}

func StreamSHA256(ctx context.Context, r io.Reader) (string, int64, error) {
	h := sha256.New()
	buf := make([]byte, streamBufferSize)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return "", total, err
		}
		n, err := r.Read(buf)
		if n > 0 {
			_, _ = h.Write(buf[:n])
			total += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", total, err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), total, nil
}

func VerifyStream(ctx context.Context, r io.Reader, expectedSize int64, expectedSHA256 string) error {
	digest, size, err := StreamSHA256(ctx, r)
	if err != nil {
		return err
	}
	if size != expectedSize {
		return fmt.Errorf("artifact size mismatch: got=%d expected=%d", size, expectedSize)
	}
	if digest != expectedSHA256 {
		return fmt.Errorf("artifact sha256 mismatch")
	}
	return nil
}

type canonicalField struct {
	Name     string        `json:"name"`
	Nullable bool          `json:"nullable"`
	Type     canonicalType `json:"type"`
}
type canonicalType struct {
	Kind       string           `json:"kind"`
	Unit       string           `json:"unit,omitempty"`
	TimeZone   string           `json:"timezone,omitempty"`
	Precision  int32            `json:"precision,omitempty"`
	Scale      int32            `json:"scale,omitempty"`
	ByteWidth  int32            `json:"byte_width,omitempty"`
	Fields     []canonicalField `json:"fields,omitempty"`
	Element    *canonicalField  `json:"element,omitempty"`
	Key        *canonicalField  `json:"key,omitempty"`
	Item       *canonicalField  `json:"item,omitempty"`
	KeysSorted bool             `json:"keys_sorted,omitempty"`
}

func SchemaFingerprint(schema *arrow.Schema) (string, error) {
	if schema == nil {
		return "", fmt.Errorf("nil arrow schema")
	}
	fields := make([]canonicalField, len(schema.Fields()))
	for i, field := range schema.Fields() {
		fields[i] = canonicalizeField(field)
	}
	b, err := json.Marshal(struct {
		Version int              `json:"version"`
		Fields  []canonicalField `json:"fields"`
	}{1, fields})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func canonicalizeField(f arrow.Field) canonicalField {
	return canonicalField{Name: f.Name, Nullable: f.Nullable, Type: canonicalizeType(f.Type)}
}
func canonicalizeType(dt arrow.DataType) canonicalType {
	out := canonicalType{Kind: dt.ID().String()}
	switch t := dt.(type) {
	case *arrow.TimestampType:
		out.Unit, out.TimeZone = t.Unit.String(), t.TimeZone
	case *arrow.Time32Type:
		out.Unit = t.Unit.String()
	case *arrow.Time64Type:
		out.Unit = t.Unit.String()
	case *arrow.DurationType:
		out.Unit = t.Unit.String()
	case *arrow.Decimal32Type:
		out.Precision, out.Scale = t.Precision, t.Scale
	case *arrow.Decimal64Type:
		out.Precision, out.Scale = t.Precision, t.Scale
	case *arrow.Decimal128Type:
		out.Precision, out.Scale = t.Precision, t.Scale
	case *arrow.Decimal256Type:
		out.Precision, out.Scale = t.Precision, t.Scale
	case *arrow.FixedSizeBinaryType:
		out.ByteWidth = int32(t.ByteWidth)
	case *arrow.StructType:
		out.Fields = make([]canonicalField, len(t.Fields()))
		for i, f := range t.Fields() {
			out.Fields[i] = canonicalizeField(f)
		}
	case *arrow.ListType:
		f := canonicalizeField(t.ElemField())
		out.Element = &f
	case *arrow.LargeListType:
		f := canonicalizeField(t.ElemField())
		out.Element = &f
	case *arrow.FixedSizeListType:
		f := canonicalizeField(t.ElemField())
		out.Element = &f
		out.ByteWidth = t.Len()
	case *arrow.MapType:
		k, v := canonicalizeField(t.KeyField()), canonicalizeField(t.ItemField())
		out.Key, out.Item, out.KeysSorted = &k, &v, t.KeysSorted
	}
	return out
}
