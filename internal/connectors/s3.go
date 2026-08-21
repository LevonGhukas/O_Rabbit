package connectors

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/xuri/excelize/v2"
)

type S3Reader struct {
	client    *s3.Client
	bucket    string
	key       string
	format    string // "csv", "json", "xml", "excel", "parquet"
	objectRes *s3.GetObjectOutput
}

// OpenS3 opens an S3 document reader.
// dsn expected format: s3://bucket/path/to/key.ext
func OpenS3(ctx context.Context, dsn string) (DocumentReader, error) {
	if !strings.HasPrefix(dsn, "s3://") {
		return nil, fmt.Errorf("invalid s3 dsn: %s", dsn)
	}
	parts := strings.SplitN(dsn[5:], "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid s3 dsn structure, expected s3://bucket/key: %s", dsn)
	}
	bucket, key := parts[0], parts[1]

	endpoint := os.Getenv("ORABBIT_DEFAULT_S3_ENDPOINT")
	accessKey := os.Getenv("ORABBIT_DEFAULT_S3_ACCESS_KEY_ID")
	secretKey := os.Getenv("ORABBIT_DEFAULT_S3_SECRET_ACCESS_KEY")
	region := os.Getenv("ORABBIT_DEFAULT_S3_REGION")
	if region == "" {
		region = "us-east-1"
	}
	forcePathStyle := strings.ToLower(os.Getenv("ORABBIT_S3_FORCE_PATH_STYLE")) == "true"

	if accessKey == "" || secretKey == "" {
		return nil, errors.New("missing S3 credentials in environment")
	}

	creds := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
	awsCfg := aws.Config{
		Region:      region,
		Credentials: aws.NewCredentialsCache(creds),
	}
	if endpoint != "" {
		awsCfg.BaseEndpoint = aws.String(endpoint)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = forcePathStyle
	})

	ext := strings.ToLower(filepath.Ext(key))
	format := "csv"
	switch ext {
	case ".json":
		format = "json"
	case ".xml":
		format = "xml"
	case ".xlsx", ".xls":
		format = "excel"
	case ".parquet":
		format = "parquet"
	}

	return &S3Reader{
		client: client,
		bucket: bucket,
		key:    key,
		format: format,
	}, nil
}

func (r *S3Reader) Close() error {
	if r.objectRes != nil && r.objectRes.Body != nil {
		return r.objectRes.Body.Close()
	}
	return nil
}

func (r *S3Reader) DescribeCollection(ctx context.Context, collection string) ([]DocumentField, error) {
	// For S3 files, the "collection" is implicitly the key.
	// Since we are creating a generic parser and O_Rabbit supports schema inference from DocumentIterator,
	// we return empty here and let the planner/worker infer it from the StreamDocuments output.
	return nil, nil
}

func (r *S3Reader) StreamDocuments(ctx context.Context, collection string, filter map[string]any, batchSize int) (DocumentIterator, error) {
	out, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get s3 object: %w", err)
	}
	r.objectRes = out

	switch r.format {
	case "csv":
		csvReader := csv.NewReader(out.Body)
		// For standard CSV uploads, fields might contain quotes or have unequal lengths.
		csvReader.FieldsPerRecord = -1
		csvReader.LazyQuotes = true

		headers, err := csvReader.Read()
		if err != nil {
			return nil, fmt.Errorf("failed to read csv headers: %w", err)
		}

		return &s3CSVIterator{
			reader:  csvReader,
			headers: headers,
		}, nil
	case "json":
		decoder := json.NewDecoder(out.Body)
		return &s3JSONIterator{
			decoder: decoder,
		}, nil
	case "xml":
		decoder := xml.NewDecoder(out.Body)
		return &s3XMLIterator{
			decoder: decoder,
		}, nil
	case "excel":
		f, err := excelize.OpenReader(out.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to open excel file: %w", err)
		}
		// Default to first sheet
		sheets := f.GetSheetList()
		if len(sheets) == 0 {
			return nil, errors.New("excel file contains no sheets")
		}
		rows, err := f.Rows(sheets[0])
		if err != nil {
			return nil, fmt.Errorf("failed to read excel rows: %w", err)
		}

		var headers []string
		if rows.Next() {
			headers, err = rows.Columns()
			if err != nil {
				return nil, fmt.Errorf("failed to read excel headers: %w", err)
			}
		} else {
			return nil, errors.New("excel sheet is empty")
		}

		return &s3ExcelIterator{
			rows:    rows,
			headers: headers,
		}, nil
	case "parquet":
		data, err := io.ReadAll(out.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read parquet body: %w", err)
		}
		parquetReader, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("read parquet metadata: %w", err)
		}
		arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 2048}, memory.DefaultAllocator)
		if err != nil {
			return nil, fmt.Errorf("open parquet arrow reader: %w", err)
		}
		recReader, err := arrowReader.GetRecordReader(ctx, nil, nil)
		if err != nil {
			return nil, fmt.Errorf("get parquet record reader: %w", err)
		}
		return &s3ParquetIterator{
			recReader: recReader,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported S3 file format: %s", r.format)
	}
}

func (r *S3Reader) DiscoverCollectionStats(ctx context.Context, collection string) (CollectionStats, error) {
	out, err := r.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(r.bucket),
		Key:    aws.String(r.key),
	})
	if err != nil {
		return CollectionStats{}, err
	}
	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return CollectionStats{
		TableBytes: size,
		RowCount:   size / 100, // Very rough heuristic for auto-tuning
	}, nil
}

func (r *S3Reader) ValidateCursorColumn(ctx context.Context, collection, cursorColumn string) (CursorColumnValidation, error) {
	return CursorColumnValidation{Found: false}, nil
}

func (r *S3Reader) DiscoverCursorStats(ctx context.Context, collection, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	return CursorStats{}, nil
}

func (r *S3Reader) BuildCursorFilter(q CursorQuery) (map[string]any, error) {
	return nil, nil
}

// Iterator implementations

type s3CSVIterator struct {
	reader     *csv.Reader
	headers    []string
	nextRecord []string
	err        error
}

func (it *s3CSVIterator) Next(ctx context.Context) bool {
	record, err := it.reader.Read()
	if err != nil {
		if err != io.EOF {
			it.err = err
		}
		return false
	}
	it.nextRecord = record
	return true
}

func (it *s3CSVIterator) Decode() (map[string]any, error) {
	doc := make(map[string]any, len(it.headers))
	for i, header := range it.headers {
		val := ""
		if i < len(it.nextRecord) {
			val = it.nextRecord[i]
		}
		if val == "" {
			doc[header] = nil
		} else if iVal, err := strconv.ParseInt(val, 10, 64); err == nil {
			doc[header] = iVal
		} else if fVal, err := strconv.ParseFloat(val, 64); err == nil {
			doc[header] = fVal
		} else {
			doc[header] = val
		}
	}
	return doc, nil
}

func (it *s3CSVIterator) Err() error {
	return it.err
}

func (it *s3CSVIterator) Close() error {
	return nil
}

type s3JSONIterator struct {
	decoder *json.Decoder
	inArray bool
	nextDoc map[string]any
	err     error
}

func (it *s3JSONIterator) Next(ctx context.Context) bool {
	if !it.inArray {
		// Expect '[' token for top-level array
		t, err := it.decoder.Token()
		if err != nil {
			if err != io.EOF {
				it.err = err
			}
			return false
		}
		if delim, ok := t.(json.Delim); ok && delim == '[' {
			it.inArray = true
		} else {
			it.err = errors.New("expected top level JSON array")
			return false
		}
	}

	if !it.decoder.More() {
		// End of array
		it.decoder.Token() // consume ']'
		return false
	}

	var doc map[string]any
	if err := it.decoder.Decode(&doc); err != nil {
		it.err = err
		return false
	}
	it.nextDoc = doc
	return true
}

func (it *s3JSONIterator) Decode() (map[string]any, error) {
	return it.nextDoc, nil
}

func (it *s3JSONIterator) Err() error {
	return it.err
}

func (it *s3JSONIterator) Close() error {
	return nil
}

func parseScalar(val string) any {
	val = strings.TrimSpace(val)
	if val == "" {
		return nil
	}
	if iVal, err := strconv.ParseInt(val, 10, 64); err == nil {
		return iVal
	}
	if fVal, err := strconv.ParseFloat(val, 64); err == nil {
		return fVal
	}
	return val
}

type s3XMLIterator struct {
	decoder *xml.Decoder
	depth   int
	nextDoc map[string]any
	err     error
}

func (it *s3XMLIterator) Next(ctx context.Context) bool {
	for {
		t, err := it.decoder.Token()
		if err != nil {
			if err != io.EOF {
				it.err = err
			}
			return false
		}

		switch elem := t.(type) {
		case xml.StartElement:
			it.depth++
			if it.depth == 2 {
				// Record-level element (e.g. <row>, <item>, <record>)
				doc := make(map[string]any)
				for _, attr := range elem.Attr {
					doc[attr.Name.Local] = parseScalar(attr.Value)
				}

				recordDepth := 1
				var currentChildTag string
				var currentChildText strings.Builder
				hasChildren := false

				for recordDepth > 0 {
					subT, subErr := it.decoder.Token()
					if subErr != nil {
						if subErr != io.EOF {
							it.err = subErr
						}
						return false
					}

					switch subElem := subT.(type) {
					case xml.StartElement:
						recordDepth++
						if recordDepth == 2 {
							hasChildren = true
							currentChildTag = subElem.Name.Local
							currentChildText.Reset()
							for _, attr := range subElem.Attr {
								doc[currentChildTag+"_"+attr.Name.Local] = parseScalar(attr.Value)
							}
						}
					case xml.CharData:
						if recordDepth == 2 {
							currentChildText.Write(subElem)
						}
					case xml.EndElement:
						if recordDepth == 2 && currentChildTag != "" {
							doc[currentChildTag] = parseScalar(currentChildText.String())
							currentChildTag = ""
						}
						recordDepth--
					}
				}

				it.depth-- // Account for the record end element consumed in loop
				if len(doc) > 0 || hasChildren {
					it.nextDoc = doc
					return true
				}
			}
		case xml.EndElement:
			it.depth--
		}
	}
}

func (it *s3XMLIterator) Decode() (map[string]any, error) {
	return it.nextDoc, nil
}

func (it *s3XMLIterator) Err() error {
	return it.err
}

func (it *s3XMLIterator) Close() error {
	return nil
}

type s3ExcelIterator struct {
	rows       *excelize.Rows
	headers    []string
	nextRecord []string
	err        error
}

func (it *s3ExcelIterator) Next(ctx context.Context) bool {
	if !it.rows.Next() {
		return false
	}
	record, err := it.rows.Columns()
	if err != nil {
		it.err = err
		return false
	}
	it.nextRecord = record
	return true
}

func (it *s3ExcelIterator) Decode() (map[string]any, error) {
	doc := make(map[string]any, len(it.headers))
	for i, header := range it.headers {
		val := ""
		if i < len(it.nextRecord) {
			val = it.nextRecord[i]
		}
		if val == "" {
			doc[header] = nil
		} else if iVal, err := strconv.ParseInt(val, 10, 64); err == nil {
			doc[header] = iVal
		} else if fVal, err := strconv.ParseFloat(val, 64); err == nil {
			doc[header] = fVal
		} else {
			doc[header] = val
		}
	}
	return doc, nil
}

func (it *s3ExcelIterator) Err() error {
	return it.err
}

func (it *s3ExcelIterator) Close() error {
	return nil
}

func arrowValueToAny(col arrow.Array, row int) any {
	if col.IsNull(row) {
		return nil
	}
	switch arr := col.(type) {
	case *array.Boolean:
		return arr.Value(row)
	case *array.Int8:
		return int64(arr.Value(row))
	case *array.Int16:
		return int64(arr.Value(row))
	case *array.Int32:
		return int64(arr.Value(row))
	case *array.Int64:
		return arr.Value(row)
	case *array.Uint8:
		return uint64(arr.Value(row))
	case *array.Uint16:
		return uint64(arr.Value(row))
	case *array.Uint32:
		return uint64(arr.Value(row))
	case *array.Uint64:
		return arr.Value(row)
	case *array.Float32:
		return float64(arr.Value(row))
	case *array.Float64:
		return arr.Value(row)
	case *array.String:
		return arr.Value(row)
	case *array.LargeString:
		return arr.Value(row)
	case *array.Binary:
		return arr.Value(row)
	case *array.LargeBinary:
		return arr.Value(row)
	case *array.Date32:
		return arr.Value(row).ToTime().Format("2006-01-02")
	case *array.Date64:
		return arr.Value(row).ToTime().Format("2006-01-02")
	case *array.Timestamp:
		unit := arr.DataType().(*arrow.TimestampType).Unit
		return arr.Value(row).ToTime(unit).Format(time.RFC3339Nano)
	case *array.Time32:
		return arr.Value(row).FormattedString(arrow.Nanosecond)
	case *array.Time64:
		return arr.Value(row).FormattedString(arrow.Nanosecond)
	default:
		return col.ValueStr(row)
	}
}

type s3ParquetIterator struct {
	recReader  pqarrow.RecordReader
	currentRec arrow.RecordBatch
	rowIndex   int
	numRows    int
	schema     *arrow.Schema
	nextDoc    map[string]any
	err        error
}

func (it *s3ParquetIterator) Next(ctx context.Context) bool {
	for it.currentRec == nil || it.rowIndex >= it.numRows {
		if it.currentRec != nil {
			it.currentRec.Release()
			it.currentRec = nil
		}
		if it.recReader == nil {
			return false
		}
		if !it.recReader.Next() {
			if err := it.recReader.Err(); err != nil && err != io.EOF {
				it.err = err
			}
			return false
		}
		rec := it.recReader.Record()
		if rec == nil || rec.NumRows() == 0 {
			continue
		}
		rec.Retain()
		it.currentRec = rec
		it.numRows = int(rec.NumRows())
		it.rowIndex = 0
		it.schema = rec.Schema()
	}

	doc := make(map[string]any, it.schema.NumFields())
	for colIdx := 0; colIdx < it.schema.NumFields(); colIdx++ {
		field := it.schema.Field(colIdx)
		col := it.currentRec.Column(colIdx)
		doc[field.Name] = arrowValueToAny(col, it.rowIndex)
	}
	it.rowIndex++
	it.nextDoc = doc
	return true
}

func (it *s3ParquetIterator) Decode() (map[string]any, error) {
	return it.nextDoc, nil
}

func (it *s3ParquetIterator) Err() error {
	return it.err
}

func (it *s3ParquetIterator) Close() error {
	if it.currentRec != nil {
		it.currentRec.Release()
		it.currentRec = nil
	}
	if it.recReader != nil {
		it.recReader.Release()
		it.recReader = nil
	}
	return nil
}
