package connectors

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DocumentField struct {
	Name     string
	Type     string
	Nullable bool
}

type CollectionStats struct {
	RowCount   int64
	TableBytes int64
}

type DocumentIterator interface {
	Next(ctx context.Context) bool
	Decode() (map[string]any, error)
	Close() error
	Err() error
}

type DocumentReader interface {
	Close() error
	DescribeCollection(ctx context.Context, collection string) ([]DocumentField, error)
	StreamDocuments(ctx context.Context, collection string, filter map[string]any, batchSize int) (DocumentIterator, error)
	DiscoverCollectionStats(ctx context.Context, collection string) (CollectionStats, error)
	ValidateCursorColumn(ctx context.Context, collection, cursorColumn string) (CursorColumnValidation, error)
	DiscoverCursorStats(ctx context.Context, collection, cursorColumn string, domain CursorDomain) (CursorStats, error)
	BuildCursorFilter(q CursorQuery) (map[string]any, error)
}

type MongoDB struct {
	client *mongo.Client
	db     *mongo.Database
}

type mongoDocumentIterator struct {
	cur *mongo.Cursor
}

func (it *mongoDocumentIterator) Next(ctx context.Context) bool {
	return it.cur.Next(ctx)
}

func (it *mongoDocumentIterator) Decode() (map[string]any, error) {
	var doc map[string]any
	if err := it.cur.Decode(&doc); err != nil {
		return nil, err
	}
	return doc, nil
}

func (it *mongoDocumentIterator) Close() error {
	return it.cur.Close(context.Background())
}

func (it *mongoDocumentIterator) Err() error {
	return it.cur.Err()
}

const (
	mongoPingTimeout      = 10 * time.Second
	mongoStatsTimeout     = 2 * time.Minute
	mongoDescribeTimeout  = 20 * time.Second
	mongoDefaultBatchSize = 10000
)

func OpenMongoDB(ctx context.Context, dsn string) (*MongoDB, error) {
	dsn = strings.TrimSpace(dsn)

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid mongodb dsn: %w", err)
	}

	dbName := strings.TrimPrefix(u.Path, "/")
	if dbName == "" {
		return nil, fmt.Errorf("mongodb dsn must include database name")
	}

	clientOpts := options.Client().ApplyURI(dsn)
	clientOpts.SetServerSelectionTimeout(3 * time.Second)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return nil, fmt.Errorf("mongodb connect: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, mongoPingTimeout)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		_ = client.Disconnect(pingCtx)
		return nil, fmt.Errorf("mongodb ping: %w", err)
	}

	return &MongoDB{
		client: client,
		db:     client.Database(dbName),
	}, nil
}

func (m *MongoDB) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return m.client.Disconnect(ctx)
}

func (m *MongoDB) getCollection(name string) *mongo.Collection {
	parts := strings.SplitN(name, ".", 2)
	if len(parts) == 2 {
		return m.client.Database(parts[0]).Collection(parts[1])
	}
	return m.db.Collection(name)
}

func (m *MongoDB) DescribeCollection(ctx context.Context, collection string) ([]DocumentField, error) {
	vctx, cancel := context.WithTimeout(ctx, mongoDescribeTimeout)
	defer cancel()

	cursor, err := m.getCollection(collection).Find(vctx, bson.M{}, options.Find().SetLimit(100).SetSort(bson.M{"$natural": 1}))
	if err != nil {
		return nil, fmt.Errorf("mongodb describe %s: %w", collection, err)
	}
	defer cursor.Close(vctx)

	fieldTypes := map[string]string{}

	for cursor.Next(vctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		for key, val := range doc {
			if _, exists := fieldTypes[key]; !exists {
				fieldTypes[key] = mongobsonTypeName(val)
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("mongodb describe cursor: %w", err)
	}

	fields := make([]DocumentField, 0, len(fieldTypes))
	for key := range fieldTypes {
		fields = append(fields, DocumentField{
			Name: key,
			Type: fieldTypes[key],
		})
	}
	if fields == nil {
		return []DocumentField{}, nil
	}
	return fields, nil
}

func mongobsonTypeName(v any) string {
	if v == nil {
		return "NULL"
	}
	switch v.(type) {
	case string:
		return "STRING"
	case int32, int64, int:
		return "INT64"
	case float64, float32:
		return "FLOAT64"
	case bool:
		return "BOOL"
	case bson.M, map[string]any:
		return "DOCUMENT"
	case primitive.A, []any:
		return "ARRAY"
	case time.Time, primitive.DateTime:
		return "TIMESTAMP"
	case primitive.ObjectID:
		return "OBJECTID"
	case []byte:
		return "BINARY"
	case primitive.Binary:
		return "BINARY"
	case primitive.Decimal128:
		return "DECIMAL"
	default:
		return "STRING"
	}
}

func (m *MongoDB) StreamDocuments(ctx context.Context, collection string, filter map[string]any, batchSize int) (DocumentIterator, error) {
	if filter == nil {
		filter = bson.M{}
	}
	if batchSize <= 0 {
		batchSize = mongoDefaultBatchSize
	}

	findOpts := options.Find().SetBatchSize(int32(batchSize))
	cur, err := m.getCollection(collection).Find(ctx, filter, findOpts)
	if err != nil {
		return nil, fmt.Errorf("mongodb stream %s: %w", collection, err)
	}
	return &mongoDocumentIterator{cur: cur}, nil
}

func (m *MongoDB) DiscoverCollectionStats(ctx context.Context, collection string) (CollectionStats, error) {
	qctx, cancel := context.WithTimeout(ctx, mongoStatsTimeout)
	defer cancel()

	var out CollectionStats

	count, err := m.getCollection(collection).EstimatedDocumentCount(qctx)
	if err == nil {
		out.RowCount = count
	}

	statsCmd := bson.M{"collStats": collection}
	var stats bson.M
	if err := m.db.RunCommand(qctx, statsCmd).Decode(&stats); err == nil {
		if size, ok := stats["size"].(int64); ok {
			out.TableBytes = size
		}
	}

	return out, nil
}

func (m *MongoDB) ValidateCursorColumn(ctx context.Context, collection, cursorColumn string) (CursorColumnValidation, error) {
	vctx, cancel := context.WithTimeout(ctx, mongoDescribeTimeout)
	defer cancel()

	// To avoid unbounded collection scans (COLLSCAN) which can crash MongoDB on large datasets,
	// we rely on the safe sampled schema from DescribeCollection instead of querying with $exists.
	fields, err := m.DescribeCollection(vctx, collection)
	if err != nil {
		return CursorColumnValidation{}, fmt.Errorf("mongodb validate cursor: describe failed: %w", err)
	}

	// 1. Try to find the cursor column in the sampled schema
	for _, field := range fields {
		if field.Name == cursorColumn {
			domain := CursorDomainString
			rangeCapable := false

			switch field.Type {
			case "OBJECTID", "STRING", "UUID":
				domain = CursorDomainString
				rangeCapable = true
			case "INT64":
				domain = CursorDomainInt64
				rangeCapable = true
			case "FLOAT64", "DECIMAL":
				domain = CursorDomainDecimal
				rangeCapable = true
			case "TIMESTAMP", "DATE":
				domain = CursorDomainTimestamp
				rangeCapable = true
			}

			return CursorColumnValidation{
				Found:         true,
				ResolvedName:  cursorColumn,
				DataType:      field.Type,
				Domain:        domain,
				Orderable:     true,
				RangeCapable:  rangeCapable,
				Nullable:      false,
				NullableKnown: false,
				Indexed:       true,
				IndexedKnown:  false,
			}, nil
		}
	}

	// 2. If it's not in the sample (or collection is empty), we safely fallback to string/objectid
	// without doing an expensive COLLSCAN.
	dt := "STRING"
	if cursorColumn == "_id" {
		dt = "OBJECTID"
	}
	return CursorColumnValidation{
		Found:         true,
		ResolvedName:  cursorColumn,
		DataType:      dt,
		Domain:        CursorDomainString,
		Orderable:     true,
		RangeCapable:  false, // Disable stats discovery if we can't confirm the column exists
		Nullable:      false,
		NullableKnown: false,
		Indexed:       true,
		IndexedKnown:  false,
	}, nil
}

func (m *MongoDB) DiscoverCursorStats(ctx context.Context, collection, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	sctx, cancel := context.WithTimeout(ctx, mongoStatsTimeout)
	defer cancel()

	out := CursorStats{}
	cStats, err := m.DiscoverCollectionStats(sctx, collection)
	if err == nil {
		out.RowCount = cStats.RowCount
		out.TableBytes = cStats.TableBytes
	}

	// Min Value
	var minDoc bson.M
	err = m.getCollection(collection).FindOne(sctx, bson.M{cursorColumn: bson.M{"$exists": true, "$ne": nil}}, options.FindOne().SetSort(bson.M{cursorColumn: 1})).Decode(&minDoc)
	if err == nil {
		if minVal, ok := minDoc[cursorColumn]; ok {
			out.MinValue = fmt.Sprint(minVal)
			if oid, isOid := minVal.(primitive.ObjectID); isOid {
				out.MinValue = oid.Hex()
			} else if d, isDec := minVal.(primitive.Decimal128); isDec {
				out.MinValue = d.String()
			} else if t, isTime := minVal.(time.Time); isTime {
				out.MinValue = t.UTC().Format(time.RFC3339Nano)
			} else if dt, isDateTime := minVal.(primitive.DateTime); isDateTime {
				out.MinValue = dt.Time().UTC().Format(time.RFC3339Nano)
			}
		}
	} else if err != mongo.ErrNoDocuments {
		return CursorStats{}, fmt.Errorf("mongo schema discovery failed: collection=%s field=%s reason=failed to discover MinValue: %w", collection, cursorColumn, err)
	}

	// Max Value
	var maxDoc bson.M
	err = m.getCollection(collection).FindOne(sctx, bson.M{cursorColumn: bson.M{"$exists": true, "$ne": nil}}, options.FindOne().SetSort(bson.M{cursorColumn: -1})).Decode(&maxDoc)
	if err == nil {
		if maxVal, ok := maxDoc[cursorColumn]; ok {
			out.MaxValue = fmt.Sprint(maxVal)
			if oid, isOid := maxVal.(primitive.ObjectID); isOid {
				out.MaxValue = oid.Hex()
			} else if d, isDec := maxVal.(primitive.Decimal128); isDec {
				out.MaxValue = d.String()
			} else if t, isTime := maxVal.(time.Time); isTime {
				out.MaxValue = t.UTC().Format(time.RFC3339Nano)
			} else if dt, isDateTime := maxVal.(primitive.DateTime); isDateTime {
				out.MaxValue = dt.Time().UTC().Format(time.RFC3339Nano)
			}
		}
	} else if err != mongo.ErrNoDocuments {
		return CursorStats{}, fmt.Errorf("mongo schema discovery failed: collection=%s field=%s reason=failed to discover MaxValue: %w", collection, cursorColumn, err)
	}

	return out, nil
}

func (m *MongoDB) BuildCursorFilter(q CursorQuery) (map[string]any, error) {
	if q.CursorColumn == "" {
		return nil, nil
	}
	cond := make(map[string]any)
	if q.LowerBound != "" {
		val, err := ParseCursorArgument(q.CursorDomain, q.LowerBound)
		if err != nil {
			return nil, err
		}
		if strVal, ok := val.(string); ok {
			if oid, err := primitive.ObjectIDFromHex(strVal); err == nil {
				val = oid
			}
		}
		if q.LowerExclusive {
			cond["$gt"] = val
		} else {
			cond["$gte"] = val
		}
	}
	if q.UpperBound != "" {
		val, err := ParseCursorArgument(q.CursorDomain, q.UpperBound)
		if err != nil {
			return nil, err
		}
		if strVal, ok := val.(string); ok {
			if oid, err := primitive.ObjectIDFromHex(strVal); err == nil {
				val = oid
			}
		}
		if q.UpperInclusive {
			cond["$lte"] = val
		} else {
			cond["$lt"] = val
		}
	}
	if len(cond) == 0 {
		return nil, nil
	}
	return map[string]any{q.CursorColumn: cond}, nil
}
