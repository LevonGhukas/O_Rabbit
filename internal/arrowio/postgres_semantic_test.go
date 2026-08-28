package arrowio

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func requirePostgresTextError(t *testing.T, err error, sourceType string) {
	t.Helper()
	var conversionErr *ScalarConversionError
	require.ErrorAs(t, err, &conversionErr)
	require.Equal(t, "PostgreSQL "+sourceType+" text", conversionErr.Target)
	require.Equal(t, "invalid PostgreSQL textual representation", conversionErr.Reason)
}

func TestPostgresUUIDPolicyAndConversion(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "id", "UUID", 0, 0, false)
	require.Equal(t, arrow.BinaryTypes.String, plan.DataType)
	require.Equal(t, MappingFallback, plan.Policy.MappingKind)
	require.Equal(t, postgresUUIDTextCodec, plan.Policy.Fallback.Name)
	require.Equal(t, "uuid", plan.Policy.Metadata.Properties["postgres.semantic_type"])

	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()
	upper := "123E4567-E89B-12D3-A456-426614174000"
	require.NoError(t, plan.Append(builder, upper))
	require.NoError(t, plan.Append(builder, nil))
	requirePostgresTextError(t, plan.Append(builder, "not-a-uuid"), "UUID")

	values := builder.NewArray().(*array.String)
	defer values.Release()
	require.Equal(t, upper, values.Value(0))
	require.True(t, values.IsNull(1))
}

func TestPostgresJSONPoliciesPreserveTextAndValidate(t *testing.T) {
	tests := []struct {
		dbType string
		codec  string
		value  string
	}{
		{"JSON", postgresJSONTextCodec, `{"message":"héllo","items":[1,true,null]}`},
		{"JSONB", postgresJSONBTextCodec, `["unicode ✓", 1, false]`},
		{"JSON", postgresJSONTextCodec, `"scalar"`},
	}
	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("postgres", "payload", tt.dbType, 0, 0, false)
			require.Equal(t, MappingFallback, plan.Policy.MappingKind)
			require.Equal(t, tt.codec, plan.Policy.Fallback.Name)
			require.Equal(t, strings.ToLower(tt.dbType), plan.Policy.Metadata.Properties["postgres.semantic_type"])

			builder := plan.Builder(memory.DefaultAllocator)
			defer builder.Release()
			require.NoError(t, plan.Append(builder, []byte(tt.value)))
			require.NoError(t, plan.Append(builder, nil))
			requirePostgresTextError(t, plan.Append(builder, `{not-json}`), tt.dbType)
			values := builder.NewArray().(*array.String)
			defer values.Release()
			require.Equal(t, tt.value, values.Value(0))
			require.True(t, values.IsNull(1))
		})
	}
}

func TestPostgresXMLAndIntervalTextPolicies(t *testing.T) {
	xmlPlan := PlanForSQLColumn("postgres", "doc", "XML", 0, 0, false)
	require.Equal(t, MappingNative, xmlPlan.Policy.MappingKind)
	builder := xmlPlan.Builder(memory.DefaultAllocator)
	defer builder.Release()
	xml := `<?xml version="1.0" encoding="UTF-8"?><root>✓</root>`
	require.NoError(t, xmlPlan.Append(builder, xml))
	require.NoError(t, xmlPlan.Append(builder, nil))
	requirePostgresTextError(t, xmlPlan.Append(builder, 1), "XML")
	values := builder.NewArray().(*array.String)
	defer values.Release()
	require.Equal(t, xml, values.Value(0))
	require.True(t, values.IsNull(1))

	intervalPlan := PlanForSQLColumn("postgres", "duration", "INTERVAL", 0, 0, false)
	require.Equal(t, MappingFallback, intervalPlan.Policy.MappingKind)
	require.Equal(t, postgresIntervalTextCodec, intervalPlan.Policy.Fallback.Name)
	intervalBuilder := intervalPlan.Builder(memory.DefaultAllocator)
	defer intervalBuilder.Release()
	for _, value := range []string{"1 year 2 mons", "3 days", "-04:05:06.123456"} {
		require.NoError(t, intervalPlan.Append(intervalBuilder, value))
	}
	require.NoError(t, intervalPlan.Append(intervalBuilder, nil))
	requirePostgresTextError(t, intervalPlan.Append(intervalBuilder, ""), "INTERVAL")
	intervals := intervalBuilder.NewArray().(*array.String)
	defer intervals.Release()
	require.Equal(t, 4, intervals.Len())
	require.True(t, intervals.IsNull(3))
}

func TestPostgresNetworkAndMACPolicies(t *testing.T) {
	tests := []struct {
		dbType string
		values []string
		codec  string
	}{
		{"INET", []string{"192.0.2.1", "2001:db8::1", "192.0.2.1/24"}, postgresNetworkTextCodec},
		{"CIDR", []string{"192.0.2.0/24", "2001:db8::/32"}, postgresNetworkTextCodec},
		{"MACADDR", []string{"08:00:2b:01:02:03"}, postgresMACTextCodec},
		{"MACADDR8", []string{"08:00:2b:01:02:03:04:05"}, postgresMACTextCodec},
	}
	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("postgres", "value", tt.dbType, 0, 0, false)
			require.Equal(t, MappingFallback, plan.Policy.MappingKind)
			require.Equal(t, tt.codec, plan.Policy.Fallback.Name)
			require.Equal(t, strings.ToLower(tt.dbType), plan.Policy.Metadata.Properties["postgres.semantic_type"])
			builder := plan.Builder(memory.DefaultAllocator)
			defer builder.Release()
			for _, value := range tt.values {
				require.NoError(t, plan.Append(builder, value))
			}
			require.NoError(t, plan.Append(builder, nil))
			requirePostgresTextError(t, plan.Append(builder, "invalid"), tt.dbType)
			values := builder.NewArray().(*array.String)
			defer values.Release()
			for i, value := range tt.values {
				require.Equal(t, value, values.Value(i))
			}
			require.True(t, values.IsNull(len(tt.values)))
		})
	}
}

func TestPostgresTemporalPolicyMetadataAndTimetzFallback(t *testing.T) {
	date := PlanForSQLColumn("postgres", "date", "DATE", 0, 0, false)
	require.Equal(t, "calendar-date", date.Policy.Metadata.TemporalSemantics)
	timestamp := PlanForSQLColumn("postgres", "timestamp", "TIMESTAMP", 0, 0, false)
	require.Equal(t, "local-wall-clock", timestamp.Policy.Metadata.TemporalSemantics)
	timestamptz := PlanForSQLColumn("postgres", "timestamp", "TIMESTAMPTZ", 0, 0, false)
	require.Equal(t, "instant", timestamptz.Policy.Metadata.TemporalSemantics)
	timePlan := PlanForSQLColumn("postgres", "time", "TIME", 0, 0, false)
	require.Equal(t, "local-time", timePlan.Policy.Metadata.TemporalSemantics)

	timetz := PlanForSQLColumn("postgres", "time_with_offset", "TIMETZ", 0, 0, false)
	require.Equal(t, arrow.BinaryTypes.String, timetz.DataType)
	require.Equal(t, MappingFallback, timetz.Policy.MappingKind)
	require.Equal(t, postgresTimetzTextCodec, timetz.Policy.Fallback.Name)
	builder := timetz.Builder(memory.DefaultAllocator)
	defer builder.Release()
	require.NoError(t, timetz.Append(builder, "12:34:56+05:30"))
	requirePostgresTextError(t, timetz.Append(builder, "12:34:56"), "TIMETZ")
}

func TestPostgresSemanticMappingDiagnostics(t *testing.T) {
	plans := []ColumnPlan{
		PlanForSQLColumn("postgres", "id", "UUID", 0, 0, false),
		PlanForSQLColumn("postgres", "payload", "JSONB", 0, 0, false),
		PlanForSQLColumn("postgres", "duration", "INTERVAL", 0, 0, false),
		PlanForSQLColumn("postgres", "network", "INET", 0, 0, false),
		PlanForSQLColumn("postgres", "body", "TEXT", 0, 0, false),
	}
	diagnostics := MappingDiagnostics(plans)
	require.Len(t, diagnostics, len(plans))
	require.Equal(t, MappingFallback, diagnostics[0].MappingKind)
	require.Equal(t, postgresUUIDTextCodec, diagnostics[0].FallbackCodecName)
	require.Equal(t, postgresJSONBTextCodec, diagnostics[1].FallbackCodecName)
	require.Equal(t, postgresIntervalTextCodec, diagnostics[2].FallbackCodecName)
	require.Equal(t, postgresNetworkTextCodec, diagnostics[3].FallbackCodecName)
	require.Equal(t, MappingNative, diagnostics[4].MappingKind)
	require.Empty(t, diagnostics[4].FallbackCodecName)
}
