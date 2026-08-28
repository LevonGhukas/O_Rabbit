package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func TestTypePolicyValidate(t *testing.T) {
	tests := []struct {
		name    string
		policy  TypePolicy
		wantErr bool
	}{
		{
			name:   "native",
			policy: TypePolicy{Version: MappingPolicyVersionV1, SourceEngine: "postgres", SourceType: "BIGINT", MappingKind: MappingNative},
		},
		{
			name:   "fallback with codec",
			policy: TypePolicy{Version: MappingPolicyVersionV1, SourceEngine: "postgres", SourceType: "my_extension", MappingKind: MappingFallback, Fallback: &FallbackCodec{Name: genericTextFallbackCodec, Version: 1}},
		},
		{
			name:    "fallback without codec",
			policy:  TypePolicy{Version: MappingPolicyVersionV1, SourceEngine: "postgres", SourceType: "my_extension", MappingKind: MappingFallback},
			wantErr: true,
		},
		{
			name:    "native with codec",
			policy:  TypePolicy{Version: MappingPolicyVersionV1, SourceEngine: "postgres", SourceType: "BIGINT", MappingKind: MappingNative, Fallback: &FallbackCodec{Name: genericTextFallbackCodec, Version: 1}},
			wantErr: true,
		},
		{
			name:    "unknown mapping kind",
			policy:  TypePolicy{Version: MappingPolicyVersionV1, SourceEngine: "postgres", SourceType: "BIGINT", MappingKind: "unknown"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.policy.Validate()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestSQLPlansAttachTypePolicyWithoutChangingArrowType(t *testing.T) {
	tests := []struct {
		name       string
		engine     string
		dbType     string
		wantType   arrow.DataType
		wantKind   MappingKind
		wantEngine string
		wantCodec  bool
	}{
		{name: "postgres bigint", engine: "postgres", dbType: "BIGINT", wantType: arrow.PrimitiveTypes.Int64, wantKind: MappingNative, wantEngine: "postgres"},
		{name: "postgres unknown string fallback", engine: "postgres", dbType: "my_extension", wantType: arrow.BinaryTypes.String, wantKind: MappingFallback, wantEngine: "postgres", wantCodec: true},
		{name: "mysql varchar native string", engine: "mysql", dbType: "VARCHAR(255)", wantType: arrow.BinaryTypes.String, wantKind: MappingNative, wantEngine: "mysql"},
		{name: "mariadb varchar keeps identity", engine: "mariadb", dbType: "VARCHAR(255)", wantType: arrow.BinaryTypes.String, wantKind: MappingNative, wantEngine: "mariadb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := PlanForSQLColumn(tt.engine, "col", tt.dbType, 0, 0, false)
			require.True(t, arrow.TypeEqual(tt.wantType, plan.DataType))
			require.NotNil(t, plan.Policy)
			require.Equal(t, MappingPolicyVersionV1, plan.Policy.Version)
			require.Equal(t, tt.wantKind, plan.Policy.MappingKind)
			require.Equal(t, tt.wantEngine, plan.Policy.SourceEngine)
			require.Equal(t, tt.dbType, plan.Policy.SourceType)
			require.Equal(t, tt.wantCodec, plan.Policy.Fallback != nil)
			require.NoError(t, plan.Policy.Validate())
		})
	}
}

func TestMappingDiagnosticsExposePolicyWithoutRowValues(t *testing.T) {
	plans := []ColumnPlan{
		PlanForSQLColumn("postgres", "extension_value", "my_extension", 0, 0, false),
		PlanForSQLColumn("mysql", "amount", "DECIMAL(39,10)", 0, 0, false),
		PlanForSQLColumn("mysql", "description", "VARCHAR(255)", 0, 0, false),
		PlanForSQLColumn("mariadb", "label", "VARCHAR(255)", 0, 0, false),
	}

	diagnostics := MappingDiagnostics(plans)
	require.Len(t, diagnostics, 4)
	require.Equal(t, "extension_value", diagnostics[0].ColumnName)
	require.Equal(t, MappingFallback, diagnostics[0].MappingKind)
	require.Equal(t, "postgres", diagnostics[0].SourceEngine)
	require.Equal(t, postgresExtensionTextCodec, diagnostics[0].FallbackCodecName)
	require.Equal(t, 1, diagnostics[0].FallbackCodecVersion)
	require.Equal(t, MappingFallback, diagnostics[1].MappingKind)
	require.Equal(t, canonicalDecimalTextFallbackCodec, diagnostics[1].FallbackCodecName)
	require.Equal(t, MappingNative, diagnostics[2].MappingKind)
	require.Empty(t, diagnostics[2].FallbackCodecName)
	require.Equal(t, "mariadb", diagnostics[3].SourceEngine)
}
