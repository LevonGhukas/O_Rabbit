package typesystem

import (
	"encoding/base64"
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDereference(t *testing.T) {
	value := 42
	pointer := &value
	nested := &pointer
	var nilPointer *int

	tests := []struct {
		name  string
		input any
		want  any
	}{
		{"nil", nil, nil},
		{"typed nil", nilPointer, nil},
		{"pointer", pointer, 42},
		{"nested pointer", nested, 42},
		{"interface pointer", any(pointer), 42},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, Dereference(test.input))
		})
	}
}

func TestSignedIntegerConversions(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		target LogicalType
		want   any
		valid  bool
	}{
		{"int8 max", 127, LogicalType{Kind: KindInt8}, int8(127), true},
		{"int8 min", -128, LogicalType{Kind: KindInt8}, int8(-128), true},
		{"int8 overflow", 128, LogicalType{Kind: KindInt8}, nil, false},
		{"int8 underflow", -129, LogicalType{Kind: KindInt8}, nil, false},
		{"int16 max", "32767", LogicalType{Kind: KindInt16}, int16(32767), true},
		{"int16 overflow", "32768", LogicalType{Kind: KindInt16}, nil, false},
		{"int32 max", int64(math.MaxInt32), LogicalType{Kind: KindInt32}, int32(math.MaxInt32), true},
		{"int32 overflow", int64(math.MaxInt32) + 1, LogicalType{Kind: KindInt32}, nil, false},
		{"int64 max uint", uint64(math.MaxInt64), LogicalType{Kind: KindInt64}, int64(math.MaxInt64), true},
		{"int64 uint overflow", uint64(math.MaxInt64) + 1, LogicalType{Kind: KindInt64}, nil, false},
		{"trimmed bytes", []byte(" 12 "), LogicalType{Kind: KindInt8}, int8(12), true},
		{"decimal text rejected", "12.5", LogicalType{Kind: KindInt64}, nil, false},
		{"float rejected", 12.0, LogicalType{Kind: KindInt64}, nil, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Convert(test.input, test.target)
			if test.valid {
				require.NoError(t, err)
				require.Equal(t, test.want, got)
			} else {
				require.Error(t, err)
				require.ErrorAs(t, err, new(*ConversionError))
			}
		})
	}
}

func TestUnsignedIntegerConversions(t *testing.T) {
	tests := []struct {
		name   string
		input  any
		target LogicalType
		want   any
		valid  bool
	}{
		{"uint8 max", 255, LogicalType{Kind: KindUInt8}, uint8(255), true},
		{"uint8 overflow", 256, LogicalType{Kind: KindUInt8}, nil, false},
		{"uint16 max", "65535", LogicalType{Kind: KindUInt16}, uint16(65535), true},
		{"uint16 overflow", "65536", LogicalType{Kind: KindUInt16}, nil, false},
		{"uint32 max", uint64(math.MaxUint32), LogicalType{Kind: KindUInt32}, uint32(math.MaxUint32), true},
		{"uint32 overflow", uint64(math.MaxUint32) + 1, LogicalType{Kind: KindUInt32}, nil, false},
		{"uint64 max", uint64(math.MaxUint64), LogicalType{Kind: KindUInt64}, uint64(math.MaxUint64), true},
		{"negative rejected", -1, LogicalType{Kind: KindUInt64}, nil, false},
		{"float rejected", 1.0, LogicalType{Kind: KindUInt8}, nil, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Convert(test.input, test.target)
			if test.valid {
				require.NoError(t, err)
				require.Equal(t, test.want, got)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestFloatConversions(t *testing.T) {
	for _, test := range []struct {
		name   string
		input  any
		target LogicalType
		want   any
		valid  bool
	}{
		{"integer to float32", 12, LogicalType{Kind: KindFloat32}, float32(12), true},
		{"text to float64", []byte(" 12.5 "), LogicalType{Kind: KindFloat64}, 12.5, true},
		{"float32 overflow", math.MaxFloat64, LogicalType{Kind: KindFloat32}, nil, false},
		{"nan preserved", math.NaN(), LogicalType{Kind: KindFloat32}, float32(0), true},
		{"positive infinity preserved", math.Inf(1), LogicalType{Kind: KindFloat32}, float32(0), true},
		{"negative infinity preserved", math.Inf(-1), LogicalType{Kind: KindFloat64}, float64(0), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := Convert(test.input, test.target)
			if !test.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			switch test.name {
			case "nan preserved":
				require.True(t, math.IsNaN(float64(got.(float32))))
			case "positive infinity preserved":
				require.True(t, math.IsInf(float64(got.(float32)), 1))
			case "negative infinity preserved":
				require.True(t, math.IsInf(got.(float64), -1))
			default:
				require.Equal(t, test.want, got)
			}
		})
	}
}

func TestDecimalConversions(t *testing.T) {
	target := Decimal(5, 2)
	tests := []struct {
		name  string
		input any
		want  string
		valid bool
	}{
		{"maximum", "999.99", "999.99", true},
		{"precision overflow", "1000.00", "", false},
		{"fractional precision overflow", "1.234", "", false},
		{"trailing fractional zeros exact", "1.230", "1.23", true},
		{"negative", "-12.30", "-12.30", true},
		{"zero", 0, "0.00", true},
		{"integer", uint64(12), "12.00", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := Convert(test.input, target)
			if !test.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			decimal := got.(DecimalValue)
			require.Equal(t, test.want, decimal.String())
		})
	}

	value, err := Convert(DecimalValue{Unscaled: big.NewInt(123), Scale: 2}, target)
	require.NoError(t, err)
	require.Equal(t, "1.23", value.(DecimalValue).String())
}

func TestBooleanDateAndTimeConversions(t *testing.T) {
	for _, input := range []any{true, 1, uint8(1), " YeS ", []byte("on")} {
		got, err := Convert(input, LogicalType{Kind: KindBool})
		require.NoError(t, err)
		require.True(t, got.(bool))
	}
	for _, input := range []any{false, 0, uint8(0), " OFF ", []byte("n")} {
		got, err := Convert(input, LogicalType{Kind: KindBool})
		require.NoError(t, err)
		require.False(t, got.(bool))
	}
	_, err := Convert("not-a-boolean", LogicalType{Kind: KindBool})
	require.Error(t, err)

	date, err := Convert("2026-02-03", LogicalType{Kind: KindDate})
	require.NoError(t, err)
	require.Equal(t, "2026-02-03", date.(time.Time).Format("2006-01-02"))
	_, err = Convert("2026-02-30", LogicalType{Kind: KindDate})
	require.Error(t, err)

	timeOfDay, err := Convert("12:30:45.123456", LogicalType{Kind: KindTime})
	require.NoError(t, err)
	require.Equal(t, 12*time.Hour+30*time.Minute+45*time.Second+123456*time.Microsecond, timeOfDay)
	for _, invalid := range []string{"x:y", "25:00:00", "12:99:00"} {
		_, err := Convert(invalid, LogicalType{Kind: KindTime})
		require.Error(t, err, invalid)
	}
}

func TestTimestampUUIDAndBinaryConversions(t *testing.T) {
	stamp, err := Convert("2026-02-03T04:05:06+02:00", LogicalType{Kind: KindTimestampTZ})
	require.NoError(t, err)
	require.Equal(t, "2026-02-03T02:05:06Z", stamp.(time.Time).Format(time.RFC3339Nano))
	_, err = Convert("not-a-timestamp", LogicalType{Kind: KindTimestamp})
	require.Error(t, err)

	uuid, err := Convert("A0B1C2D3-E4F5-6789-ABCD-0123456789AB", LogicalType{Kind: KindUUID})
	require.NoError(t, err)
	require.Equal(t, "a0b1c2d3-e4f5-6789-abcd-0123456789ab", uuid)
	raw := [16]byte{0xa0, 0xb1, 0xc2, 0xd3, 0xe4, 0xf5, 0x67, 0x89, 0xab, 0xcd, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab}
	uuid, err = Convert(raw, LogicalType{Kind: KindUUID})
	require.NoError(t, err)
	require.Equal(t, "a0b1c2d3-e4f5-6789-abcd-0123456789ab", uuid)
	_, err = Convert("not-a-uuid", LogicalType{Kind: KindUUID})
	require.Error(t, err)

	input := []byte{0xff, 0x00}
	binary, err := Convert(input, LogicalType{Kind: KindBinary})
	require.NoError(t, err)
	binary.([]byte)[0] = 0
	require.Equal(t, byte(0xff), input[0])
	_, err = Convert(struct{}{}, LogicalType{Kind: KindBinary})
	require.Error(t, err)
}

func TestArrayAndFallbackConversions(t *testing.T) {
	int8Array := ArrayOf(LogicalType{Kind: KindInt8})
	converted, err := Convert([]any{1, "2", []byte("3")}, int8Array)
	require.NoError(t, err)
	require.Equal(t, []any{int8(1), int8(2), int8(3)}, converted)
	_, err = Convert([]any{1, 128}, int8Array)
	require.Error(t, err)

	nullableArray := ArrayOf(Nullable(LogicalType{Kind: KindInt8}))
	converted, err = Convert("[1,null,3]", nullableArray)
	require.NoError(t, err)
	require.Equal(t, []any{int8(1), nil, int8(3)}, converted)
	_, err = Convert("{1,NULL,3}", int8Array)
	require.Error(t, err)

	for _, test := range []struct {
		input any
		want  string
	}{
		{"text", "text"},
		{12, "12"},
		{1.5, "1.5"},
		{true, "true"},
		{time.Date(2026, 2, 3, 4, 5, 6, 0, time.FixedZone("UTC+2", 2*60*60)), "2026-02-03T02:05:06Z"},
		{map[string]any{"b": 2, "a": "one"}, "json:{\"a\":\"one\",\"b\":2}"},
		{[]any{"a", 2}, "json:[\"a\",2]"},
		{fallbackStruct{Name: "one", Count: 2}, "json:{\"Name\":\"one\",\"Count\":2}"},
	} {
		got, err := ToLosslessString(test.input)
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}

	bytes := []byte{0xff, 0x00}
	fallback, err := ToLosslessString(bytes)
	require.NoError(t, err)
	require.Equal(t, "base64:"+base64.StdEncoding.EncodeToString(bytes), fallback)
	decoded, err := base64.StdEncoding.DecodeString(fallback[len("base64:"):])
	require.NoError(t, err)
	require.Equal(t, bytes, decoded)
	arrayBytes := [2]byte{0xff, 0x00}
	fallback, err = ToLosslessString(arrayBytes)
	require.NoError(t, err)
	require.Equal(t, "base64:"+base64.StdEncoding.EncodeToString(arrayBytes[:]), fallback)

	_, err = ToLosslessString(map[int]string{1: "not-safe"})
	require.Error(t, err)

	value, err := Convert(map[string]any{"a": "one"}, LogicalType{Kind: KindUnknown})
	require.NoError(t, err)
	require.Equal(t, "json:{\"a\":\"one\"}", value)
}

func TestNilAndStructuredErrors(t *testing.T) {
	value, err := Convert(nil, LogicalType{Kind: KindInt8})
	require.NoError(t, err)
	require.Nil(t, value)

	_, err = Convert("abc", LogicalType{Kind: KindInt8})
	var conversion *ConversionError
	require.True(t, errors.As(err, &conversion))
	require.Equal(t, KindInt8, conversion.Target.Kind)
	require.Contains(t, err.Error(), "string")
	require.Contains(t, err.Error(), "invalid integer")
}

type fallbackStruct struct {
	Name  string
	Count int
}
