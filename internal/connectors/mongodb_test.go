package connectors

import (
	"testing"
)

func TestMongoDBTypeName(t *testing.T) {
	tests := []struct {
		in   any
		want string
	}{
		{in: "hello", want: "STRING"},
		{in: int32(42), want: "INT64"},
		{in: int64(42), want: "INT64"},
		{in: 42, want: "INT64"},
		{in: float64(3.14), want: "FLOAT64"},
		{in: true, want: "BOOL"},
		{in: nil, want: "NULL"},
	}
	for _, tc := range tests {
		got := mongobsonTypeName(tc.in)
		if got != tc.want {
			t.Fatalf("mongobsonTypeName(%T(%v))=%q, want %q", tc.in, tc.in, got, tc.want)
		}
	}
}
