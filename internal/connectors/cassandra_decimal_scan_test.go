package connectors

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type scanningReader struct {
	TableReader
	asked []string
}

func (r *scanningReader) ScanDecimalColumns(_ context.Context, _, _ string, columns []string) (map[string]DecimalColumnStats, error) {
	r.asked = columns
	return map[string]DecimalColumnStats{"balance": {Column: "balance", Values: 2, IntegerDigits: 3, Scale: 2}}, nil
}

// The planner only sees wrapped readers; the wrapper must pass the decimal
// scan through, or Cassandra decimals silently stay text.
func TestWrappedReaderForwardsDecimalScans(t *testing.T) {
	inner := &scanningReader{}
	wrapped := wrapCursorReader(inner)
	scanner, ok := wrapped.(DecimalColumnScanner)
	require.True(t, ok)
	stats, err := scanner.ScanDecimalColumns(context.Background(), "ks.t", "", []string{"balance"})
	require.NoError(t, err)
	require.Equal(t, []string{"balance"}, inner.asked)
	require.Equal(t, 2, stats["balance"].Scale)

	plain := wrapCursorReader(struct{ TableReader }{})
	_, err = plain.(DecimalColumnScanner).ScanDecimalColumns(context.Background(), "ks.t", "", []string{"balance"})
	require.ErrorIs(t, err, ErrColumnProbeUnsupported)
}
