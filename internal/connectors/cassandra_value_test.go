package connectors

import (
	"math"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
)

func TestCassandraToDriverValueIsLossless(t *testing.T) {
	value, err := cassandraToDriverValue(uint64(math.MaxUint64))
	require.NoError(t, err)
	require.Equal(t, "18446744073709551615", value)
	bytes := []byte{0, 255, 1}
	value, err = cassandraToDriverValue(bytes)
	require.NoError(t, err)
	require.Equal(t, bytes, value)
	id := gocql.UUID{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00}
	value, err = cassandraToDriverValue(id)
	require.NoError(t, err)
	require.Equal(t, "123e4567-e89b-12d3-a456-426614174000", value)
	value, err = cassandraToDriverValue(map[string]any{"b": 2, "a": 1})
	require.NoError(t, err)
	require.Equal(t, `json:{"a":1,"b":2}`, value)
}
