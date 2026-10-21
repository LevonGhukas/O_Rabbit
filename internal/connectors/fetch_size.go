package connectors

import (
	"net/url"
	"strconv"
	"strings"
)

// Fetch size is how many rows a driver fetches from the source per network
// round-trip. It trades a little memory for far fewer round-trips on large
// exports. Only some drivers expose it; it is applied through each driver's
// own DSN parameter, so connectors and their callers need no extra wiring.

// OracleDefaultPrefetchRows replaces go-ora's default of 25 rows per
// round-trip, which makes large exports wait on the network.
const OracleDefaultPrefetchRows = 5000

// MaxFetchSize bounds fetch sizes to keep per-worker memory predictable.
const MaxFetchSize = 1_000_000

// fetchSizeParams names the DSN parameter each engine uses for its fetch size.
var fetchSizeParams = map[string]string{
	"oracle":     "PREFETCH_ROWS",
	"cassandra":  "page_size",
	"clickhouse": "max_block_size",
}

// SupportsFetchSize reports whether engine applies a fetch size. MongoDB takes
// it as the cursor batch size from the worker instead of the DSN.
func SupportsFetchSize(engine string) bool {
	engine = NormalizeSourceEngine(engine)
	_, ok := fetchSizeParams[engine]
	return ok || engine == "mongodb"
}

// WithFetchSize returns dsn with the engine's fetch-size parameter set to n,
// replacing any value already in the DSN: a job's fetch_size is the more
// specific setting. dsn is returned unchanged when n <= 0, the engine has no
// DSN fetch size, or dsn is not a URL.
func WithFetchSize(engine, dsn string, n int) string {
	param, ok := fetchSizeParams[NormalizeSourceEngine(engine)]
	if !ok || n <= 0 {
		return dsn
	}
	return setDSNParam(dsn, param, strconv.Itoa(n), true)
}

// oracleDSNWithDefaultPrefetch sets PREFETCH_ROWS to the default when the DSN
// does not choose one.
func oracleDSNWithDefaultPrefetch(dsn string) string {
	return setDSNParam(dsn, fetchSizeParams["oracle"], strconv.Itoa(OracleDefaultPrefetchRows), false)
}

// setDSNParam sets a query parameter on a URL-style DSN. Keys match case
// insensitively, because some drivers (go-ora) read them that way. With
// replace false an existing value is kept.
func setDSNParam(dsn, key, value string, replace bool) string {
	u, err := url.Parse(strings.TrimSpace(dsn))
	if err != nil || u.Scheme == "" {
		return dsn
	}
	q := u.Query()
	for existing := range q {
		if strings.EqualFold(existing, key) {
			if !replace {
				return dsn
			}
			q.Del(existing)
		}
	}
	q.Set(key, value)
	u.RawQuery = q.Encode()
	return u.String()
}
