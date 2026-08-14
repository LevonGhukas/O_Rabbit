package connectors

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	adbcflightsql "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
)

// parseFlightSQLDSN converts a user-facing DSN string into ADBC options.
// Kept in a dedicated file to isolate connector-string parsing concerns from query execution.
func parseFlightSQLDSN(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty FlightSQL DSN")
	}

	opts := map[string]string{}
	if isFlightSQLKVDSN(raw) {
		fields := strings.FieldsFunc(raw, func(r rune) bool { return r == ';' || r == '\n' })
		for _, field := range fields {
			field = strings.TrimSpace(field)
			if field == "" {
				continue
			}
			key, val, ok := strings.Cut(field, "=")
			if !ok {
				return nil, fmt.Errorf("invalid FlightSQL DSN field %q (expected key=value)", field)
			}
			k := normalizeFlightSQLOptionKey(key)
			v := strings.TrimSpace(strings.Trim(val, `"'`))
			if k == "" || v == "" {
				continue
			}
			opts[k] = v
		}
	} else {
		opts[adbc.OptionKeyURI] = raw
	}

	uri := strings.TrimSpace(opts[adbc.OptionKeyURI])
	if uri == "" {
		return nil, fmt.Errorf("FlightSQL DSN must include uri")
	}

	if parsed, err := url.Parse(uri); err == nil {
		if parsed.User != nil {
			if _, ok := opts[adbc.OptionKeyUsername]; !ok {
				if u := strings.TrimSpace(parsed.User.Username()); u != "" {
					opts[adbc.OptionKeyUsername] = u
				}
			}
			if _, ok := opts[adbc.OptionKeyPassword]; !ok {
				if p, set := parsed.User.Password(); set && strings.TrimSpace(p) != "" {
					opts[adbc.OptionKeyPassword] = strings.TrimSpace(p)
				}
			}
		}

		query := parsed.Query()
		fillIfMissing(opts, adbc.OptionKeyUsername, query.Get("username"))
		fillIfMissing(opts, adbc.OptionKeyPassword, query.Get("password"))
		fillIfMissing(opts, adbcflightsql.OptionAuthorizationHeader, query.Get("authorization_header"))
		fillIfMissing(opts, adbcflightsql.OptionSSLSkipVerify, query.Get("tls_skip_verify"))
		fillIfMissing(opts, adbcflightsql.OptionSSLRootCerts, query.Get("tls_root_certs"))
	}
	return opts, nil
}

// isFlightSQLKVDSN reports if the DSN is in key=value form.
func isFlightSQLKVDSN(raw string) bool {
	firstKey, _, ok := strings.Cut(raw, "=")
	if !ok {
		return false
	}
	firstKey = strings.TrimSpace(firstKey)
	if firstKey == "" {
		return false
	}
	return !strings.Contains(firstKey, "://")
}

// normalizeFlightSQLOptionKey maps friendly DSN keys to ADBC option names.
func normalizeFlightSQLOptionKey(raw string) string {
	key := strings.ToLower(strings.TrimSpace(raw))
	key = strings.ReplaceAll(key, "-", "_")
	key = strings.ReplaceAll(key, ".", "_")
	switch key {
	case "uri":
		return adbc.OptionKeyURI
	case "username", "user":
		return adbc.OptionKeyUsername
	case "password", "pass", "pwd":
		return adbc.OptionKeyPassword
	case "authorization_header", "auth_header", "bearer_token":
		return adbcflightsql.OptionAuthorizationHeader
	case "tls_skip_verify", "insecure_skip_verify":
		return adbcflightsql.OptionSSLSkipVerify
	case "tls_root_certs", "ssl_root_certs", "root_certs":
		return adbcflightsql.OptionSSLRootCerts
	case "mtls_cert_chain":
		return adbcflightsql.OptionMTLSCertChain
	case "mtls_private_key":
		return adbcflightsql.OptionMTLSPrivateKey
	default:
		return strings.TrimSpace(raw)
	}
}

// fillIfMissing sets option key only when it is currently unset.
func fillIfMissing(opts map[string]string, key, val string) {
	if _, ok := opts[key]; ok {
		return
	}
	val = strings.TrimSpace(val)
	if val == "" {
		return
	}
	opts[key] = val
}
