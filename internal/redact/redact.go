// Package redact masks credentials in log text and structured log fields
// before they leave the master (log and event APIs, live streams).
package redact

import (
	"encoding/json"
	"regexp"
	"strings"
)

// Mask replaces a hidden value.
const Mask = "***"

var (
	// scheme://user:password@host: keep the user, hide the password.
	urlCredentials = regexp.MustCompile(`(://[^/\s:@]*:)[^@\s]*@`)
	// Authorization: Bearer abc / Basic abc.
	authHeader = regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`)
	// password=..., "token": "...", secret_access_key: ... in free text.
	// (Authorization headers are handled by authHeader, which keeps the scheme.)
	keyValue = regexp.MustCompile(`(?i)("?\b[\w.-]*(?:password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential|dsn)[\w.-]*"?\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,;&}]+)`)
	// Field names whose whole value is secret in structured logs.
	sensitiveKey = regexp.MustCompile(`(?i)(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential|authorization|^dsn$|_dsn$)`)
	// PEM private key blocks.
	pemKey = regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----`)
)

// Text masks credentials embedded in free-form text.
func Text(s string) string {
	if s == "" {
		return s
	}
	s = pemKey.ReplaceAllString(s, "-----PRIVATE KEY "+Mask+"-----")
	s = urlCredentials.ReplaceAllString(s, "${1}"+Mask+"@")
	s = authHeader.ReplaceAllString(s, "${1} "+Mask)
	s = keyValue.ReplaceAllStringFunc(s, func(m string) string {
		parts := keyValue.FindStringSubmatch(m)
		value := parts[2]
		if strings.HasPrefix(value, `"`) {
			return parts[1] + `"` + Mask + `"`
		}
		if strings.HasPrefix(value, `'`) {
			return parts[1] + `'` + Mask + `'`
		}
		return parts[1] + Mask
	})
	return s
}

// IsSensitiveKey reports whether a field name holds a secret.
func IsSensitiveKey(key string) bool {
	return sensitiveKey.MatchString(strings.TrimSpace(key))
}

// Value masks a decoded JSON value: sensitive keys are replaced, other strings
// are passed through Text. Maps and slices are copied, never modified in place.
func Value(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if IsSensitiveKey(k) && val != nil {
				out[k] = Mask
				continue
			}
			out[k] = Value(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = Value(val)
		}
		return out
	case string:
		return Text(t)
	default:
		return v
	}
}

// JSON masks a raw JSON document. Invalid JSON is treated as text so nothing
// leaks just because a field was not well formed.
func JSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		b, _ := json.Marshal(Text(string(raw)))
		return b
	}
	b, err := json.Marshal(Value(v))
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return b
}
