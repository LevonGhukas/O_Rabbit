package icebergreg

import (
	"strings"
	"sync"
)

// redactedPlaceholder replaces secret values found in captured CLI output.
const redactedPlaceholder = "***"

// tailRedactMargin is extra capacity kept beyond the reported tail so a secret
// straddling the tail boundary is redacted whole before the tail is cut.
const tailRedactMargin = 1024

// tailBuffer keeps only the last bytes written to it and reports at most the
// last tail bytes. It is safe for concurrent use because exec copies stderr
// from its own goroutine.
type tailBuffer struct {
	mu   sync.Mutex
	tail int
	buf  []byte
}

func newTailBuffer(tail int) *tailBuffer {
	return &tailBuffer{tail: tail}
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, p...)
	if over := len(t.buf) - (t.tail + tailRedactMargin); over > 0 {
		t.buf = append([]byte(nil), t.buf[over:]...)
	}
	return len(p), nil
}

// String returns the buffered tail, trimmed, with every non-empty secret
// replaced by a placeholder.
func (t *tailBuffer) String(secrets ...string) string {
	t.mu.Lock()
	raw := string(t.buf)
	t.mu.Unlock()
	out := redactSecrets(raw, secrets...)
	if len(out) > t.tail {
		out = out[len(out)-t.tail:]
	}
	return strings.TrimSpace(out)
}

func redactSecrets(text string, secrets ...string) string {
	for _, secret := range secrets {
		if secret = strings.TrimSpace(secret); secret != "" {
			text = strings.ReplaceAll(text, secret, redactedPlaceholder)
		}
	}
	return text
}
