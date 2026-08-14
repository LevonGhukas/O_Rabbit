package orabbitcli

import (
	"context"
	"errors"
	"testing"
)

func TestExitCodeMappings(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "success", err: nil, want: exitSuccess},
		{name: "interrupt", err: ErrInterrupted, want: exitInterrupted},
		{name: "context canceled", err: context.Canceled, want: exitInterrupted},
		{name: "prompt tty", err: ErrPromptTTYRequired, want: exitUsage},
		{name: "prompt eof", err: ErrPromptEOF, want: exitUsage},
		{name: "prompt invalid", err: ErrPromptInvalid, want: exitUsage},
		{name: "operational", err: errors.New("boom"), want: exitOperational},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := exitCode(tt.err); got != tt.want {
				t.Fatalf("exitCode(%v) = %d, want %d", tt.err, got, tt.want)
			}
		})
	}
}
