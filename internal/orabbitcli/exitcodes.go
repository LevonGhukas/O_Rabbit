package orabbitcli

import (
	"context"
	"errors"
	"os/exec"
)

const (
	exitSuccess     = 0
	exitOperational = 1
	exitUsage       = 2
	exitInterrupted = 130
)

// exitCode maps shared CLI error classes onto user-facing process exit codes.
func exitCode(err error) int {
	if err == nil {
		return exitSuccess
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, ErrInterrupted) {
		return exitInterrupted
	}
	if errors.Is(err, ErrPromptTTYRequired) || errors.Is(err, ErrPromptEOF) || errors.Is(err, ErrPromptInvalid) || errors.Is(err, ErrRunSubmitSpecInvalid) {
		return exitUsage
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	return exitOperational
}
