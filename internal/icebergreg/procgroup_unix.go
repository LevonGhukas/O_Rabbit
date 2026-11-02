//go:build unix

package icebergreg

import (
	"os/exec"
	"syscall"
)

// killProcessGroupOnCancel runs cmd in its own process group and makes context
// cancellation kill the whole group, so a JVM started by the ice launcher
// cannot outlive the attempt and commit after it was recorded as failed.
func killProcessGroupOnCancel(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	cmd.Cancel = func() error {
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
