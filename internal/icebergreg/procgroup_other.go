//go:build !unix

package icebergreg

import "os/exec"

// killProcessGroupOnCancel keeps exec's default cancellation, which kills only
// the direct child, on platforms without process groups.
func killProcessGroupOnCancel(cmd *exec.Cmd) {}
