//go:build !unix

package index

import "os/exec"

// setupProcessGroup is a no-op on non-unix platforms. exec.CommandContext's
// default cancel behavior (SIGKILL to the direct child only) is what we get.
func setupProcessGroup(cmd *exec.Cmd) {}
