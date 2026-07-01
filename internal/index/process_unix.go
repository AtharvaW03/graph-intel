//go:build unix

package index

import (
	"os/exec"
	"syscall"
)

// setupProcessGroup puts the subprocess in its own process group and installs
// a Cancel hook that signals the whole group on ctx cancellation. Without
// this, a CommandContext cancel only sends SIGKILL to the direct child —
// long-running git or graphify subprocesses that spawn helpers (ssh,
// git-credential-*) would orphan those helpers, leaking file descriptors and
// pinning the shell after a Ctrl-C.
func setupProcessGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	prev := cmd.Cancel
	cmd.Cancel = func() error {
		if cmd.Process != nil {
			// Negative pid targets the whole process group.
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		if prev != nil {
			return prev()
		}
		return nil
	}
}
