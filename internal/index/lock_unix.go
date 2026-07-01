//go:build unix

package index

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// LockWorkDir takes an exclusive advisory lock (flock) on <dir>/.indexer.lock
// so concurrent indexer processes against the same workdir refuse to run
// instead of racing on state.json / clones / graphify outputs.
//
// The returned *os.File MUST be kept open for the lifetime of the process;
// closing the file releases the lock. On exit, the OS releases it for us.
//
// Non-unix builds fall back to a no-op via lock_other.go — single-host POSIX
// is the only environment with guaranteed flock semantics.
func LockWorkDir(dir string) (*os.File, error) {
	path := filepath.Join(dir, ".indexer.lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open workdir lock %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, fmt.Errorf("another indexer is already running against workdir %s", dir)
		}
		return nil, fmt.Errorf("lock workdir %s: %w", dir, err)
	}
	return f, nil
}
