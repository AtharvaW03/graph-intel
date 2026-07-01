//go:build !unix

package index

import "os"

// LockWorkDir is a no-op on non-unix platforms where flock semantics are
// not guaranteed. Concurrent indexer processes are the operator's
// responsibility there.
func LockWorkDir(dir string) (*os.File, error) {
	return nil, nil
}
