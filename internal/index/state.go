package index

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// JSONStateStore persists indexing state to a JSON file. Writes are atomic
// (write to a tempfile in the same directory, fsync, rename, fsync parent)
// and serialized by a mutex so the orchestrator can call Set after every
// repo without worrying about partial writes or interleaving.
//
// Single-process, single-host scope. Swap in a Redis/Postgres-backed
// StateStore to coordinate across distributed workers.
type JSONStateStore struct {
	path string
	mu   sync.Mutex
	data map[string]RepoState
}

type stateFile struct {
	Version int                  `json:"version"`
	Repos   map[string]RepoState `json:"repos"`
}

// stateVersion is bumped when the on-disk schema changes incompatibly.
// LoadJSONStateStore refuses to read versions newer than this to avoid
// downgrade-driven data loss.
const stateVersion = 1

// LoadJSONStateStore reads the state file at path, creating an empty store
// if it does not exist. If the file exists but is malformed JSON it is
// quarantined to <path>.corrupt-<timestamp> and a fresh empty store is
// returned, with a warning logged via stderr. This is the only way a
// long-running daemon recovers from a corrupted state file — fataling here
// would wedge the indexer permanently against a single bad write.
//
// A versioned file that is NEWER than stateVersion is treated as a hard
// error: a downgrade should refuse to load future-version data and lose it.
func LoadJSONStateStore(path string) (*JSONStateStore, error) {
	s := &JSONStateStore{path: path, data: map[string]RepoState{}}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("read state file: %w", err)
	}
	if len(raw) == 0 {
		return s, nil
	}
	var f stateFile
	if err := json.Unmarshal(raw, &f); err != nil {
		quarantine := fmt.Sprintf("%s.corrupt-%s", path, time.Now().UTC().Format("20060102T150405Z"))
		if renameErr := os.Rename(path, quarantine); renameErr != nil {
			return nil, fmt.Errorf("state file %s is malformed and could not be quarantined: %v (parse error: %w)", path, renameErr, err)
		}
		log.Printf("WARNING: state file %s was malformed; quarantined to %s and starting with empty state (parse error: %v)", path, quarantine, err)
		return s, nil
	}
	if f.Version > stateVersion {
		return nil, fmt.Errorf("state file %s has version %d, newer than supported %d (downgrade?); refusing to load to avoid data loss", path, f.Version, stateVersion)
	}
	if f.Repos != nil {
		s.data = f.Repos
	}
	return s, nil
}

func (s *JSONStateStore) Get(name string) (RepoState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[name]
	return v, ok
}

// Set updates the in-memory map and atomically flushes to disk. On flush
// failure the in-memory map is rolled back to its previous contents so
// memory and disk stay consistent — the next Set will be a clean retry.
func (s *JSONStateStore) Set(state RepoState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	prev, existed := s.data[state.Name]
	s.data[state.Name] = state
	if err := s.flushLocked(); err != nil {
		if existed {
			s.data[state.Name] = prev
		} else {
			delete(s.data, state.Name)
		}
		return err
	}
	return nil
}

func (s *JSONStateStore) All() map[string]RepoState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]RepoState, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

func (s *JSONStateStore) flushLocked() error {
	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("ensure state dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".state-*.json")
	if err != nil {
		return fmt.Errorf("create temp state: %w", err)
	}
	tmpPath := tmp.Name()
	// Best-effort cleanup if anything below fails before rename succeeds.
	defer func() {
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
	}()

	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(stateFile{Version: stateVersion, Repos: s.data}); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("encode state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close state tmp: %w", err)
	}
	if err := os.Rename(tmpPath, s.path); err != nil {
		return fmt.Errorf("rename state: %w", err)
	}
	tmpPath = "" // suppress deferred cleanup; rename consumed the file
	// Without an fsync on the parent directory, a crash between rename and
	// directory flush can leave the rename invisible after recovery on some
	// filesystems. Best-effort on POSIX; skipped on Windows where Open of a
	// directory is not supported.
	if runtime.GOOS != "windows" {
		if d, err := os.Open(dir); err == nil {
			_ = d.Sync()
			_ = d.Close()
		}
	}
	return nil
}
