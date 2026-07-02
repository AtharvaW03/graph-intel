package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// GitSyncer is the default Syncer: it shells out to the local `git` binary.
// Authentication is whatever the user's git is configured for (SSH keys,
// credential helpers, etc.) — the indexer never touches credentials.
//
// All subprocesses run with credential prompts disabled (GIT_TERMINAL_PROMPT=0,
// BatchMode SSH) so a missing key surfaces as a fast error instead of hanging
// until the per-command timeout fires. On unix, cancellation kills the entire
// process group via process_unix.go so long-running clone/fetch subprocesses
// aren't orphaned.
type GitSyncer struct {
	Timeout time.Duration
}

func NewGitSyncer(cfg GitConfig) *GitSyncer {
	return &GitSyncer{Timeout: cfg.Timeout}
}

// Sync clones repo at dest if absent, fetches+resets to origin/<branch> if
// present, and returns the resulting HEAD commit.
func (g *GitSyncer) Sync(ctx context.Context, repo Repository, dest string) (string, error) {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return "", fmt.Errorf("ensure parent: %w", err)
	}

	if _, err := os.Stat(filepath.Join(dest, ".git")); err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat repo: %w", err)
		}
		if err := g.clone(ctx, repo, dest); err != nil {
			return "", err
		}
	} else {
		if err := g.update(ctx, repo, dest); err != nil {
			return "", err
		}
	}

	return g.head(ctx, dest)
}

func (g *GitSyncer) clone(ctx context.Context, repo Repository, dest string) error {
	// If the directory exists but is not a git repo (e.g. partial previous
	// clone interrupted), refuse to clobber it; operator should clean up.
	if entries, err := os.ReadDir(dest); err == nil && len(entries) > 0 {
		return fmt.Errorf("clone target %s is non-empty and not a git repo; remove it manually before retrying", dest)
	}
	// Shallow single-branch clone: the indexer only ever reads the branch
	// tip, and full history across dozens of org repos is pure waste. Later
	// fetches into a shallow clone stay shallow.
	args := []string{"clone", "--depth", "1", "--single-branch", "--branch", repo.Branch, "--", repo.URL, dest}
	if _, err := g.run(ctx, "", "git", args...); err != nil {
		return fmt.Errorf("clone %s: %w", repo.URL, err)
	}
	return nil
}

func (g *GitSyncer) update(ctx context.Context, repo Repository, dest string) error {
	// Sanity-check the on-disk repo with a cheap call; this catches half-broken
	// .git directories (interrupted prior clones, manual deletes) early instead
	// of bubbling up a misleading error from a deeper command.
	if _, err := g.run(ctx, dest, "git", "rev-parse", "--git-dir"); err != nil {
		return fmt.Errorf("corrupt clone at %s (rm and re-run to heal): %w", dest, err)
	}
	// Verify the existing clone points at the same remote URL. If a repo's URL
	// changes (e.g. ownership transfer), refuse to silently re-sync against
	// the wrong remote — surface it so the operator can remediate.
	out, err := g.run(ctx, dest, "git", "remote", "get-url", "origin")
	if err != nil {
		return fmt.Errorf("read remote: %w", err)
	}
	current := strings.TrimSpace(out)
	if current != repo.URL {
		return fmt.Errorf("remote mismatch at %s: configured %q, on-disk %q", dest, repo.URL, current)
	}
	if _, err := g.run(ctx, dest, "git", "fetch", "--prune", "origin", repo.Branch); err != nil {
		return fmt.Errorf("fetch %s: %w", repo.Branch, err)
	}
	if _, err := g.run(ctx, dest, "git", "reset", "--hard", "origin/"+repo.Branch); err != nil {
		return fmt.Errorf("reset to origin/%s: %w", repo.Branch, err)
	}
	return nil
}

func (g *GitSyncer) head(ctx context.Context, dest string) (string, error) {
	out, err := g.run(ctx, dest, "git", "rev-parse", "HEAD")
	if err != nil {
		return "", fmt.Errorf("rev-parse: %w", err)
	}
	return strings.TrimSpace(out), nil
}

func (g *GitSyncer) run(ctx context.Context, workdir, name string, args ...string) (string, error) {
	cmdCtx := ctx
	if g.Timeout > 0 {
		var cancel context.CancelFunc
		cmdCtx, cancel = context.WithTimeout(ctx, g.Timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(cmdCtx, name, args...)
	if workdir != "" {
		cmd.Dir = workdir
	}
	// GIT_TERMINAL_PROMPT=0 makes missing credentials fail fast instead of
	// hanging on a prompt; BatchMode does the same for SSH. No ASKPASS
	// override — hardcoding /bin/echo breaks on Windows and adds nothing
	// once prompts are disabled.
	cmd.Env = append(os.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"GIT_SSH_COMMAND=ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10",
	)
	setupProcessGroup(cmd) // unix: own pgid so cancellation kills children too
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("%s %s: timed out after %s", name, strings.Join(args, " "), g.Timeout)
		}
		if errors.Is(cmdCtx.Err(), context.Canceled) {
			return "", fmt.Errorf("%s %s: canceled", name, strings.Join(args, " "))
		}
		return "", fmt.Errorf("%s %s: %w (%s)", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}
