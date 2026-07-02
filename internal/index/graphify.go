package index

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// ExecGraphifier runs an external extractor command (the locally-installed
// `graphify` CLI by default, invoked as `graphify update <repo_path>`) and
// resolves the produced graph.json inside the repository's own
// graphify-out/ directory.
//
// `update` is graphify's incremental, AST-only-by-default workflow per
// docs/graphify/ ("re-extract only changed files",
// "auto-rebuild on git commit (AST only, no API cost)"). This wrapper
// matches that model:
//   - the command runs from repoPath as its working directory
//   - the {repo_path} placeholder is substituted into Args; {out_dir} is no
//     longer a valid placeholder (update has no --out flag)
//   - the post-run output is located at <repoPath>/<OutputFile>
//   - the pre-run output is NOT deleted — `update` consumes the prior
//     graph.json to do incremental updates, so wiping it would force a full
//     re-extract every cycle and kill incremental performance
//   - the post-run file must exist and be non-empty for the run to be
//     considered successful; this catches a graphify that exited 0 without
//     producing output
type ExecGraphifier struct {
	Command    string
	Args       []string
	OutputFile string // relative to repoPath; default graphify-out/graph.json
	Timeout    time.Duration
	Stderr     io.Writer
}

func NewExecGraphifier(cfg GraphifyConfig, stderr io.Writer) *ExecGraphifier {
	return &ExecGraphifier{
		Command:    cfg.Command,
		Args:       cfg.Args,
		OutputFile: cfg.OutputFile,
		Timeout:    cfg.Timeout,
		Stderr:     stderr,
	}
}

// Generate runs the configured extractor command for the repo at repoPath
// and returns the absolute path of the resulting graph.json. The output
// path is OutputFile resolved relative to the absolute repoPath — graphify
// `update` writes into the repo, so there is no separate out directory.
//
// Subprocess stdout and stderr are routed to the configured Stderr writer
// so the daemon's protocol streams (the MCP stdio transport, in particular)
// are never corrupted by extractor chatter.
func (g *ExecGraphifier) Generate(ctx context.Context, repoPath string) (string, error) {
	absRepo, err := filepath.Abs(repoPath)
	if err != nil {
		return "", fmt.Errorf("abs repo path: %w", err)
	}

	args := make([]string, len(g.Args))
	for i, a := range g.Args {
		a = strings.ReplaceAll(a, "{repo_path}", absRepo)
		args[i] = a
	}

	cmdCtx := ctx
	if g.Timeout > 0 {
		var cancel context.CancelFunc
		cmdCtx, cancel = context.WithTimeout(ctx, g.Timeout)
		defer cancel()
	}

	cmd := exec.CommandContext(cmdCtx, g.Command, args...)
	cmd.Dir = absRepo
	setupProcessGroup(cmd)

	stderrSink := g.Stderr
	if stderrSink == nil {
		stderrSink = os.Stderr
	}
	tail := &tailWriter{max: 4096}
	cmd.Stdout = io.MultiWriter(stderrSink, tail)
	cmd.Stderr = io.MultiWriter(stderrSink, tail)

	if err := cmd.Run(); err != nil {
		if errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("%s timed out after %s\n--- output tail ---\n%s", g.Command, g.Timeout, tail.String())
		}
		if errors.Is(cmdCtx.Err(), context.Canceled) {
			return "", fmt.Errorf("%s canceled\n--- output tail ---\n%s", g.Command, tail.String())
		}
		return "", fmt.Errorf("%s %s: %w\n--- output tail ---\n%s", g.Command, strings.Join(args, " "), err, tail.String())
	}

	produced := filepath.Join(absRepo, g.OutputFile)
	info, err := os.Stat(produced)
	if err != nil {
		return "", fmt.Errorf("expected output not found at %s: %w\n--- output tail ---\n%s", produced, err, tail.String())
	}
	if info.Size() == 0 {
		return "", fmt.Errorf("expected output at %s is empty\n--- output tail ---\n%s", produced, tail.String())
	}
	return produced, nil
}

// tailWriter buffers the LAST `max` bytes of writes so subprocess output can
// be quoted in error messages without growing unbounded for chatty tools.
// Goroutine-safe because cmd.Stdout and cmd.Stderr are both wired to the
// same instance via MultiWriter and the exec package's two output goroutines
// write concurrently.
type tailWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
	max int
}

func (t *tailWriter) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(p) >= t.max {
		t.buf.Reset()
		t.buf.Write(p[len(p)-t.max:])
		return len(p), nil
	}
	if t.buf.Len()+len(p) > t.max {
		overflow := t.buf.Len() + len(p) - t.max
		t.buf.Next(overflow)
	}
	t.buf.Write(p)
	return len(p), nil
}

func (t *tailWriter) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return strings.TrimSpace(t.buf.String())
}
