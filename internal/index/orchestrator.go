package index

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"graph-platform/internal/extract"
	"graph-platform/internal/importer"
)

// Orchestrator drives the per-repo pipeline for a configured set of
// repositories. It owns no domain knowledge — every step is delegated to a
// pluggable component (Source, Syncer, Graphifier, Importer, Store,
// optional Scheduler and HealthChecker). Future concurrency (parallel
// indexing) or transport (SQS, webhooks) changes happen by swapping one of
// those collaborators.
type Orchestrator struct {
	Source   JobSource
	Syncer   Syncer
	Graphify Graphifier
	Importer ImportRunner
	Store    StateStore
	WorkDir  string
	Log      *log.Logger
	Clock    func() time.Time

	// Extractors, if non-nil, runs the configured platform extractors after
	// graphify and merges their fragments into the unified graph.json before
	// the importer reads it. Extractor failures never block import — they
	// surface as per-extractor errors on the RepoResult.
	Extractors *extract.Runner

	// HealthChecker, if set, is pinged before each cycle in continuous mode.
	// A failed ping is logged but does not abort — the cycle proceeds and
	// individual stage failures will be recorded per-repo.
	HealthChecker HealthChecker
}

// ImportRunner is the importer-side interface. The default implementation
// adapts internal/importer.Run; tests or alternative sinks can swap in.
type ImportRunner interface {
	Run(ctx context.Context, repo, commit, graphPath string) (*importer.Summary, error)
}

// HealthChecker pings a downstream dependency. The default impl wraps
// *neo4j.Client; future variants can compose multiple checks.
type HealthChecker interface {
	VerifyConnectivity(ctx context.Context) error
}

// Options modulate a single RunOnce invocation.
type Options struct {
	// Names selects a subset of repositories; empty means "all".
	Names []string
	// Force re-indexes even if HEAD matches the previously-indexed commit.
	Force bool
}

// RunOnce indexes every selected repository sequentially. One repo failing
// never stops the others — failures are recorded on the result and state is
// flushed before moving on. ctx cancellation aborts the current repo and
// stops the loop after. RunOnce never panics; any panic in collaborators is
// recovered, logged, and recorded on the run.
func (o *Orchestrator) RunOnce(ctx context.Context, opts Options) (summary RunSummary, err error) {
	summary = RunSummary{StartedAt: o.now()}
	defer func() { summary.FinishedAt = o.now() }()
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("RunOnce panic: %v", p)
		}
	}()

	repos, err := o.Source.Repositories(ctx)
	if err != nil {
		return summary, fmt.Errorf("load repositories: %w", err)
	}
	repos, err = filterRepos(repos, opts.Names)
	if err != nil {
		return summary, err
	}

	for _, repo := range repos {
		if ctx.Err() != nil {
			o.Log.Printf("context canceled, stopping after %d/%d repositories", len(summary.Results), len(repos))
			break
		}
		result := o.IndexOne(ctx, repo, opts.Force)
		summary.Results = append(summary.Results, result)
	}

	return summary, nil
}

// RunForever loops RunOnce on the configured Scheduler until ctx is canceled.
// Cycles never overlap: the next pass only starts after the current pass
// returns AND the Scheduler signals. A panic inside the loop is recovered
// and the next cycle proceeds — the daemon never dies on a recoverable bug.
func (o *Orchestrator) RunForever(ctx context.Context, opts Options, sched Scheduler) error {
	if sched == nil {
		return fmt.Errorf("scheduler is required for continuous mode")
	}
	o.Log.Printf("continuous indexing started")
	for {
		o.runCycleSafely(ctx, opts)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := sched.Wait(ctx); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			return err
		}
	}
}

func (o *Orchestrator) runCycleSafely(ctx context.Context, opts Options) {
	defer func() {
		if p := recover(); p != nil {
			o.Log.Printf("cycle panic recovered: %v", p)
		}
	}()
	if o.HealthChecker != nil {
		if err := o.HealthChecker.VerifyConnectivity(ctx); err != nil {
			o.Log.Printf("WARNING: health check failed (continuing anyway): %v", err)
		}
	}
	s, err := o.RunOnce(ctx, opts)
	if err != nil {
		o.Log.Printf("run aborted: %v", err)
	}
	o.LogSummary(s)
}

// IndexOne runs the full sync→change-detect→graphify→import pipeline for one
// repository, persists state, and returns the result. It is the single-repo
// entry point that webhook handlers, queue consumers, or future parallel
// workers should call — RunOnce composes it for the configured set.
//
// IndexOne never panics: panics in any stage are recovered into a
// StagePanic-tagged StatusFailed result. State persistence failures are
// logged but do not propagate, so a transient disk error does not block the
// next repo.
func (o *Orchestrator) IndexOne(ctx context.Context, repo Repository, force bool) RepoResult {
	start := o.now()
	prev, _ := o.Store.Get(repo.Name)

	result := RepoResult{
		Name:       repo.Name,
		URL:        repo.URL,
		Branch:     repo.Branch,
		PrevCommit: prev.LastIndexedCommit,
		StartedAt:  start,
	}

	defer func() {
		if p := recover(); p != nil {
			result.Status = StatusFailed
			result.Stage = StagePanic
			result.Error = fmt.Sprintf("panic: %v", p)
			result.Duration = o.now().Sub(start)
			o.persistResult(repo.Name, result, ctx)
		}
	}()

	o.runPipeline(ctx, repo, force, prev, start, &result)
	o.persistResult(repo.Name, result, ctx)
	o.logResult(result)
	return result
}

func (o *Orchestrator) persistResult(name string, r RepoResult, ctx context.Context) {
	if r.Canceled {
		// A canceled run is not the repo's fault — leave state untouched
		// so consecutive_fails and last_error are not polluted by SIGINT.
		return
	}
	if err := o.Store.Set(toState(r, o.Store)); err != nil {
		o.Log.Printf("[%s] WARNING: persist state failed: %v", name, err)
	}
}

// runPipeline performs the per-stage work, writing progress directly into
// the caller-provided *RepoResult. This lets the deferred recover in
// IndexOne see partial progress (Commit, Nodes) when a stage panics.
func (o *Orchestrator) runPipeline(ctx context.Context, repo Repository, force bool, prev RepoState, start time.Time, result *RepoResult) {
	repoPath := filepath.Join(o.WorkDir, "repos", repo.Name)
	// graphify update writes its output INSIDE the repo working tree
	// (<repo>/graphify-out/graph.json), so there is no separate out_dir
	// argument. Untracked graphify-out/ survives `git reset --hard`, giving
	// the update command its prior-state cache for free.

	o.Log.Printf("[%s] sync %s @ %s", repo.Name, repo.URL, repo.Branch)
	commit, err := o.Syncer.Sync(ctx, repo, repoPath)
	if o.recordFailure(result, StageSync, err, start, ctx) {
		return
	}
	result.Commit = commit

	if ctx.Err() != nil {
		o.markCanceled(result, StageSync, start)
		return
	}

	if !force && prev.LastStatus == StatusSuccess && prev.LastIndexedCommit == commit {
		result.Status = StatusSkipped
		result.Reason = fmt.Sprintf("HEAD %s unchanged since %s", commit, prev.LastIndexedAt.Format(time.RFC3339))
		result.Duration = o.now().Sub(start)
		return
	}

	o.Log.Printf("[%s] graphify %s", repo.Name, commit)
	graphPath, err := o.Graphify.Generate(ctx, repoPath)
	if o.recordFailure(result, StageGraphify, err, start, ctx) {
		return
	}

	if ctx.Err() != nil {
		o.markCanceled(result, StageGraphify, start)
		return
	}

	// Run platform extractors and merge their fragments into the unified
	// graphify-format file the importer will read. One extractor failing
	// records an error on the RepoResult but never blocks the others or
	// the import; this matches the "be resilient" spec point.
	if o.Extractors != nil && len(o.Extractors.Extractors) > 0 {
		o.Log.Printf("[%s] extract (%d extractors)", repo.Name, len(o.Extractors.Extractors))
		extResult := o.Extractors.Run(ctx, repoPath, repo.Name)
		if len(extResult.Errors) > 0 {
			result.ExtractorErrors = map[string]string{}
			for n, e := range extResult.Errors {
				result.ExtractorErrors[n] = e.Error()
			}
		}
		if len(extResult.Fragments) > 0 {
			result.ExtractorStats = map[string]ExtractorStat{}
			for _, f := range extResult.Fragments {
				result.ExtractorStats[f.Extractor] = ExtractorStat{
					Nodes: len(f.Nodes),
					Edges: len(f.Edges),
				}
			}
			if err := extract.MergeIntoGraphFile(graphPath, extResult.Fragments); err != nil {
				if o.recordFailure(result, StageMerge, err, start, ctx) {
					return
				}
			}
		}
	}

	if ctx.Err() != nil {
		o.markCanceled(result, StageExtract, start)
		return
	}

	o.Log.Printf("[%s] import %s", repo.Name, graphPath)
	sum, err := o.Importer.Run(ctx, repo.Name, commit, graphPath)
	if o.recordFailure(result, StageImport, err, start, ctx) {
		return
	}

	result.Status = StatusSuccess
	result.Nodes = sum.NodesTotal
	result.Links = sum.LinksImported
	result.NodesSwept = sum.NodesSwept
	result.EdgesSwept = sum.EdgesSwept
	result.NodesInGraph = sum.NodesInGraph
	result.Mismatch = sum.NodesMismatch()
	if result.Mismatch {
		o.Log.Printf("[%s] WARNING: node-count mismatch — imported %d, Neo4j holds %d (delta %d). Investigate node_key collisions.",
			repo.Name, sum.NodesTotal, sum.NodesInGraph, sum.NodesTotal-sum.NodesInGraph)
	}
	result.Duration = o.now().Sub(start)
}

// recordFailure writes a Stage-tagged failure to result if err is non-nil
// AND the failure was the repo's fault. If ctx was canceled, the run is
// marked Canceled (not Failed) so consecutive_fails is not polluted by
// operator-initiated shutdowns. Returns true if the caller should bail out.
func (o *Orchestrator) recordFailure(r *RepoResult, stage Stage, err error, start time.Time, ctx context.Context) bool {
	if err == nil {
		return false
	}
	r.Duration = o.now().Sub(start)
	if ctx.Err() != nil {
		o.markCanceled(r, stage, start)
		return true
	}
	r.Status = StatusFailed
	r.Stage = stage
	r.Error = err.Error()
	return true
}

func (o *Orchestrator) markCanceled(r *RepoResult, stage Stage, start time.Time) {
	r.Status = StatusSkipped
	r.Stage = stage
	r.Canceled = true
	r.Reason = "canceled"
	if r.Duration == 0 {
		r.Duration = o.now().Sub(start)
	}
}

// LogSummary writes a human-readable summary block to the orchestrator's logger.
func (o *Orchestrator) LogSummary(s RunSummary) {
	total, success, skipped, failed := s.Counts()
	dur := s.FinishedAt.Sub(s.StartedAt).Round(time.Millisecond)
	if s.FinishedAt.IsZero() {
		dur = 0
	}
	o.Log.Printf("--- indexing summary (%s elapsed) ---", dur)
	o.Log.Printf("  total: %d  success: %d  skipped: %d  failed: %d", total, success, skipped, failed)
	for _, r := range s.Results {
		switch r.Status {
		case StatusSuccess:
			swept := ""
			if r.NodesSwept > 0 || r.EdgesSwept > 0 {
				swept = fmt.Sprintf(", swept %d/%d", r.NodesSwept, r.EdgesSwept)
			}
			mismatch := ""
			if r.Mismatch {
				mismatch = fmt.Sprintf(" [MISMATCH: %d in graph]", r.NodesInGraph)
			}
			o.Log.Printf("  + %s @ %s: %d nodes, %d links (%s%s)%s", r.Name, shortSHA(r.Commit), r.Nodes, r.Links, r.Duration.Round(time.Millisecond), swept, mismatch)
		case StatusSkipped:
			o.Log.Printf("  = %s @ %s: %s", r.Name, shortSHA(r.Commit), r.Reason)
		case StatusFailed:
			o.Log.Printf("  ! %s: %s failed: %s", r.Name, r.Stage, r.Error)
		}
	}
}

func (o *Orchestrator) logResult(r RepoResult) {
	switch r.Status {
	case StatusSuccess:
		o.Log.Printf("[%s] success: %d nodes, %d links in %s (swept %d nodes / %d edges)",
			r.Name, r.Nodes, r.Links, r.Duration.Round(time.Millisecond), r.NodesSwept, r.EdgesSwept)
	case StatusSkipped:
		if r.Canceled {
			o.Log.Printf("[%s] canceled during %s", r.Name, r.Stage)
		} else {
			o.Log.Printf("[%s] skipped: %s", r.Name, r.Reason)
		}
	case StatusFailed:
		o.Log.Printf("[%s] FAILED (%s): %s", r.Name, r.Stage, r.Error)
	}
}

func (o *Orchestrator) now() time.Time {
	if o.Clock != nil {
		return o.Clock()
	}
	return time.Now()
}

// toState merges a result into existing state. Existing fields are preserved
// for repos that were skipped, except LastAttemptAt which always advances.
func toState(r RepoResult, store StateStore) RepoState {
	prev, _ := store.Get(r.Name)
	out := prev
	out.Name = r.Name
	now := r.StartedAt.Add(r.Duration)
	out.LastAttemptAt = now
	out.LastStatus = r.Status
	out.LastStage = r.Stage
	out.LastDurationMS = r.Duration.Milliseconds()

	switch r.Status {
	case StatusSuccess:
		out.LastIndexedAt = now
		out.LastIndexedCommit = r.Commit
		out.LastNodes = r.Nodes
		out.LastLinks = r.Links
		out.LastError = ""
		out.ConsecutiveFails = 0
	case StatusSkipped:
		// Skipped (no-change) and canceled both clear the error counter.
		out.LastError = ""
		out.ConsecutiveFails = 0
	case StatusFailed:
		out.LastError = r.Error
		out.ConsecutiveFails = prev.ConsecutiveFails + 1
	}
	return out
}

func filterRepos(all []Repository, names []string) ([]Repository, error) {
	if len(names) == 0 {
		return all, nil
	}
	wanted := make(map[string]bool, len(names))
	for _, n := range names {
		wanted[n] = true
	}
	out := make([]Repository, 0, len(names))
	for _, r := range all {
		if wanted[r.Name] {
			out = append(out, r)
			delete(wanted, r.Name)
		}
	}
	if len(wanted) > 0 {
		var missing []string
		for n := range wanted {
			missing = append(missing, n)
		}
		return nil, fmt.Errorf("unknown repositories: %v", missing)
	}
	return out, nil
}

func shortSHA(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
