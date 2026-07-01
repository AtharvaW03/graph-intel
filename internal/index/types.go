// Package index provides the indexing service: it discovers repositories,
// keeps them cloned and in sync, runs Graphify on changed repos, and feeds
// the produced graph.json into the existing Neo4j importer.
//
// The package is designed so future enhancements — GitHub webhooks, SQS or
// other job queues, distributed workers, retry policies, parallel indexing,
// and additional extractors — can be added by swapping out the JobSource,
// Syncer, Graphifier, or StateStore implementations without touching the
// orchestrator or pipeline.
package index

import (
	"context"
	"time"
)

// Status is the outcome of one repository's indexing attempt.
type Status string

const (
	StatusSuccess Status = "success"
	StatusSkipped Status = "skipped"
	StatusFailed  Status = "failed"
)

// Stage identifies where in the pipeline an outcome was produced. When a
// failure occurs, Stage records which step blew up so operators can triage.
type Stage string

const (
	StageSync     Stage = "sync"
	StageGraphify Stage = "graphify"
	StageExtract  Stage = "extract"
	StageMerge    Stage = "merge"
	StageImport   Stage = "import"
	StagePanic    Stage = "panic"
)

// Repository is one entry in the indexer's repo manifest. Fields are
// YAML-tagged so the configuration file is readable by ops.
type Repository struct {
	Name   string `yaml:"name"`
	URL    string `yaml:"url"`
	Branch string `yaml:"branch"`
}

// RepoState is the persisted indexing record for one repository. It survives
// process restarts; the indexer uses LastIndexedCommit to decide whether a
// repo has actually changed since the last successful run.
type RepoState struct {
	Name              string    `json:"name"`
	LastAttemptAt     time.Time `json:"last_attempt_at,omitempty"`
	LastIndexedAt     time.Time `json:"last_indexed_at,omitempty"`
	LastIndexedCommit string    `json:"last_indexed_commit,omitempty"`
	LastStatus        Status    `json:"last_status,omitempty"`
	LastStage         Stage     `json:"last_stage,omitempty"`
	LastError         string    `json:"last_error,omitempty"`
	LastDurationMS    int64     `json:"last_duration_ms,omitempty"`
	LastNodes         int       `json:"last_nodes,omitempty"`
	LastLinks         int       `json:"last_links,omitempty"`
	ConsecutiveFails  int       `json:"consecutive_fails,omitempty"`
}

// RepoResult captures everything that happened to a single repo during a run.
// It is the unit of return from the per-repo pipeline.
type RepoResult struct {
	Name       string
	URL        string
	Branch     string
	Status     Status
	Stage      Stage
	Commit     string
	PrevCommit string
	Reason     string
	Error      string
	Nodes      int
	Links      int
	NodesSwept int
	EdgesSwept int
	// NodesInGraph is Neo4j's actual :Entity count for the repo after import.
	// Compared to Nodes it exposes silent data loss (e.g. node_key collisions).
	NodesInGraph int
	// Mismatch is true when NodesInGraph != Nodes with commit stamping on —
	// a signal that the importer's input and Neo4j's post-import state diverged.
	Mismatch bool
	// ExtractorStats records per-extractor node/edge counts so operators can
	// see at a glance which extractors contributed how much to the unified
	// graph. Empty for runs where no extractors were registered.
	ExtractorStats map[string]ExtractorStat
	// ExtractorErrors maps extractor name → error message for extractors
	// that failed for this repo. Other extractors' fragments still go through.
	ExtractorErrors map[string]string
	// Canceled is true when ctx cancellation, not the repo itself, caused the
	// run to stop early. State persistence is skipped in this case so a SIGINT
	// does not pollute consecutive_fails or overwrite last_error with a kill
	// signal.
	Canceled  bool
	StartedAt time.Time
	Duration  time.Duration
}

// ExtractorStat is a per-extractor node/edge count summary.
type ExtractorStat struct {
	Nodes int
	Edges int
}

// RunSummary is the aggregate of one indexing pass over a set of repositories.
type RunSummary struct {
	StartedAt  time.Time
	FinishedAt time.Time
	Results    []RepoResult
}

// Counts breaks down a RunSummary by outcome for logging.
func (s RunSummary) Counts() (total, success, skipped, failed int) {
	total = len(s.Results)
	for _, r := range s.Results {
		switch r.Status {
		case StatusSuccess:
			success++
		case StatusSkipped:
			skipped++
		case StatusFailed:
			failed++
		}
	}
	return
}

// JobSource yields the set of repositories to index. The YAML-backed
// implementation reads from a config file; future implementations can drain a
// queue, listen to webhooks, or merge multiple sources.
type JobSource interface {
	Repositories(ctx context.Context) ([]Repository, error)
}

// Syncer ensures a repo is cloned at dest and synchronized to its branch.
// It returns the current HEAD commit so the orchestrator can compare against
// previously-indexed state.
type Syncer interface {
	Sync(ctx context.Context, repo Repository, dest string) (commit string, err error)
}

// Graphifier produces (or incrementally updates) a graph.json for a repo at
// repoPath. It returns the resolved absolute path of the produced file so
// callers do not need to know the extractor's output layout.
//
// The default implementation invokes `graphify update <repo_path>`, which is
// graphify's incremental, AST-only-by-default workflow — it does NOT pass an
// --out flag because `update` writes inside the repo (at
// <repo_path>/graphify-out/graph.json by default). The output_file knob on
// GraphifyConfig stays relative to repoPath.
type Graphifier interface {
	Generate(ctx context.Context, repoPath string) (graphPath string, err error)
}

// StateStore is the persistence layer for per-repo indexing state.
// The JSON-file implementation is enough for single-process operation; a
// future Redis or Postgres backend can swap in by satisfying this interface.
type StateStore interface {
	Get(name string) (RepoState, bool)
	Set(state RepoState) error
	All() map[string]RepoState
}
