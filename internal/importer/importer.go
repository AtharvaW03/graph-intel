// Package importer drives the graph.json → Neo4j load as a reusable library.
// It is the single source of truth for the import sequence; the importer CLI
// and the indexer service both call into it.
package importer

import (
	"context"
	"fmt"
	"sort"

	"graph-platform/internal/graphify"
)

// Neo4jClient is the slice of *neo4j.Client behavior the importer needs. The
// interface lives here (not in internal/neo4j) so the importer can be tested
// without a live database and so internal/neo4j can evolve without touching
// the import sequence.
type Neo4jClient interface {
	EnsureConstraints(ctx context.Context) error
	MergeRepository(ctx context.Context, repo string) error
	ImportNodes(ctx context.Context, repo, commit string, nodes []graphify.Node) (map[string]string, map[string]int, error)
	ImportLinks(ctx context.Context, repo, commit string, links []graphify.Link, idToKey map[string]string) (map[string]int, int, int, error)
	SweepStale(ctx context.Context, repo, commit string) (int, int, error)
	CountEntitiesForRepo(ctx context.Context, repo string) (int, error)
}

// Stage names emitted to the Progress callback (and used as error wrapping
// prefixes so callers can log.Fatal(err) and preserve the original CLI's
// "stage: detail" format).
const (
	StageConstraints = "constraints"
	StageRepo        = "merge repository"
	StageNodes       = "import nodes"
	StageLinks       = "import links"
	StageSweep       = "sweep stale"
	StageVerify      = "verify count"
	StageLoad        = "load graph"
)

// Options configures a single import run.
//
// Commit identifies the source revision being imported. When non-empty:
//   - every node and edge is stamped with last_commit = Commit,
//   - SweepStale runs after the import to delete repo-scoped nodes/edges
//     stamped with anything other than Commit, removing data from prior
//     commits that no longer exists in the source tree.
//
// When Commit is empty, neither stamping nor sweeping happens — this is the
// legacy mode used by the importer CLI on a static graph.json.
//
// Progress, if non-nil, is invoked at the start of each stage with a stage
// constant. It exists so the importer CLI can preserve its mid-progress
// prints; long-running daemons (the indexer) can leave it nil.
type Options struct {
	Repo      string
	Commit    string
	GraphPath string
	Progress  func(stage string)
}

// Summary captures everything a caller might want to surface after a run.
// Counts reflect post-allowlist remapping for labels and post-skip totals for
// links, so they match what was actually written to Neo4j.
//
// Hyperedges from the input graph.json are intentionally ignored; the field
// records how many were dropped so callers don't silently assume they were
// processed.
type Summary struct {
	Repo              string
	Commit            string
	NodesTotal        int
	LinksTotal        int
	LinksImported     int
	LabelCounts       map[string]int
	RelationCounts    map[string]int
	SkippedUnknown    int
	SkippedDangling   int
	SkippedHyperedges int
	NodesSwept        int
	EdgesSwept        int
	// NodesInGraph is the :Entity count Neo4j actually holds for this repo
	// after the import completes (post-sweep). Compared against NodesTotal it
	// reveals silent data loss — e.g. before the StableKey.n.ID fix, a repo
	// with 68,058 input nodes ended up with 63,131 in Neo4j; the summary
	// used to report only NodesTotal, hiding the 4,927-node gap.
	NodesInGraph int
}

// NodesMismatch reports whether the graph.json input count and Neo4j's final
// :Entity count for this repo disagree. Meaningful only when Commit != ""
// (the sweep path); in legacy no-commit mode NodesInGraph reflects a
// cumulative count across every prior import so a mismatch is expected.
func (s *Summary) NodesMismatch() bool {
	return s.Commit != "" && s.NodesTotal != s.NodesInGraph
}

// SortedLabels returns label names with stable ordering for human-readable output.
func (s *Summary) SortedLabels() []string {
	return sortedKeys(s.LabelCounts)
}

// SortedRelations returns relation names with stable ordering for human-readable output.
func (s *Summary) SortedRelations() []string {
	return sortedKeys(s.RelationCounts)
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// LoadGraph parses a graph.json file into a *graphify.Graph. Exposed so the
// indexer (and tests) can pre-load a graph and call RunWithGraph to keep the
// parse step out of the import-time critical path if desired.
func LoadGraph(path string) (*graphify.Graph, error) {
	g, err := graphify.Load(path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", StageLoad, err)
	}
	return g, nil
}

// Run loads the graph at opts.GraphPath and imports it via client. It is the
// canonical entry point for the import pipeline.
func Run(ctx context.Context, client Neo4jClient, opts Options) (*Summary, error) {
	g, err := LoadGraph(opts.GraphPath)
	if err != nil {
		return nil, err
	}
	return RunWithGraph(ctx, client, opts.Repo, opts.Commit, g, opts.Progress)
}

// RunWithGraph imports an already-loaded graph. Behavior matches Run but skips
// the JSON parse. Useful when the caller has the graph in memory (e.g. unit
// tests or future streaming variants).
func RunWithGraph(ctx context.Context, client Neo4jClient, repo, commit string, g *graphify.Graph, progress func(string)) (*Summary, error) {
	if progress == nil {
		progress = func(string) {}
	}

	progress(StageConstraints)
	if err := client.EnsureConstraints(ctx); err != nil {
		return nil, fmt.Errorf("%s: %w", StageConstraints, err)
	}

	progress(StageRepo)
	if err := client.MergeRepository(ctx, repo); err != nil {
		return nil, fmt.Errorf("%s: %w", StageRepo, err)
	}

	progress(StageNodes)
	idToKey, labelCounts, err := client.ImportNodes(ctx, repo, commit, g.Nodes)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", StageNodes, err)
	}

	progress(StageLinks)
	relCounts, skippedUnknown, skippedDangling, err := client.ImportLinks(ctx, repo, commit, g.Links, idToKey)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", StageLinks, err)
	}

	var nodesSwept, edgesSwept int
	if commit != "" {
		progress(StageSweep)
		ns, es, err := client.SweepStale(ctx, repo, commit)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", StageSweep, err)
		}
		nodesSwept, edgesSwept = ns, es
	}

	progress(StageVerify)
	nodesInGraph, err := client.CountEntitiesForRepo(ctx, repo)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", StageVerify, err)
	}

	return &Summary{
		Repo:              repo,
		Commit:            commit,
		NodesTotal:        len(g.Nodes),
		LinksTotal:        len(g.Links),
		LinksImported:     len(g.Links) - skippedUnknown - skippedDangling,
		LabelCounts:       labelCounts,
		RelationCounts:    relCounts,
		SkippedUnknown:    skippedUnknown,
		SkippedDangling:   skippedDangling,
		SkippedHyperedges: len(g.HyperEdges),
		NodesSwept:        nodesSwept,
		EdgesSwept:        edgesSwept,
		NodesInGraph:      nodesInGraph,
	}, nil
}
