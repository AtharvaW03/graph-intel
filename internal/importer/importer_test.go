package importer

import (
	"context"
	"testing"

	"graph-platform/internal/graphify"
)

// fakeClient records the call sequence and the arguments the importer passed,
// so the import pipeline can be verified without a live Neo4j.
type fakeClient struct {
	calls       []string
	nodesRepo   string
	nodesCommit string
	linksRepo   string
	linksCommit string
	sweepRepo   string
	sweepCommit string
}

func (f *fakeClient) EnsureConstraints(context.Context) error {
	f.calls = append(f.calls, "constraints")
	return nil
}

func (f *fakeClient) MergeRepository(_ context.Context, repo string) error {
	f.calls = append(f.calls, "repo")
	return nil
}

func (f *fakeClient) ImportNodes(_ context.Context, repo, commit string, nodes []graphify.Node) (map[string]string, map[string]int, error) {
	f.calls = append(f.calls, "nodes")
	f.nodesRepo, f.nodesCommit = repo, commit
	idToKey := map[string]string{}
	counts := map[string]int{}
	for _, n := range nodes {
		idToKey[n.ID] = graphify.StableKey(repo, n)
		counts[graphify.InferLabel(n)]++
	}
	return idToKey, counts, nil
}

func (f *fakeClient) ImportLinks(_ context.Context, repo, commit string, links []graphify.Link, idToKey map[string]string) (map[string]int, int, int, error) {
	f.calls = append(f.calls, "links")
	f.linksRepo, f.linksCommit = repo, commit
	counts := map[string]int{}
	unknown, dangling := 0, 0
	for _, l := range links {
		rel, ok := graphify.MapRelation(l.Relation)
		if !ok {
			unknown++
			continue
		}
		if _, ok1 := idToKey[l.Source]; !ok1 {
			dangling++
			continue
		}
		if _, ok2 := idToKey[l.Target]; !ok2 {
			dangling++
			continue
		}
		counts[rel]++
	}
	return counts, unknown, dangling, nil
}

func (f *fakeClient) SweepStale(_ context.Context, repo, commit string) (int, int, error) {
	f.calls = append(f.calls, "sweep")
	f.sweepRepo, f.sweepCommit = repo, commit
	return 2, 1, nil
}

func (f *fakeClient) CountEntitiesForRepo(context.Context, string) (int, error) {
	f.calls = append(f.calls, "verify")
	return 2, nil
}

func testGraph() *graphify.Graph {
	return &graphify.Graph{
		Nodes: []graphify.Node{
			{ID: "n1", Label: "main()"},
			{ID: "n2", Label: "helper()"},
		},
		Links: []graphify.Link{
			{Source: "n1", Target: "n2", Relation: "calls"},
			{Source: "n1", Target: "n2", Relation: "made_up"},  // unknown relation
			{Source: "n1", Target: "ghost", Relation: "calls"}, // dangling target
		},
		HyperEdges: []graphify.HyperEdge{{}},
	}
}

func TestRunWithGraphSequenceAndSummary(t *testing.T) {
	f := &fakeClient{}
	sum, err := RunWithGraph(context.Background(), f, "svc", "abc123", testGraph(), nil)
	if err != nil {
		t.Fatal(err)
	}

	wantCalls := []string{"constraints", "repo", "nodes", "links", "sweep", "verify"}
	if len(f.calls) != len(wantCalls) {
		t.Fatalf("calls = %v, want %v", f.calls, wantCalls)
	}
	for i, c := range wantCalls {
		if f.calls[i] != c {
			t.Fatalf("call[%d] = %q, want %q (full: %v)", i, f.calls[i], c, f.calls)
		}
	}

	if f.linksRepo != "svc" || f.linksCommit != "abc123" {
		t.Errorf("links got repo=%q commit=%q", f.linksRepo, f.linksCommit)
	}
	if sum.NodesTotal != 2 || sum.LinksTotal != 3 || sum.LinksImported != 1 {
		t.Errorf("summary counts: %+v", sum)
	}
	if sum.SkippedUnknown != 1 || sum.SkippedDangling != 1 || sum.SkippedHyperedges != 1 {
		t.Errorf("skip counts: %+v", sum)
	}
	if sum.NodesSwept != 2 || sum.EdgesSwept != 1 {
		t.Errorf("sweep counts: %+v", sum)
	}
	if sum.NodesMismatch() {
		t.Errorf("2 in, 2 in graph — mismatch flagged: %+v", sum)
	}
}

func TestRunWithGraphLegacyModeSkipsSweep(t *testing.T) {
	f := &fakeClient{}
	sum, err := RunWithGraph(context.Background(), f, "svc", "", testGraph(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range f.calls {
		if c == "sweep" {
			t.Error("sweep ran with empty commit")
		}
	}
	if sum.NodesMismatch() {
		t.Error("mismatch must be meaningless (false) in legacy no-commit mode")
	}
}
