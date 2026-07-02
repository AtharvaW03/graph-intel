package extract

import "testing"

func TestAddNodeDedupesAndMerges(t *testing.T) {
	f := NewFragment("test")
	f.AddNode(FragmentNode{ID: "a", Label: "A"})
	f.AddNode(FragmentNode{ID: "a", Label: "ignored", SourceFile: "x.go", Metadata: map[string]any{"k": "v"}})
	f.AddNode(FragmentNode{ID: "b", Label: "B"})

	if len(f.Nodes) != 2 {
		t.Fatalf("nodes = %d, want 2", len(f.Nodes))
	}
	a := f.Nodes[0]
	if a.Label != "A" {
		t.Errorf("first-write-wins violated: label = %q", a.Label)
	}
	if a.SourceFile != "x.go" {
		t.Errorf("empty field not filled from later add: source_file = %q", a.SourceFile)
	}
	if a.Metadata["k"] != "v" {
		t.Errorf("metadata not merged: %v", a.Metadata)
	}
	if a.Origin != "platform" {
		t.Errorf("origin default = %q, want platform", a.Origin)
	}
}

func TestAddEdgeDefaults(t *testing.T) {
	f := NewFragment("test")
	f.AddEdge(FragmentEdge{Source: "a", Target: "b", Relation: "calls"})
	e := f.Edges[0]
	if e.Confidence != ConfidenceExtracted || e.ConfidenceScore != 1.0 || e.Weight != 1.0 {
		t.Errorf("defaults not applied: %+v", e)
	}

	f.AddEdge(FragmentEdge{Source: "a", Target: "b", Relation: "calls", Confidence: ConfidenceInferred})
	if got := f.Edges[1].ConfidenceScore; got != 0.75 {
		t.Errorf("inferred score = %v, want 0.75", got)
	}
}

func TestValidate(t *testing.T) {
	good := NewFragment("t")
	good.AddNode(FragmentNode{ID: "a", Label: "A"})
	good.AddEdge(FragmentEdge{Source: "a", Target: "external-node", Relation: "calls"})
	if err := good.Validate(); err != nil {
		t.Errorf("dangling edge target must be allowed at validate time: %v", err)
	}

	bad := NewFragment("t")
	bad.AddNode(FragmentNode{ID: "", Label: "A"})
	if err := bad.Validate(); err == nil {
		t.Error("missing node ID accepted")
	}

	badEdge := NewFragment("t")
	badEdge.Edges = append(badEdge.Edges, FragmentEdge{Source: "a", Target: "b", Relation: "calls", Confidence: "MAYBE"})
	if err := badEdge.Validate(); err == nil {
		t.Error("invalid confidence accepted")
	}
}
