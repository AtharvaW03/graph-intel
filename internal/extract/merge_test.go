package extract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestMergeIntoGraphFile(t *testing.T) {
	dir := t.TempDir()
	graphPath := filepath.Join(dir, "graph.json")
	envelope := map[string]any{
		"directed":   false,
		"multigraph": false,
		"graph":      map[string]any{"built_at_commit": "abc123"},
		"nodes": []any{
			map[string]any{"id": "func::main", "label": "main()"},
			map[string]any{"id": "repo::svc", "label": "svc"}, // exists, missing type
		},
		"links": []any{
			map[string]any{"source": "func::main", "target": "repo::svc", "relation": "contains"},
		},
	}
	raw, _ := json.Marshal(envelope)
	if err := os.WriteFile(graphPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	frag := NewFragment("deps")
	frag.AddNode(FragmentNode{ID: "repo::svc", Label: "svc", Type: "package"}) // duplicate: fills type
	frag.AddNode(FragmentNode{ID: "pkg::go::lodash", Label: "lodash", Type: "package"})
	frag.AddEdge(FragmentEdge{Source: "repo::svc", Target: "pkg::go::lodash", Relation: "depends_on"})

	if err := MergeIntoGraphFile(graphPath, []*Fragment{frag}); err != nil {
		t.Fatal(err)
	}

	merged, _ := os.ReadFile(graphPath)
	var out map[string]any
	if err := json.Unmarshal(merged, &out); err != nil {
		t.Fatal(err)
	}

	nodes := out["nodes"].([]any)
	links := out["links"].([]any)
	if len(nodes) != 3 {
		t.Errorf("nodes = %d, want 3 (duplicate must collapse)", len(nodes))
	}
	if len(links) != 2 {
		t.Errorf("links = %d, want 2", len(links))
	}
	// Duplicate merge fills the missing field on the existing entry.
	svc := nodes[1].(map[string]any)
	if svc["type"] != "package" {
		t.Errorf("existing node not enriched: type = %v", svc["type"])
	}
	// Unknown envelope fields survive the round trip.
	if g := out["graph"].(map[string]any); g["built_at_commit"] != "abc123" {
		t.Errorf("envelope metadata lost: %v", g)
	}
}
