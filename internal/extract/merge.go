package extract

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"graph-platform/internal/graphify"
)

// MergeIntoGraphFile merges a set of Fragments into an existing
// graphify-format graph.json on disk, producing a unified file the importer
// can read. The semantics match what Graphify's own build_graph() does:
// node IDs are unioned (duplicates collapse, later non-empty fields
// overwrite earlier empty ones); edges are appended.
//
// This in-process merge is preferred over shelling out to
// `graphify merge-graphs` because:
//   - it preserves the exact NetworkX node-link envelope graphify emits
//   - it adds zero process-startup overhead per repo
//   - it lets the orchestrator surface merge errors in Go-native form
//
// The resulting file is written atomically (temp + rename) so a crash
// mid-write never leaves an incomplete graph.json the importer would partially
// read.
func MergeIntoGraphFile(graphPath string, fragments []*Fragment) error {
	raw, err := os.ReadFile(graphPath)
	if err != nil {
		return fmt.Errorf("read graph file: %w", err)
	}

	// Decode into a map so we preserve every top-level field graphify emitted
	// (directed, multigraph, graph metadata, hyperedges, built_at_commit, ...)
	// even ones our Go types don't know about.
	var envelope map[string]any
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("parse graph file: %w", err)
	}

	// Pull out the typed slices we care about.
	existingNodes, _ := envelope["nodes"].([]any)
	existingLinks, _ := envelope["links"].([]any)

	// Index existing nodes by id for fast duplicate detection.
	byID := make(map[string]int, len(existingNodes))
	for i, n := range existingNodes {
		nm, ok := n.(map[string]any)
		if !ok {
			continue
		}
		if id, _ := nm["id"].(string); id != "" {
			byID[id] = i
		}
	}

	// Merge each fragment's nodes.
	for _, frag := range fragments {
		if frag == nil {
			continue
		}
		for _, fn := range frag.Nodes {
			nm := nodeToMap(fn)
			if idx, exists := byID[fn.ID]; exists {
				// Merge into existing entry: existing wins on populated fields,
				// the fragment fills in anything missing.
				prev, _ := existingNodes[idx].(map[string]any)
				for k, v := range nm {
					if !hasMeaningfulValue(prev[k]) {
						prev[k] = v
					}
				}
				existingNodes[idx] = prev
			} else {
				existingNodes = append(existingNodes, nm)
				byID[fn.ID] = len(existingNodes) - 1
			}
		}
		for _, fe := range frag.Edges {
			existingLinks = append(existingLinks, edgeToMap(fe))
		}
	}

	envelope["nodes"] = existingNodes
	envelope["links"] = existingLinks

	// Marshal and atomically replace.
	out, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		return fmt.Errorf("encode merged graph: %w", err)
	}
	dir := filepath.Dir(graphPath)
	tmp, err := os.CreateTemp(dir, ".graph-merge-*.json")
	if err != nil {
		return fmt.Errorf("create merge tmp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(out); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write merge tmp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fsync merge tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close merge tmp: %w", err)
	}
	if err := os.Rename(tmpPath, graphPath); err != nil {
		return fmt.Errorf("rename merge tmp: %w", err)
	}
	tmpPath = ""
	return nil
}

// WriteFragment writes a single fragment to disk as a standalone graphify-
// format node-link JSON file. Useful for debugging and for the
// `graphify merge-graphs` CLI compatibility path.
func WriteFragment(path string, frag *Fragment) error {
	envelope := map[string]any{
		"directed":   false,
		"multigraph": false,
		"graph":      map[string]any{"hyperedges": []any{}},
		"nodes":      fragmentNodes(frag),
		"links":      fragmentLinks(frag),
		"hyperedges": []any{},
	}
	data, err := json.MarshalIndent(envelope, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func fragmentNodes(f *Fragment) []map[string]any {
	if f == nil {
		return nil
	}
	out := make([]map[string]any, 0, len(f.Nodes))
	for _, n := range f.Nodes {
		out = append(out, nodeToMap(n))
	}
	return out
}

func fragmentLinks(f *Fragment) []map[string]any {
	if f == nil {
		return nil
	}
	out := make([]map[string]any, 0, len(f.Edges))
	for _, e := range f.Edges {
		out = append(out, edgeToMap(e))
	}
	return out
}

func nodeToMap(n FragmentNode) map[string]any {
	m := map[string]any{
		"id":    n.ID,
		"label": n.Label,
	}
	if n.NormLabel != "" {
		m["norm_label"] = n.NormLabel
	}
	if n.Origin != "" {
		m["_origin"] = n.Origin
	}
	if n.Type != "" {
		m["type"] = n.Type
	}
	if n.Ecosystem != "" {
		m["ecosystem"] = n.Ecosystem
	}
	if n.FileType != "" {
		m["file_type"] = n.FileType
	}
	if n.SourceFile != "" {
		m["source_file"] = n.SourceFile
	}
	if n.SourceLocation != "" {
		m["source_location"] = n.SourceLocation
	}
	if n.Community != 0 {
		m["community"] = n.Community
	}
	if n.CommunityName != "" {
		m["community_name"] = n.CommunityName
	}
	if len(n.Metadata) > 0 {
		m["metadata"] = n.Metadata
	}
	return m
}

func edgeToMap(e FragmentEdge) map[string]any {
	m := map[string]any{
		"source":   e.Source,
		"target":   e.Target,
		"relation": e.Relation,
	}
	if e.Confidence != "" {
		m["confidence"] = e.Confidence
	}
	if e.ConfidenceScore != 0 {
		m["confidence_score"] = e.ConfidenceScore
	}
	if e.Weight != 0 {
		m["weight"] = e.Weight
	}
	if e.SourceFile != "" {
		m["source_file"] = e.SourceFile
	}
	if e.SourceLocation != "" {
		m["source_location"] = e.SourceLocation
	}
	if e.Context != "" {
		m["context"] = e.Context
	}
	return m
}

func hasMeaningfulValue(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case string:
		return x != ""
	case float64:
		return x != 0
	case int:
		return x != 0
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	default:
		return true
	}
}

// Ensure graphify import is exercised — Validate uses it indirectly via the
// node type field, which graphify.InferLabel maps. Reserve this for a future
// refactor that may want to cross-check that the emitted node.Type is
// recognized by graphify.InferLabel. For now it's a tiny safety net: any
// platform-emitted node whose type is not in graphify's typeToLabel falls
// back to one of the heuristic rules in InferLabel, which is fine.
var _ = graphify.InferLabel
