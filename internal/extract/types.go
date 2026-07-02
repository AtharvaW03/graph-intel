// Package extract defines the platform's extractor framework. Extractors
// scan a freshly-synced repository checkout and emit Graphify-compatible
// extraction fragments — the same {nodes, edges} dictionary shape that
// graphify's own `extract()` functions produce.
//
// The orchestrator runs every Extractor for a repo, merges all returned
// Fragments with graphify's main graph.json (via Graphify's NetworkX
// node-link merge semantics), and feeds the unified graph into the existing
// Neo4j importer. This keeps Graphify responsible for graph construction and
// our extractors strictly responsible for producing entity dictionaries.
//
// Adding a new extractor is one struct + one Extract method. No orchestration
// changes are required.
package extract

import (
	"context"
	"fmt"
)

// Confidence labels follow Graphify's three-tier model (see
// docs/graphify/graphify-how-it-works.md). EXTRACTED edges always have
// confidence_score 1.0.
const (
	ConfidenceExtracted = "EXTRACTED"
	ConfidenceInferred  = "INFERRED"
	ConfidenceAmbiguous = "AMBIGUOUS"
)

// FragmentNode mirrors a graphify-format node. Field tags match graphify's
// NetworkX node-link serialization so a Fragment written to disk is a
// drop-in input for `graphify merge-graphs`.
type FragmentNode struct {
	ID             string         `json:"id"`
	Label          string         `json:"label"`
	NormLabel      string         `json:"norm_label,omitempty"`
	Origin         string         `json:"_origin,omitempty"` // matches graphify's _origin field
	Type           string         `json:"type,omitempty"`
	Ecosystem      string         `json:"ecosystem,omitempty"`
	FileType       string         `json:"file_type,omitempty"`
	SourceFile     string         `json:"source_file,omitempty"`
	SourceLocation string         `json:"source_location,omitempty"`
	Community      int            `json:"community,omitempty"`
	CommunityName  string         `json:"community_name,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

// FragmentEdge mirrors a graphify-format edge. The relation field uses
// Graphify's lowercase verb form (e.g. "depends_on", "produces"); the
// importer's MapRelation translates these to Neo4j UPPER_SNAKE_CASE.
type FragmentEdge struct {
	Source          string  `json:"source"`
	Target          string  `json:"target"`
	Relation        string  `json:"relation"`
	Confidence      string  `json:"confidence"`
	ConfidenceScore float64 `json:"confidence_score,omitempty"`
	Weight          float64 `json:"weight,omitempty"`
	SourceFile      string  `json:"source_file,omitempty"`
	SourceLocation  string  `json:"source_location,omitempty"`
	Context         string  `json:"context,omitempty"`
}

// Fragment is one extractor's contribution to the unified graph. Each
// Extractor.Extract returns exactly one Fragment; the orchestrator merges
// them all with graphify's main extraction before import.
type Fragment struct {
	Extractor string         `json:"-"` // set by the runner; not serialized into graphify format
	Nodes     []FragmentNode `json:"nodes"`
	Edges     []FragmentEdge `json:"edges"`
	Warnings  []string       `json:"-"` // non-fatal issues surfaced to operators

	// byID indexes Nodes by ID for AddNode's dedup. Built lazily so a
	// Fragment constructed by literal or JSON unmarshal still works.
	byID map[string]int
}

// NewFragment returns an empty fragment tagged with the extractor's name.
func NewFragment(extractor string) *Fragment {
	return &Fragment{Extractor: extractor}
}

// AddNode appends a node, defaulting common fields and stamping _origin so
// downstream consumers can tell platform-emitted nodes apart from graphify's
// own AST extraction (which uses `_origin: "ast"`).
//
// AddNode is idempotent per ID: a second call with the same id merges its
// metadata into the existing node (later values fill empty fields; existing
// values are preserved). This makes fragments a set-of-nodes naturally and
// lets extractors emit the same hub node from multiple discovery sites
// without tracking a private "seen" map.
func (f *Fragment) AddNode(n FragmentNode) {
	if n.Origin == "" {
		n.Origin = "platform"
	}
	if n.NormLabel == "" {
		n.NormLabel = n.Label
	}
	if f.byID == nil {
		f.byID = make(map[string]int, len(f.Nodes))
		for i := range f.Nodes {
			f.byID[f.Nodes[i].ID] = i
		}
	}
	if i, ok := f.byID[n.ID]; ok {
		mergeNodeInPlace(&f.Nodes[i], n)
		return
	}
	f.byID[n.ID] = len(f.Nodes)
	f.Nodes = append(f.Nodes, n)
}

// mergeNodeInPlace fills empty fields of dst from src; populated fields on
// dst are preserved (first-write wins for top-level fields). Metadata is
// merged key-by-key with the same rule.
func mergeNodeInPlace(dst *FragmentNode, src FragmentNode) {
	if dst.Label == "" {
		dst.Label = src.Label
	}
	if dst.NormLabel == "" {
		dst.NormLabel = src.NormLabel
	}
	if dst.Type == "" {
		dst.Type = src.Type
	}
	if dst.Ecosystem == "" {
		dst.Ecosystem = src.Ecosystem
	}
	if dst.FileType == "" {
		dst.FileType = src.FileType
	}
	if dst.SourceFile == "" {
		dst.SourceFile = src.SourceFile
	}
	if dst.SourceLocation == "" {
		dst.SourceLocation = src.SourceLocation
	}
	if dst.Community == 0 {
		dst.Community = src.Community
	}
	if dst.CommunityName == "" {
		dst.CommunityName = src.CommunityName
	}
	if dst.Origin == "" {
		dst.Origin = src.Origin
	}
	if dst.Metadata == nil {
		dst.Metadata = map[string]any{}
	}
	for k, v := range src.Metadata {
		if _, exists := dst.Metadata[k]; !exists {
			dst.Metadata[k] = v
		}
	}
}

// AddEdge appends an edge, defaulting confidence + confidence_score so an
// extractor that omits them still produces a valid graphify fragment.
func (f *Fragment) AddEdge(e FragmentEdge) {
	if e.Confidence == "" {
		e.Confidence = ConfidenceExtracted
	}
	if e.ConfidenceScore == 0 {
		switch e.Confidence {
		case ConfidenceExtracted:
			e.ConfidenceScore = 1.0
		case ConfidenceInferred:
			e.ConfidenceScore = 0.75
		case ConfidenceAmbiguous:
			e.ConfidenceScore = 0.5
		}
	}
	if e.Weight == 0 {
		e.Weight = 1.0
	}
	f.Edges = append(f.Edges, e)
}

// Warn records a non-fatal issue without aborting extraction.
func (f *Fragment) Warn(msg string) {
	f.Warnings = append(f.Warnings, msg)
}

// Empty reports whether the fragment carries no graph data. Empty fragments
// are dropped before merge so they don't clutter the unified file.
func (f *Fragment) Empty() bool {
	return len(f.Nodes) == 0 && len(f.Edges) == 0
}

// Extractor is the framework's only contract. Each implementation reads files
// under repoPath (a freshly synced clone) and returns a Fragment containing
// the entities it discovered for the given repo. Extractors:
//   - operate independently — no shared state between extractors
//   - emit Graphify-compatible nodes and edges
//   - validate their own output before returning
//   - report errors independently — returning an error never aborts other
//     extractors for the same repo
//   - are pure with respect to disk: they read but never write
type Extractor interface {
	Name() string
	Extract(ctx context.Context, repoPath, repoName string) (*Fragment, error)
}

// Validate enforces the minimal schema invariants build_graph relies on:
// non-empty node IDs and labels, non-empty edge endpoints and relations, and
// a recognized confidence tier. Edges referencing node IDs absent from this
// fragment are deliberately NOT rejected — they may resolve against nodes
// another fragment or graphify itself emits; anything still dangling is
// counted and skipped at import time.
func (f *Fragment) Validate() error {
	if f == nil {
		return fmt.Errorf("nil fragment")
	}
	for i, n := range f.Nodes {
		if n.ID == "" {
			return fmt.Errorf("node[%d]: missing id", i)
		}
		if n.Label == "" {
			return fmt.Errorf("node[%d] (%s): missing label", i, n.ID)
		}
	}
	for i, e := range f.Edges {
		if e.Source == "" || e.Target == "" {
			return fmt.Errorf("edge[%d]: missing source/target", i)
		}
		if e.Relation == "" {
			return fmt.Errorf("edge[%d] (%s -> %s): missing relation", i, e.Source, e.Target)
		}
		if e.Confidence != ConfidenceExtracted && e.Confidence != ConfidenceInferred && e.Confidence != ConfidenceAmbiguous {
			return fmt.Errorf("edge[%d] (%s -> %s): invalid confidence %q", i, e.Source, e.Target, e.Confidence)
		}
	}
	return nil
}
