// Package deps implements repository dependency extraction across the
// ecosystems used at Angel One. Each parser is a pure function that consumes
// one manifest file and emits a slice of Dep entries; the package-level
// Extractor walks the repo, dispatches to parsers by manifest filename, and
// produces a single Graphify-compatible Fragment containing:
//   - one (:Package {name}) node per distinct dependency
//   - (:Repository)-[:DEPENDS_ON]->(:Package) edges
//   - (:Repository)-[:DEPENDS_ON_REPO]->(:Repository) edges when a
//     dependency name matches a configured internal-org prefix (so questions
//     like "which repos depend on auth-service?" become a one-hop traversal)
//
// The Extractor is registered automatically by the indexer; per-repo
// enablement is the indexer's concern, not the extractor's.
package deps

import (
	"strings"
)

// Dep is one normalized dependency extracted from any manifest format.
type Dep struct {
	Name      string // canonical package identifier
	Version   string // optional; empty if the manifest declared none
	Ecosystem string // go | npm | pypi | maven | gradle | sbt | cargo | nuget | composer | rubygems | swiftpm | cmake | conan | vcpkg
	Scope     string // optional: runtime | dev | test | build; empty for unspecified
	Manifest  string // path to the manifest file the dep was found in (repo-relative)
}

// PackageNodeID is the canonical fragment-node ID for a package. Ecosystem
// is folded into the ID so a Go package and an npm package with the same
// short name don't collide in the unified graph.
func PackageNodeID(d Dep) string {
	return "pkg::" + d.Ecosystem + "::" + safeID(d.Name)
}

// safeID makes a string usable inside a fragment-node ID — no spaces, no
// path-traversal sequences, lowercased so manifest casing variations
// (org/Repo vs org/repo) collapse to one node.
func safeID(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "..", "_")
	return s
}

// InternalRepoNameFromDep returns the internal repository name for a dep if
// the dep matches any of the configured org prefixes (e.g.
// "github.com/angel-one/auth-service" → "auth-service"). Empty string when
// no prefix matches.
//
// Prefixes should include the trailing slash. Order matters: the first
// matching prefix wins.
func InternalRepoNameFromDep(name string, orgPrefixes []string) string {
	for _, prefix := range orgPrefixes {
		if prefix == "" {
			continue
		}
		if strings.HasPrefix(name, prefix) {
			tail := strings.TrimPrefix(name, prefix)
			// Strip trailing "/v2"-style Go module suffixes and version tags.
			if i := strings.Index(tail, "/"); i >= 0 {
				tail = tail[:i]
			}
			return tail
		}
	}
	return ""
}
