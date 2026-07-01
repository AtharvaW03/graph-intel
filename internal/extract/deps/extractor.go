package deps

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"graph-platform/internal/extract"
)

// Extractor scans a repo for known dependency manifests and emits a unified
// Graphify-compatible fragment describing every external dependency the
// repo declares. It is the platform's analogue of graphify's own apm.yml /
// go.mod / pyproject.toml / pom.xml extraction (which only covers 4
// manifest types) — extended to cover the 13 ecosystems used at Angel One.
type Extractor struct {
	// OrgPrefixes are package-name prefixes that identify "internal" repos
	// in the org's package namespace. A dep that matches a prefix produces
	// an additional Repository→Repository edge so cross-repo dependency
	// queries become one-hop traversals.
	//
	// Examples: "github.com/angel-one/", "github.com/atharva-co/".
	OrgPrefixes []string

	// MaxManifests caps the number of manifest files inspected per repo
	// (defensive against monorepos with thousands of nested manifests).
	// Zero or negative means no cap.
	MaxManifests int
}

func New(orgPrefixes []string) *Extractor {
	return &Extractor{OrgPrefixes: orgPrefixes, MaxManifests: 1000}
}

func (e *Extractor) Name() string { return "deps" }

// parserFn parses a single manifest file. Returns the deps it found plus an
// optional non-fatal error description (the caller turns it into a Warning).
type parserFn func(path, contents string) ([]Dep, error)

// dispatchByBasename maps manifest filenames to their parser. Some
// ecosystems (Gradle, .NET) have multiple file extensions; those are handled
// in dispatchByExt below.
var dispatchByBasename = map[string]parserFn{
	"go.mod":           parseGoMod,
	"package.json":     parsePackageJSON,
	"pyproject.toml":   parsePyProject,
	"requirements.txt": parseRequirementsTxt,
	"Pipfile":          parsePipfile,
	"pom.xml":          parsePomXML,
	"build.sbt":        parseBuildSbt,
	"Cargo.toml":       parseCargoToml,
	"composer.json":    parseComposerJSON,
	"Gemfile":          parseGemfile,
	"Package.swift":    parsePackageSwift,
	"CMakeLists.txt":   parseCMakeLists,
	"conanfile.txt":    parseConanFile,
	"conanfile.py":     parseConanFile,
	"vcpkg.json":       parseVcpkgJSON,
}

// dispatchByExt maps file suffixes to parsers for ecosystems whose manifests
// have well-defined extensions but variable basenames (Gradle, .NET).
type extEntry struct {
	suffix string
	parse  parserFn
}

var dispatchByExt = []extEntry{
	{".gradle", parseBuildGradle},
	{".gradle.kts", parseBuildGradle},
	{".csproj", parseDotNetProj},
	{".fsproj", parseDotNetProj},
	{".vbproj", parseDotNetProj},
}

// Extract walks repoPath, dispatches each manifest to its parser, and emits
// one Package node per unique (ecosystem, name) pair plus DEPENDS_ON edges
// from the repo. The repoName argument is used as the source side of every
// DEPENDS_ON edge so the relationship is anchored to (:Repository {name}).
func (e *Extractor) Extract(ctx context.Context, repoPath, repoName string) (*extract.Fragment, error) {
	frag := extract.NewFragment(e.Name())
	if repoPath == "" {
		return frag, fmt.Errorf("empty repoPath")
	}

	repoNodeID := "repo::" + repoName
	frag.AddNode(extract.FragmentNode{
		ID:    repoNodeID,
		Label: repoName,
		Type:  "package", // repos behave like package hubs for dep queries
		Metadata: map[string]any{
			"is_repository": true,
		},
	})

	seenPkg := map[string]bool{}
	seenEdge := map[string]bool{}
	seenRepo := map[string]bool{}
	manifestsScanned := 0

	walk := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable entries are skipped, not fatal
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if d.IsDir() {
			if shouldSkipDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if e.MaxManifests > 0 && manifestsScanned >= e.MaxManifests {
			return filepath.SkipAll
		}

		parser, rel := matchParser(repoPath, path, d.Name())
		if parser == nil {
			return nil
		}
		manifestsScanned++

		contents, rerr := readFileLimited(path, 8*1024*1024)
		if rerr != nil {
			frag.Warn(fmt.Sprintf("%s: %v", rel, rerr))
			return nil
		}
		dlist, perr := parser(rel, contents)
		if perr != nil {
			frag.Warn(fmt.Sprintf("%s: %v", rel, perr))
			return nil
		}
		for _, dep := range dlist {
			dep.Manifest = rel
			pkgID := PackageNodeID(dep)
			if !seenPkg[pkgID] {
				seenPkg[pkgID] = true
				frag.AddNode(extract.FragmentNode{
					ID:        pkgID,
					Label:     dep.Name,
					Type:      "package",
					Ecosystem: dep.Ecosystem,
					Metadata: map[string]any{
						"version":  dep.Version,
						"scope":    dep.Scope,
						"manifest": dep.Manifest,
					},
				})
			}
			edgeKey := repoNodeID + "->" + pkgID
			if !seenEdge[edgeKey] {
				seenEdge[edgeKey] = true
				frag.AddEdge(extract.FragmentEdge{
					Source:     repoNodeID,
					Target:     pkgID,
					Relation:   "depends_on",
					Confidence: extract.ConfidenceExtracted,
					SourceFile: dep.Manifest,
					Context:    dep.Scope,
				})
			}
			// Cross-repo link when the dep maps to an internal repo name.
			if internal := InternalRepoNameFromDep(dep.Name, e.OrgPrefixes); internal != "" && internal != repoName {
				internalID := "repo::" + internal
				if !seenRepo[internalID] {
					seenRepo[internalID] = true
					frag.AddNode(extract.FragmentNode{
						ID:    internalID,
						Label: internal,
						Type:  "package",
						Metadata: map[string]any{
							"is_repository": true,
							"discovered_as": dep.Name,
						},
					})
				}
				key2 := repoNodeID + "=>" + internalID
				if !seenEdge[key2] {
					seenEdge[key2] = true
					frag.AddEdge(extract.FragmentEdge{
						Source:     repoNodeID,
						Target:     internalID,
						Relation:   "depends_on_repo",
						Confidence: extract.ConfidenceInferred,
						SourceFile: dep.Manifest,
						Context:    "inferred from org-prefix match",
					})
				}
			}
		}
		return nil
	}

	if err := filepath.WalkDir(repoPath, walk); err != nil && err != filepath.SkipAll {
		return frag, fmt.Errorf("walk repo: %w", err)
	}

	return frag, nil
}

// matchParser returns the parser to use for a file plus the repo-relative path.
// Returns (nil, rel) when the file is not a recognized manifest.
func matchParser(repoRoot, path, base string) (parserFn, string) {
	rel, _ := filepath.Rel(repoRoot, path)
	rel = filepath.ToSlash(rel)
	if p, ok := dispatchByBasename[base]; ok {
		return p, rel
	}
	for _, e := range dispatchByExt {
		if strings.HasSuffix(base, e.suffix) {
			return e.parse, rel
		}
	}
	return nil, rel
}

// shouldSkipDir tells the walker to skip vendor/build/cache directories so we
// don't scan generated dependency lockfiles or copies of vendored packages.
func shouldSkipDir(name string) bool {
	switch name {
	case ".git", "node_modules", "vendor", "target", "build", "dist",
		"__pycache__", ".venv", "venv", ".tox", ".gradle", ".idea",
		".vs", "bin", "obj", ".mvn":
		return true
	}
	return false
}
