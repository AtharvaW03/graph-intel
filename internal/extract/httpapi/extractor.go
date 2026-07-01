// Package httpapi extracts HTTP routes exposed by a repository across the
// major backend frameworks used at Angel One: gin/echo/chi/mux/net-http (Go),
// Spring (Java/Kotlin), Express (JS/TS), Flask/FastAPI/Django (Python), and
// ASP.NET attributes (.NET).
//
// The extractor is intentionally heuristic — full-grammar parsing of every
// supported framework would balloon the codebase and is unnecessary given the
// grep-level patterns each framework standardizes around. Confidence on every
// emitted edge is INFERRED, reflecting the heuristic nature.
package httpapi

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"graph-platform/internal/extract"
)

type Extractor struct {
	MaxFileBytes int64 // skip files larger than this (defensive); 0 = 2 MiB default
}

func New() *Extractor { return &Extractor{MaxFileBytes: 2 * 1024 * 1024} }

func (e *Extractor) Name() string { return "httpapi" }

// route is the unified shape every language-specific matcher returns.
type route struct {
	Method  string // GET | POST | PUT | PATCH | DELETE | HEAD | OPTIONS | ANY
	Path    string
	Handler string // empty if not statically resolvable
	Line    int
}

// matcher fingerprints a single source file by extension and applies the
// matching framework's regex set. Each language family lives in its own file.
type matcher func(line string, lineNum int) []route

// matchers per file extension. Each entry is a NON-OVERLAPPING set: matchGin
// covers gin / echo / chi-upper / generic recv.METHOD patterns; matchChi
// supplements with chi's lowercase aliases; matchGorillaMux and matchNetHTTP
// catch their respective specific shapes. Duplication across matchers would
// produce duplicate route nodes (caught only by Fragment.AddNode's dedup).
var matchers = map[string][]matcher{
	".go":   {matchGin, matchChi, matchGorillaMux, matchNetHTTP},
	".py":   {matchFlaskFastAPI, matchDjango},
	".js":   {matchExpress},
	".jsx":  {matchExpress},
	".ts":   {matchExpress},
	".tsx":  {matchExpress},
	".mjs":  {matchExpress},
	".java": {matchSpring},
	".kt":   {matchSpring},
	".kts":  {matchSpring},
	".cs":   {matchAspNet},
	".fs":   {matchAspNet},
	".vb":   {matchAspNet},
	".rb":   {matchRails},
	".php":  {matchLaravel},
}

func (e *Extractor) Extract(ctx context.Context, repoPath, repoName string) (*extract.Fragment, error) {
	frag := extract.NewFragment(e.Name())
	repoNodeID := "repo::" + repoName

	maxBytes := e.MaxFileBytes
	if maxBytes <= 0 {
		maxBytes = 2 * 1024 * 1024
	}

	walk := func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			if d != nil && d.IsDir() && shouldSkipDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ext := strings.ToLower(filepath.Ext(path))
		ms, ok := matchers[ext]
		if !ok {
			return nil
		}
		info, statErr := d.Info()
		if statErr != nil || info.Size() > maxBytes {
			return nil
		}
		rel, _ := filepath.Rel(repoPath, path)
		rel = filepath.ToSlash(rel)

		f, ferr := os.Open(path)
		if ferr != nil {
			frag.Warn(fmt.Sprintf("%s: %v", rel, ferr))
			return nil
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)

		var lastClassAnnotationPath string
		lineNum := 0
		for scanner.Scan() {
			lineNum++
			line := scanner.Text()
			// Spring/ASP.NET class-level @RequestMapping("/api") prefix is
			// applied to method-level mappings below. Track it cheaply.
			if ext == ".java" || ext == ".kt" || ext == ".kts" {
				if p := classLevelMapping(line); p != "" {
					lastClassAnnotationPath = p
				}
			}
			for _, m := range ms {
				rs := m(line, lineNum)
				for _, r := range rs {
					if lastClassAnnotationPath != "" && !strings.HasPrefix(r.Path, "/") {
						r.Path = strings.TrimRight(lastClassAnnotationPath, "/") + "/" + r.Path
					} else if lastClassAnnotationPath != "" && !strings.HasPrefix(r.Path, lastClassAnnotationPath) {
						r.Path = strings.TrimRight(lastClassAnnotationPath, "/") + r.Path
					}
					emitRoute(frag, repoNodeID, repoName, rel, r)
				}
			}
		}
		_ = f.Close()
		return nil
	}

	if err := filepath.WalkDir(repoPath, walk); err != nil {
		return frag, fmt.Errorf("walk repo: %w", err)
	}
	return frag, nil
}

func emitRoute(frag *extract.Fragment, repoNodeID, repoName, file string, r route) {
	if r.Method == "" || r.Path == "" {
		return
	}
	method := strings.ToUpper(r.Method)
	path := normalizePath(r.Path)
	id := "route::" + repoName + "::" + method + "::" + path + "::" + file
	frag.AddNode(extract.FragmentNode{
		ID:             id,
		Label:          method + " " + path,
		Type:           "http_route",
		SourceFile:     file,
		SourceLocation: fmt.Sprintf("L%d", r.Line),
		Metadata: map[string]any{
			"method":  method,
			"path":    path,
			"handler": r.Handler,
			"repo":    repoName,
		},
	})
	frag.AddEdge(extract.FragmentEdge{
		Source:         repoNodeID,
		Target:         id,
		Relation:       "exposes_route",
		Confidence:     extract.ConfidenceInferred,
		SourceFile:     file,
		SourceLocation: fmt.Sprintf("L%d", r.Line),
	})
}

func normalizePath(p string) string {
	p = strings.TrimSpace(p)
	// Strip wrapping quotes if any leaked through.
	p = strings.Trim(p, `"' `)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

func shouldSkipDir(name string) bool {
	switch name {
	case ".git", "node_modules", "vendor", "target", "build", "dist",
		"__pycache__", ".venv", "venv", ".tox", ".gradle", ".idea",
		".vs", "bin", "obj", ".mvn", "tests", "test":
		return true
	}
	return false
}

// classLevelMapping returns the path prefix declared on a Spring
// @RequestMapping/@Path annotation at the class level (rough heuristic — we
// match any @RequestMapping with a literal path string preceding the class
// declaration on the same logical declaration unit).
var classMappingRe = regexp.MustCompile(`@(?:RequestMapping|Path)\s*\(\s*(?:value\s*=\s*)?"([^"]+)"`)

func classLevelMapping(line string) string {
	if !strings.Contains(line, "@RequestMapping") && !strings.Contains(line, "@Path(") {
		return ""
	}
	m := classMappingRe.FindStringSubmatch(line)
	if m == nil {
		return ""
	}
	return m[1]
}
