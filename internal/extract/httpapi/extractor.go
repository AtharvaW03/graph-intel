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

		isJVM := ext == ".java" || ext == ".kt" || ext == ".kts"
		classPrefix := ""
		var pending *pendingMapping
		lineNum := 0
		for scanner.Scan() {
			lineNum++
			line := scanner.Text()
			if isJVM {
				pending, classPrefix = resolveRequestMapping(frag, repoNodeID, repoName, rel, line, lineNum, pending, classPrefix)
				if m := requestMappingRe.FindStringSubmatch(line); m != nil {
					if classDeclRe.MatchString(line) {
						// Annotation and class declaration on one line
						// (common in Kotlin): it's a class-level prefix.
						classPrefix = m[1]
					} else {
						pending = &pendingMapping{path: m[1], method: m[2], line: lineNum}
					}
					continue
				}
			}
			for _, m := range ms {
				for _, r := range m(line, lineNum) {
					if isJVM && classPrefix != "" {
						r.Path = joinPrefix(classPrefix, r.Path)
					}
					emitRoute(frag, repoNodeID, repoName, rel, r)
				}
			}
		}
		if serr := scanner.Err(); serr != nil {
			frag.Warn(fmt.Sprintf("%s: scan: %v", rel, serr))
		}
		// An annotation still pending at EOF annotated nothing we saw —
		// emit it as a route rather than dropping it silently.
		if pending != nil {
			emitPendingAsRoute(frag, repoNodeID, repoName, rel, pending, classPrefix)
		}
		_ = f.Close()
		return nil
	}

	if err := filepath.WalkDir(repoPath, walk); err != nil {
		return frag, fmt.Errorf("walk repo: %w", err)
	}

	// Emit the repo hub node ourselves so EXPOSES_ROUTE edges don't dangle
	// when the deps extractor (which also creates this hub) is disabled.
	if len(frag.Nodes) > 0 {
		frag.AddNode(extract.FragmentNode{
			ID:    repoNodeID,
			Label: repoName,
			Type:  "package",
			Metadata: map[string]any{
				"is_repository": true,
			},
		})
	}
	return frag, nil
}

// pendingMapping is an @RequestMapping annotation whose role — class-level
// path prefix vs method-level route — is not yet known. Spring reuses the
// same annotation for both; only the declaration that follows disambiguates.
type pendingMapping struct {
	path   string
	method string // captured RequestMethod.X; empty means ANY
	line   int
}

var (
	requestMappingRe = regexp.MustCompile(`@RequestMapping\s*\(\s*(?:value\s*=\s*)?"([^"]+)"(?:[^)]*method\s*=\s*RequestMethod\.([A-Z]+))?`)
	classDeclRe      = regexp.MustCompile(`\b(?:class|interface|record|object)\s+\w+`)
)

// resolveRequestMapping advances a pending @RequestMapping against the next
// line: a class/interface declaration promotes it to the class-level prefix,
// any other declaration means it was a method-level route (emitted here), and
// blank lines / comments / stacked annotations keep it pending. Returns the
// updated pending state and class prefix.
func resolveRequestMapping(frag *extract.Fragment, repoNodeID, repoName, file, line string, _ int, pending *pendingMapping, classPrefix string) (*pendingMapping, string) {
	if pending == nil {
		return nil, classPrefix
	}
	t := strings.TrimSpace(line)
	switch {
	case t == "" || strings.HasPrefix(t, "//") || strings.HasPrefix(t, "*") || strings.HasPrefix(t, "/*") || strings.HasPrefix(t, "@"):
		return pending, classPrefix // still looking past comments/annotations
	case classDeclRe.MatchString(t):
		return nil, pending.path
	default:
		emitPendingAsRoute(frag, repoNodeID, repoName, file, pending, classPrefix)
		return nil, classPrefix
	}
}

func emitPendingAsRoute(frag *extract.Fragment, repoNodeID, repoName, file string, p *pendingMapping, classPrefix string) {
	method := p.method
	if method == "" {
		method = "ANY"
	}
	emitRoute(frag, repoNodeID, repoName, file, route{
		Method: method,
		Path:   joinPrefix(classPrefix, p.path),
		Line:   p.line,
	})
}

// joinPrefix concatenates a Spring class-level prefix and a method-level path
// per Spring's semantics: "/api" + "users" and "/api" + "/users" both resolve
// to "/api/users".
func joinPrefix(prefix, p string) string {
	if prefix == "" {
		return p
	}
	return strings.TrimRight(prefix, "/") + "/" + strings.TrimLeft(strings.TrimSpace(p), "/")
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

