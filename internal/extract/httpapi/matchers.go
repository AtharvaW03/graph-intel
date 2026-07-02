package httpapi

import (
	"regexp"
	"strings"
)

// Each matcher is responsible for ONE framework family. They are deliberately
// narrow regexes — the framework's own canonical method names. False positives
// (e.g. a variable named `app.get` in unrelated code) are rare and confidence
// is INFERRED so consumers know these are heuristic.

// --- Go: gin / echo / chi ---

var ginEchoChiRe = regexp.MustCompile(`(?P<recv>\b\w+)\.(GET|POST|PUT|DELETE|PATCH|OPTIONS|HEAD|Any)\s*\(\s*"([^"]+)"(?:\s*,\s*([A-Za-z0-9_.]+))?`)

// matchGin captures gin / echo / chi (upper-case) and any other framework
// using the `<recv>.METHOD("path", ...)` shape with HTTP-verb-cased method
// names. This single matcher covers the bulk of Go web frameworks.
func matchGin(line string, lineNum int) []route { return runRegex(ginEchoChiRe, line, lineNum) }

// matchChi handles chi's lowercase variants (`r.Get`, `r.Post`) which
// matchGin does NOT cover (it's case-sensitive on the method). Run alongside
// matchGin so a file using both call styles is fully scanned.
//
// `.Get("...")` is an extremely common shape in non-router Go code —
// viper.Get("server.port"), cache.Get("key"), headers.Get("Accept") — so
// only paths starting with "/" are accepted. chi route patterns always
// start with "/"; config keys and cache keys essentially never do.
func matchChi(line string, lineNum int) []route {
	rs := runRegex(chiLowerRe, line, lineNum)
	out := rs[:0]
	for _, r := range rs {
		if strings.HasPrefix(r.Path, "/") {
			out = append(out, r)
		}
	}
	return out
}

var chiLowerRe = regexp.MustCompile(`\b\w+\.(Get|Post|Put|Delete|Patch|Options|Head|Connect|Trace)\s*\(\s*"([^"]+)"(?:\s*,\s*([A-Za-z0-9_.]+))?`)

// --- Go: gorilla/mux + net/http ---

var gorillaMuxRe = regexp.MustCompile(`\.HandleFunc\s*\(\s*"([^"]+)"\s*,\s*([A-Za-z0-9_.]+)\)\s*\.Methods\s*\(\s*"([A-Z]+)"`)
var netHTTPRe = regexp.MustCompile(`http\.HandleFunc\s*\(\s*"([^"]+)"\s*,\s*([A-Za-z0-9_.]+)`)

func matchGorillaMux(line string, lineNum int) []route {
	m := gorillaMuxRe.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	return []route{{Method: m[3], Path: m[1], Handler: m[2], Line: lineNum}}
}

func matchNetHTTP(line string, lineNum int) []route {
	m := netHTTPRe.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	// net/http's HandleFunc doesn't carry a method. Mark as ANY.
	return []route{{Method: "ANY", Path: m[1], Handler: m[2], Line: lineNum}}
}

// --- Python: Flask / FastAPI / blueprint / APIRouter ---

var flaskRoute = regexp.MustCompile(`@\w+\.(?:route|get|post|put|delete|patch|head|options)\s*\(\s*["']([^"']+)["'](?:\s*,\s*methods\s*=\s*\[([^\]]+)\])?`)
var fastAPIInferMethod = regexp.MustCompile(`@\w+\.(get|post|put|delete|patch|head|options)\s*\(`)

func matchFlaskFastAPI(line string, lineNum int) []route {
	m := flaskRoute.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	method := "GET"
	if m[2] != "" {
		// Flask methods list: ['GET','POST'] — take the first as the canonical.
		method = strings.ToUpper(strings.Trim(strings.SplitN(m[2], ",", 2)[0], "' \""))
	} else if mm := fastAPIInferMethod.FindStringSubmatch(line); mm != nil {
		method = strings.ToUpper(mm[1])
	}
	return []route{{Method: method, Path: m[1], Line: lineNum}}
}

// --- Python: Django urls.py ---

var djangoRoute = regexp.MustCompile(`(?:^|\s)(?:re_)?path\s*\(\s*r?["']([^"']+)["']\s*,\s*([A-Za-z0-9_.]+)`)

func matchDjango(line string, lineNum int) []route {
	m := djangoRoute.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	return []route{{Method: "ANY", Path: m[1], Handler: m[2], Line: lineNum}}
}

// --- JS/TS: Express ---

var expressRoute = regexp.MustCompile(`\b(?:app|router|api)\.(get|post|put|delete|patch|head|options|all|use)\s*\(\s*["'\x60]([^"'\x60]+)["'\x60]`)

func matchExpress(line string, lineNum int) []route {
	m := expressRoute.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	method := strings.ToUpper(m[1])
	if method == "ALL" || method == "USE" {
		method = "ANY"
	}
	return []route{{Method: method, Path: m[2], Line: lineNum}}
}

// --- Java/Kotlin: Spring annotations ---
//
// Only the method-specific @GetMapping/@PostMapping/... shortcuts are matched
// here. Bare @RequestMapping is ambiguous — Spring uses it both as a
// class-level path prefix and as a method-level route — so the extractor's
// scan loop handles it with declaration lookahead (see resolveRequestMapping
// in extractor.go) instead of a stateless per-line matcher.

var springMethodMapping = regexp.MustCompile(`@(GetMapping|PostMapping|PutMapping|DeleteMapping|PatchMapping)\s*\(\s*(?:value\s*=\s*)?"([^"]+)"`)

func matchSpring(line string, lineNum int) []route {
	m := springMethodMapping.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	method := strings.ToUpper(strings.TrimSuffix(m[1], "Mapping"))
	return []route{{Method: method, Path: m[2], Line: lineNum}}
}

// --- ASP.NET attributes ---

var aspNetAttribute = regexp.MustCompile(`\[(?:Http(Get|Post|Put|Delete|Patch|Head|Options))(?:\s*\(\s*"([^"]+)")?\s*\]`)
var aspNetRoute = regexp.MustCompile(`\[Route\s*\(\s*"([^"]+)"`)

func matchAspNet(line string, lineNum int) []route {
	if m := aspNetAttribute.FindStringSubmatch(line); m != nil {
		method := strings.ToUpper(m[1])
		path := m[2]
		if path == "" {
			path = "/"
		}
		return []route{{Method: method, Path: path, Line: lineNum}}
	}
	if m := aspNetRoute.FindStringSubmatch(line); m != nil {
		return []route{{Method: "ANY", Path: m[1], Line: lineNum}}
	}
	return nil
}

// --- Ruby on Rails: config/routes.rb get/post/etc. shortcuts ---

var railsRoute = regexp.MustCompile(`^\s*(get|post|put|delete|patch|head|options|match)\s+["']([^"']+)["']`)

func matchRails(line string, lineNum int) []route {
	m := railsRoute.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	method := strings.ToUpper(m[1])
	if method == "MATCH" {
		method = "ANY"
	}
	return []route{{Method: method, Path: m[2], Line: lineNum}}
}

// --- PHP: Laravel route facade ---

var laravelRoute = regexp.MustCompile(`Route::(get|post|put|delete|patch|head|options|any)\s*\(\s*["']([^"']+)["']`)

func matchLaravel(line string, lineNum int) []route {
	m := laravelRoute.FindStringSubmatch(line)
	if m == nil {
		return nil
	}
	method := strings.ToUpper(m[1])
	if method == "ANY" {
		method = "ANY"
	}
	return []route{{Method: method, Path: m[2], Line: lineNum}}
}

// runRegex drives a regex that captures (recv, METHOD, "path"[, handler]).
func runRegex(re *regexp.Regexp, line string, lineNum int) []route {
	var out []route
	for _, m := range re.FindAllStringSubmatch(line, -1) {
		// the indices depend on whether the regex has named groups; both of
		// our patterns put the method at index 1 (or 2 for ginEchoChiRe).
		var method, path, handler string
		switch len(m) {
		case 4:
			method, path, handler = m[1], m[2], m[3]
		case 5:
			method, path, handler = m[2], m[3], m[4]
		default:
			continue
		}
		out = append(out, route{Method: strings.ToUpper(method), Path: path, Handler: handler, Line: lineNum})
	}
	return out
}
