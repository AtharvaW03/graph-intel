package deps

import (
	"regexp"
	"strings"
)

// parseGoMod handles Go modules. We look at `require` blocks AND inline
// `require` lines. Indirect deps (`// indirect` suffix) are extracted with
// scope="indirect" so consumers can filter them out.
func parseGoMod(_, contents string) ([]Dep, error) {
	var deps []Dep
	inBlock := false
	for _, line := range strings.Split(contents, "\n") {
		l := strings.TrimSpace(line)
		if l == "" || strings.HasPrefix(l, "//") {
			continue
		}
		switch {
		case strings.HasPrefix(l, "require (") || l == "require (":
			inBlock = true
			continue
		case inBlock && l == ")":
			inBlock = false
			continue
		case strings.HasPrefix(l, "require ") && !inBlock:
			l = strings.TrimPrefix(l, "require ")
			deps = appendGoModLine(deps, l)
		case inBlock:
			deps = appendGoModLine(deps, l)
		}
	}
	return deps, nil
}

func appendGoModLine(deps []Dep, line string) []Dep {
	// Detect the indirect marker BEFORE stripping trailing comments — the
	// marker lives inside the comment ("// indirect").
	scope := "runtime"
	if strings.Contains(line, "// indirect") {
		scope = "indirect"
	}
	if i := strings.Index(line, "//"); i >= 0 {
		line = strings.TrimSpace(line[:i])
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return deps
	}
	return append(deps, Dep{
		Name:      fields[0],
		Version:   fields[1],
		Ecosystem: "go",
		Scope:     scope,
	})
}

// parseRequirementsTxt handles pip's requirements.txt. We strip environment
// markers (`; python_version > "3.10"`), comments, and inline options.
// Editable installs (`-e .`) and `-r other.txt` references are ignored.
var reqLineRe = regexp.MustCompile(`^\s*([A-Za-z0-9_.\-\[\]]+)\s*([<>=!~]=?[^;\s]+|@\s*\S+)?`)

func parseRequirementsTxt(_, contents string) ([]Dep, error) {
	var deps []Dep
	for _, raw := range strings.Split(contents, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "-") {
			continue
		}
		if i := strings.Index(line, "#"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if i := strings.Index(line, ";"); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		m := reqLineRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		name := strings.SplitN(m[1], "[", 2)[0] // strip extras
		deps = append(deps, Dep{Name: name, Version: m[2], Ecosystem: "pypi", Scope: "runtime"})
	}
	return deps, nil
}

// parsePyProject handles pyproject.toml. We support both PEP 621
// (`[project]` section's dependencies array) and Poetry
// (`[tool.poetry.dependencies]`). We do not pull in a full TOML parser —
// the manifest's relevant sections are simple enough to parse line-by-line.
func parsePyProject(_, contents string) ([]Dep, error) {
	var deps []Dep
	section := ""
	for _, raw := range strings.Split(contents, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = line[1 : len(line)-1]
			continue
		}
		switch section {
		case "project":
			// dependencies = ["fastapi>=0.110", "pydantic>=2"]
			if strings.HasPrefix(line, "dependencies") && strings.Contains(line, "[") {
				// Single-line list — pull names with regex below.
			}
			deps = append(deps, parsePyProjectArrayLine(line, "runtime")...)
		case "project.optional-dependencies":
			deps = append(deps, parsePyProjectArrayLine(line, "optional")...)
		case "tool.poetry.dependencies":
			if d, ok := parsePoetryLine(line, "runtime"); ok {
				deps = append(deps, d)
			}
		case "tool.poetry.dev-dependencies", "tool.poetry.group.dev.dependencies":
			if d, ok := parsePoetryLine(line, "dev"); ok {
				deps = append(deps, d)
			}
		}
	}
	return deps, nil
}

var pep621Re = regexp.MustCompile(`"([A-Za-z0-9_.\-\[\]]+)\s*([<>=!~]=?[^"]*)?"`)

func parsePyProjectArrayLine(line, scope string) []Dep {
	var deps []Dep
	for _, m := range pep621Re.FindAllStringSubmatch(line, -1) {
		name := strings.SplitN(m[1], "[", 2)[0]
		if name == "python" {
			continue
		}
		deps = append(deps, Dep{Name: name, Version: m[2], Ecosystem: "pypi", Scope: scope})
	}
	return deps
}

var poetryLineRe = regexp.MustCompile(`^([A-Za-z0-9_.\-]+)\s*=\s*"?([^"]*)"?`)

func parsePoetryLine(line, scope string) (Dep, bool) {
	m := poetryLineRe.FindStringSubmatch(line)
	if m == nil {
		return Dep{}, false
	}
	if m[1] == "python" {
		return Dep{}, false
	}
	return Dep{Name: m[1], Version: m[2], Ecosystem: "pypi", Scope: scope}, true
}

// parsePipfile handles Pipenv's Pipfile (TOML-shape). Same per-line approach
// as pyproject.toml's poetry section.
func parsePipfile(_, contents string) ([]Dep, error) {
	var deps []Dep
	section := ""
	for _, raw := range strings.Split(contents, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = line[1 : len(line)-1]
			continue
		}
		scope := ""
		switch section {
		case "packages":
			scope = "runtime"
		case "dev-packages":
			scope = "dev"
		default:
			continue
		}
		if d, ok := parsePoetryLine(line, scope); ok {
			deps = append(deps, d)
		}
	}
	return deps, nil
}

// parseGemfile handles Ruby's Gemfile. The grammar is Ruby DSL, but the
// common cases (`gem 'name'`, `gem 'name', 'version'`,
// `gem 'name', '~> 1.0'`, `gem 'name', git: '...'`) are captured by one
// regex. `group :development do ... end` blocks are honored so dev-scope
// deps are tagged correctly.
var gemRe = regexp.MustCompile(`^\s*gem\s+["']([^"']+)["'](?:\s*,\s*["']([^"']+)["'])?`)

func parseGemfile(_, contents string) ([]Dep, error) {
	var deps []Dep
	scopeStack := []string{"runtime"}
	for _, raw := range strings.Split(contents, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "group ") {
			lower := strings.ToLower(line)
			switch {
			case strings.Contains(lower, ":development"), strings.Contains(lower, ":test"):
				scopeStack = append(scopeStack, "dev")
			default:
				scopeStack = append(scopeStack, "runtime")
			}
			continue
		}
		if line == "end" && len(scopeStack) > 1 {
			scopeStack = scopeStack[:len(scopeStack)-1]
			continue
		}
		m := gemRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		deps = append(deps, Dep{
			Name:      m[1],
			Version:   m[2],
			Ecosystem: "rubygems",
			Scope:     scopeStack[len(scopeStack)-1],
		})
	}
	return deps, nil
}

// parseBuildGradle handles Groovy build.gradle and Kotlin build.gradle.kts.
// We capture:
//   - implementation 'group:artifact:version'
//   - implementation "group:artifact:$version"
//   - implementation(platform("group:artifact:version"))
//   - api group: 'org', name: 'lib', version: '1.0'
//
// All scope keywords (implementation, api, compileOnly, runtimeOnly,
// testImplementation, ...) feed into Dep.Scope.
var gradleShortRe = regexp.MustCompile(`^\s*(implementation|api|compileOnly|runtimeOnly|testImplementation|testRuntimeOnly|annotationProcessor|kapt|ksp)\s*\(?\s*(?:platform\s*\()?["']([^"']+)["']`)
var gradleLongRe = regexp.MustCompile(`group\s*[:=]\s*["']([^"']+)["']\s*,\s*name\s*[:=]\s*["']([^"']+)["'](?:\s*,\s*version\s*[:=]\s*["']([^"']+)["'])?`)

func parseBuildGradle(_, contents string) ([]Dep, error) {
	var deps []Dep
	for _, raw := range strings.Split(contents, "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "//") || strings.HasPrefix(line, "#") {
			continue
		}
		if m := gradleShortRe.FindStringSubmatch(line); m != nil {
			scope := m[1]
			coord := m[2]
			parts := strings.Split(coord, ":")
			if len(parts) >= 2 {
				name := parts[0] + ":" + parts[1]
				ver := ""
				if len(parts) >= 3 {
					ver = parts[2]
				}
				deps = append(deps, Dep{Name: name, Version: ver, Ecosystem: "gradle", Scope: scope})
				continue
			}
		}
		if m := gradleLongRe.FindStringSubmatch(line); m != nil {
			name := m[1] + ":" + m[2]
			deps = append(deps, Dep{Name: name, Version: m[3], Ecosystem: "gradle", Scope: "runtime"})
		}
	}
	return deps, nil
}

// parseBuildSbt handles Scala's build.sbt. libraryDependencies entries look
// like: libraryDependencies += "org.scala-lang" %% "scala-library" % "2.13.10"
// We treat %% the same as %; the % between strings is the separator.
var sbtRe = regexp.MustCompile(`"([^"]+)"\s*%%?\s*"([^"]+)"\s*%\s*"([^"]+)"(?:\s*%\s*"([^"]+)")?`)

func parseBuildSbt(_, contents string) ([]Dep, error) {
	var deps []Dep
	for _, m := range sbtRe.FindAllStringSubmatch(contents, -1) {
		scope := "runtime"
		if len(m) >= 5 && m[4] != "" {
			scope = strings.ToLower(m[4])
		}
		deps = append(deps, Dep{
			Name:      m[1] + ":" + m[2],
			Version:   m[3],
			Ecosystem: "sbt",
			Scope:     scope,
		})
	}
	return deps, nil
}

// parsePackageSwift handles Swift Package Manager manifests. The relevant
// lines are `.package(url: "...", from: "1.0")` or `.package(url: "...",
// .upToNextMajor(from: "1.0"))`. The url is the canonical dependency name.
var swiftPMRe = regexp.MustCompile(`\.package\s*\(\s*(?:name\s*:\s*"[^"]*"\s*,\s*)?url\s*:\s*"([^"]+)"[^)]*"?([0-9][^"]*)?`)

func parsePackageSwift(_, contents string) ([]Dep, error) {
	var deps []Dep
	for _, m := range swiftPMRe.FindAllStringSubmatch(contents, -1) {
		deps = append(deps, Dep{
			Name:      m[1],
			Version:   m[2],
			Ecosystem: "swiftpm",
			Scope:     "runtime",
		})
	}
	return deps, nil
}

// parseCMakeLists handles C/C++ projects using CMake's find_package. The
// signature is find_package(<name> [version] [REQUIRED|OPTIONAL]).
var cmakeRe = regexp.MustCompile(`(?i)find_package\s*\(\s*([A-Za-z0-9_]+)(?:\s+([0-9][0-9A-Za-z.\-]*))?`)

func parseCMakeLists(_, contents string) ([]Dep, error) {
	var deps []Dep
	for _, m := range cmakeRe.FindAllStringSubmatch(contents, -1) {
		deps = append(deps, Dep{
			Name:      m[1],
			Version:   m[2],
			Ecosystem: "cmake",
			Scope:     "runtime",
		})
	}
	return deps, nil
}

// parseConanFile handles Conan's conanfile.txt and conanfile.py. The
// `[requires]` section of conanfile.txt is line-based ("name/version@user/channel").
// conanfile.py uses Python; we look for `self.requires("name/version")` patterns.
var conanLineRe = regexp.MustCompile(`^([A-Za-z0-9_.\-]+)/([^@\s]+)`)
var conanPyRe = regexp.MustCompile(`self\.requires\s*\(\s*["']([A-Za-z0-9_.\-]+)/([^@"']+)`)

func parseConanFile(_, contents string) ([]Dep, error) {
	var deps []Dep
	// Try conanfile.txt format first.
	section := ""
	for _, raw := range strings.Split(contents, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = line[1 : len(line)-1]
			continue
		}
		if section == "requires" || section == "tool_requires" || section == "build_requires" {
			if m := conanLineRe.FindStringSubmatch(line); m != nil {
				deps = append(deps, Dep{Name: m[1], Version: m[2], Ecosystem: "conan", Scope: "runtime"})
			}
		}
	}
	// Also pick up self.requires() calls in conanfile.py.
	for _, m := range conanPyRe.FindAllStringSubmatch(contents, -1) {
		deps = append(deps, Dep{Name: m[1], Version: m[2], Ecosystem: "conan", Scope: "runtime"})
	}
	return deps, nil
}
