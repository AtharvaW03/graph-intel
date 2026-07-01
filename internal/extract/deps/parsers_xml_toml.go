package deps

import (
	"encoding/xml"
	"fmt"
	"regexp"
	"strings"
)

// parsePomXML handles Maven's pom.xml. We use the stdlib's encoding/xml with
// a permissive schema — Maven adds a lot of fields we don't care about.
func parsePomXML(_, contents string) ([]Dep, error) {
	type dep struct {
		GroupID    string `xml:"groupId"`
		ArtifactID string `xml:"artifactId"`
		Version    string `xml:"version"`
		Scope      string `xml:"scope"`
	}
	type project struct {
		Dependencies struct {
			Dependency []dep `xml:"dependency"`
		} `xml:"dependencies"`
		DependencyManagement struct {
			Dependencies struct {
				Dependency []dep `xml:"dependency"`
			} `xml:"dependencies"`
		} `xml:"dependencyManagement"`
	}
	var p project
	if err := xml.Unmarshal([]byte(contents), &p); err != nil {
		return nil, fmt.Errorf("pom.xml: %w", err)
	}
	all := append([]dep{}, p.Dependencies.Dependency...)
	all = append(all, p.DependencyManagement.Dependencies.Dependency...)
	out := make([]Dep, 0, len(all))
	for _, d := range all {
		if d.GroupID == "" || d.ArtifactID == "" {
			continue
		}
		scope := d.Scope
		if scope == "" {
			scope = "runtime"
		}
		out = append(out, Dep{
			Name:      d.GroupID + ":" + d.ArtifactID,
			Version:   d.Version,
			Ecosystem: "maven",
			Scope:     scope,
		})
	}
	return out, nil
}

// parseDotNetProj handles .csproj / .fsproj / .vbproj files. These are XML
// with <PackageReference Include="..." Version="..."/> entries — sometimes
// the version is a child element, so we cover both shapes.
func parseDotNetProj(_, contents string) ([]Dep, error) {
	type packageRef struct {
		Include string `xml:"Include,attr"`
		Version string `xml:"Version,attr"`
		VerNode string `xml:"Version"`
	}
	type itemGroup struct {
		PackageReferences []packageRef `xml:"PackageReference"`
	}
	type project struct {
		ItemGroups []itemGroup `xml:"ItemGroup"`
	}
	var p project
	if err := xml.Unmarshal([]byte(contents), &p); err != nil {
		return nil, fmt.Errorf(".csproj: %w", err)
	}
	var out []Dep
	for _, g := range p.ItemGroups {
		for _, pr := range g.PackageReferences {
			if pr.Include == "" {
				continue
			}
			ver := pr.Version
			if ver == "" {
				ver = pr.VerNode
			}
			out = append(out, Dep{
				Name:      pr.Include,
				Version:   ver,
				Ecosystem: "nuget",
				Scope:     "runtime",
			})
		}
	}
	return out, nil
}

// parseCargoToml handles Rust's Cargo.toml. We extract the [dependencies],
// [dev-dependencies] and [build-dependencies] sections. Both
// `name = "1.2.3"` and `name = { version = "1.2.3", features = [...] }`
// shapes are supported.
var cargoSimpleRe = regexp.MustCompile(`^([A-Za-z0-9_\-]+)\s*=\s*"([^"]*)"`)
var cargoTableRe = regexp.MustCompile(`^([A-Za-z0-9_\-]+)\s*=\s*\{([^}]*)\}`)
var cargoVersionRe = regexp.MustCompile(`version\s*=\s*"([^"]*)"`)

func parseCargoToml(_, contents string) ([]Dep, error) {
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
		case "dependencies":
			scope = "runtime"
		case "dev-dependencies":
			scope = "dev"
		case "build-dependencies":
			scope = "build"
		default:
			continue
		}
		if m := cargoTableRe.FindStringSubmatch(line); m != nil {
			ver := ""
			if vm := cargoVersionRe.FindStringSubmatch(m[2]); vm != nil {
				ver = vm[1]
			}
			deps = append(deps, Dep{Name: m[1], Version: ver, Ecosystem: "cargo", Scope: scope})
			continue
		}
		if m := cargoSimpleRe.FindStringSubmatch(line); m != nil {
			deps = append(deps, Dep{Name: m[1], Version: m[2], Ecosystem: "cargo", Scope: scope})
		}
	}
	return deps, nil
}
