package deps

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// readFileLimited reads up to maxBytes from path. Returns an error if the
// file exceeds the limit — manifests are tiny in practice; a multi-megabyte
// manifest is almost certainly a generated lockfile that we don't want to
// load into memory.
func readFileLimited(path string, maxBytes int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	buf, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return "", err
	}
	if int64(len(buf)) > maxBytes {
		return "", fmt.Errorf("manifest exceeds %d bytes", maxBytes)
	}
	return string(buf), nil
}

// parsePackageJSON handles npm / Node.js / TypeScript projects (the same
// manifest covers JS, TS, JSX, TSX). Both "dependencies" and "devDependencies"
// are extracted; the scope is preserved as Dep.Scope so downstream queries can
// filter out dev-only packages if desired.
func parsePackageJSON(_, contents string) ([]Dep, error) {
	var pkg struct {
		Dependencies         map[string]string `json:"dependencies"`
		DevDependencies      map[string]string `json:"devDependencies"`
		PeerDependencies     map[string]string `json:"peerDependencies"`
		OptionalDependencies map[string]string `json:"optionalDependencies"`
	}
	if err := json.Unmarshal([]byte(contents), &pkg); err != nil {
		return nil, fmt.Errorf("package.json: %w", err)
	}
	out := make([]Dep, 0, len(pkg.Dependencies)+len(pkg.DevDependencies))
	for name, ver := range pkg.Dependencies {
		out = append(out, Dep{Name: name, Version: ver, Ecosystem: "npm", Scope: "runtime"})
	}
	for name, ver := range pkg.DevDependencies {
		out = append(out, Dep{Name: name, Version: ver, Ecosystem: "npm", Scope: "dev"})
	}
	for name, ver := range pkg.PeerDependencies {
		out = append(out, Dep{Name: name, Version: ver, Ecosystem: "npm", Scope: "peer"})
	}
	for name, ver := range pkg.OptionalDependencies {
		out = append(out, Dep{Name: name, Version: ver, Ecosystem: "npm", Scope: "optional"})
	}
	return out, nil
}

// parseComposerJSON handles PHP / Composer. Same shape as package.json: a
// top-level "require" map and a "require-dev" map.
func parseComposerJSON(_, contents string) ([]Dep, error) {
	var pkg struct {
		Require    map[string]string `json:"require"`
		RequireDev map[string]string `json:"require-dev"`
	}
	if err := json.Unmarshal([]byte(contents), &pkg); err != nil {
		return nil, fmt.Errorf("composer.json: %w", err)
	}
	out := make([]Dep, 0, len(pkg.Require)+len(pkg.RequireDev))
	for name, ver := range pkg.Require {
		if name == "php" || strings.HasPrefix(name, "ext-") || strings.HasPrefix(name, "lib-") {
			continue
		}
		out = append(out, Dep{Name: name, Version: ver, Ecosystem: "composer", Scope: "runtime"})
	}
	for name, ver := range pkg.RequireDev {
		out = append(out, Dep{Name: name, Version: ver, Ecosystem: "composer", Scope: "dev"})
	}
	return out, nil
}

// parseVcpkgJSON handles C/C++ projects using vcpkg's manifest mode. Deps can
// be plain strings or {name, features, version>=} objects; we tolerate both.
func parseVcpkgJSON(_, contents string) ([]Dep, error) {
	var pkg struct {
		Dependencies []json.RawMessage `json:"dependencies"`
	}
	if err := json.Unmarshal([]byte(contents), &pkg); err != nil {
		return nil, fmt.Errorf("vcpkg.json: %w", err)
	}
	out := make([]Dep, 0, len(pkg.Dependencies))
	for _, raw := range pkg.Dependencies {
		var asString string
		if err := json.Unmarshal(raw, &asString); err == nil && asString != "" {
			out = append(out, Dep{Name: asString, Ecosystem: "vcpkg", Scope: "runtime"})
			continue
		}
		var asObj struct {
			Name    string `json:"name"`
			Version string `json:"version>="`
		}
		if err := json.Unmarshal(raw, &asObj); err == nil && asObj.Name != "" {
			out = append(out, Dep{Name: asObj.Name, Version: asObj.Version, Ecosystem: "vcpkg", Scope: "runtime"})
		}
	}
	return out, nil
}
