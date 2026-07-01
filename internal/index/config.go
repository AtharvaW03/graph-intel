package index

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

// validRepoName restricts Repository.Name to characters that are safe to use
// as filesystem path segments and Neo4j string properties: alphanumerics
// plus '.', '_', and '-'. The first character is restricted further (no
// dot/dash leading) to prevent hidden-file or option-flag confusion.
// Names like "../bad" or "/etc/passwd" or "foo bar" are rejected.
var validRepoName = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]*$`)

// Config is the indexer's on-disk configuration. It declares the repository
// manifest and tunables for the git, graphify, and orchestrator subsystems.
type Config struct {
	Repositories []Repository   `yaml:"repositories"`
	Git          GitConfig      `yaml:"git"`
	Graphify     GraphifyConfig `yaml:"graphify"`
	Extractors   ExtractorsConfig `yaml:"extractors"`
	Org          OrgConfig      `yaml:"org"`
}

// ExtractorsConfig toggles each platform extractor on/off. All default to
// enabled — operators can disable any one by setting it to false.
type ExtractorsConfig struct {
	Deps     *bool `yaml:"deps"`
	HTTPAPI  *bool `yaml:"http_api"`
	Kafka    *bool `yaml:"kafka"`
	MSSQL    *bool `yaml:"mssql"`
	Glue     *bool `yaml:"glue"`
	// MaxParallel caps concurrent extractors per repo. Zero or negative means
	// run all configured extractors at once.
	MaxParallel int `yaml:"max_parallel"`
}

// OrgConfig captures organization-wide conventions used by extractors. The
// most important is Prefixes — when a dependency's name starts with one of
// these, the deps extractor emits a cross-repository edge so questions like
// "which repos depend on auth-service?" become one-hop traversals.
type OrgConfig struct {
	Prefixes []string `yaml:"prefixes"`
}

// boolDefault dereferences an opt-out *bool, returning the default when the
// pointer is nil (operator left the YAML field empty).
func boolDefault(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}

// DepsEnabled reports whether the deps extractor should run.
func (c ExtractorsConfig) DepsEnabled() bool    { return boolDefault(c.Deps, true) }
func (c ExtractorsConfig) HTTPAPIEnabled() bool { return boolDefault(c.HTTPAPI, true) }
func (c ExtractorsConfig) KafkaEnabled() bool   { return boolDefault(c.Kafka, true) }
func (c ExtractorsConfig) MSSQLEnabled() bool   { return boolDefault(c.MSSQL, true) }
func (c ExtractorsConfig) GlueEnabled() bool    { return boolDefault(c.Glue, true) }

// GitConfig tunes the GitSyncer.
type GitConfig struct {
	Timeout time.Duration `yaml:"timeout"`
}

// GraphifyConfig tunes the ExecGraphifier. Args supports the {repo_path}
// placeholder, substituted at run-time. There is no {out_dir} placeholder:
// the default invocation is `graphify update <repo_path>` which writes
// inside the repo (no --out flag). OutputFile is interpreted relative to
// {repo_path}.
type GraphifyConfig struct {
	Command    string        `yaml:"command"`
	Args       []string      `yaml:"args"`
	OutputFile string        `yaml:"output_file"`
	Timeout    time.Duration `yaml:"timeout"`
}

// DefaultConfig returns sane defaults that match the locally installed
// graphify CLI and the existing project conventions. ApplyDefaults uses
// these to fill any unset fields in a loaded Config.
func DefaultConfig() Config {
	return Config{
		Git: GitConfig{
			Timeout: 10 * time.Minute,
		},
		Graphify: GraphifyConfig{
			Command:    "graphify",
			Args:       []string{"update", "{repo_path}"},
			OutputFile: "graphify-out/graph.json",
			Timeout:    20 * time.Minute,
		},
	}
}

// LoadConfig reads a YAML config file, fills in defaults, and validates it.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.ApplyDefaults()
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return &c, nil
}

// ApplyDefaults fills any zero-valued field with the value from DefaultConfig.
// Existing user values are preserved.
func (c *Config) ApplyDefaults() {
	d := DefaultConfig()
	if c.Git.Timeout == 0 {
		c.Git.Timeout = d.Git.Timeout
	}
	if c.Graphify.Command == "" {
		c.Graphify.Command = d.Graphify.Command
	}
	if len(c.Graphify.Args) == 0 {
		c.Graphify.Args = d.Graphify.Args
	}
	if c.Graphify.OutputFile == "" {
		c.Graphify.OutputFile = d.Graphify.OutputFile
	}
	if c.Graphify.Timeout == 0 {
		c.Graphify.Timeout = d.Graphify.Timeout
	}
}

// Validate enforces invariants that would otherwise produce confusing failures
// deep in the pipeline (e.g. a repo with no URL would fail at clone time).
func (c *Config) Validate() error {
	if len(c.Repositories) == 0 {
		return fmt.Errorf("no repositories configured")
	}
	seen := map[string]bool{}
	for i, r := range c.Repositories {
		if r.Name == "" {
			return fmt.Errorf("repositories[%d]: missing name", i)
		}
		if !validRepoName.MatchString(r.Name) {
			return fmt.Errorf("repositories[%d]: invalid name %q (must match %s; no path separators, spaces, or leading dot/dash)", i, r.Name, validRepoName.String())
		}
		if r.URL == "" {
			return fmt.Errorf("repositories[%d] (%s): missing url", i, r.Name)
		}
		if r.Branch == "" {
			return fmt.Errorf("repositories[%d] (%s): missing branch", i, r.Name)
		}
		if seen[r.Name] {
			return fmt.Errorf("repositories: duplicate name %q", r.Name)
		}
		seen[r.Name] = true
	}
	return nil
}

// ConfigJobSource is the default JobSource: it serves the static repository
// list from a Config struct. Replace with a queue- or webhook-backed source
// to evolve toward event-driven indexing.
type ConfigJobSource struct {
	cfg *Config
}

func NewConfigJobSource(cfg *Config) *ConfigJobSource {
	return &ConfigJobSource{cfg: cfg}
}

func (s *ConfigJobSource) Repositories(_ context.Context) ([]Repository, error) {
	out := make([]Repository, len(s.cfg.Repositories))
	copy(out, s.cfg.Repositories)
	return out, nil
}
