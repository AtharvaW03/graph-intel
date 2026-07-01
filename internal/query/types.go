package query

// SearchResult is one hit from a partial-match search across name/norm_name.
type SearchResult struct {
	NodeKey    string   `json:"node_key"`
	GraphifyID string   `json:"graphify_id"`
	Name       string   `json:"name"`
	Labels     []string `json:"labels"`
	Repo       string   `json:"repo"`
	Path       string   `json:"path"`
	Line       string   `json:"line"`
}

// SymbolResult is one occurrence of a symbol matched by exact name.
type SymbolResult struct {
	Name      string   `json:"name"`
	Repo      string   `json:"repo"`
	Path      string   `json:"path"`
	Line      string   `json:"line"`
	Labels    []string `json:"labels"`
	Community int      `json:"community"`
}

// CallEdge is one CALLS relationship, shared by FindCallers and FindCallees.
// In FindCallers, Labels are the caller's labels; in FindCallees, the callee's.
type CallEdge struct {
	Caller     string   `json:"caller"`
	CallerRepo string   `json:"caller_repo"`
	CallerPath string   `json:"caller_path"`
	CallerLine string   `json:"caller_line"`
	Callee     string   `json:"callee"`
	CalleeRepo string   `json:"callee_repo"`
	CalleePath string   `json:"callee_path"`
	Labels     []string `json:"labels"`
}

// ImpactNode is one downstream node reachable from a symbol within depth N.
type ImpactNode struct {
	Name     string   `json:"name"`
	Repo     string   `json:"repo"`
	Path     string   `json:"path"`
	Line     string   `json:"line"`
	Labels   []string `json:"labels"`
	Distance int      `json:"distance"`
}

// PathNode is one node along a shortest path; Relationship is the edge type
// leading INTO this node from the previous one (empty on the first node).
type PathNode struct {
	Name         string   `json:"name"`
	Repo         string   `json:"repo"`
	Path         string   `json:"path"`
	Labels       []string `json:"labels"`
	Relationship string   `json:"relationship,omitempty"`
}

// DependencyEdge is one repo→package or repo→repo edge from the deps extractor.
type DependencyEdge struct {
	Repo      string   `json:"repo"`
	Name      string   `json:"name"`       // package or repo name
	Labels    []string `json:"labels"`     // includes Package and (when cross-repo) Repository
	Ecosystem string   `json:"ecosystem"`  // go | npm | pypi | maven | …
	Version   string   `json:"version,omitempty"`
	Scope     string   `json:"scope,omitempty"`
	Cross     bool     `json:"cross_repo"` // true if this is a DEPENDS_ON_REPO edge
}

// HTTPRoute is one row from the routes inventory.
type HTTPRoute struct {
	Repo     string   `json:"repo"`
	Method   string   `json:"method"`
	Path     string   `json:"path"`
	Handler  string   `json:"handler,omitempty"`
	Labels   []string `json:"labels"`
	File     string   `json:"file"`
	Line     string   `json:"line"`
}

// KafkaTopicInfo describes one topic plus its producer/consumer repos.
type KafkaTopicInfo struct {
	Topic     string   `json:"topic"`
	Producers []string `json:"producers"`
	Consumers []string `json:"consumers"`
}

// SQLObjectInfo describes one SQL Server object plus the tables it reads,
// writes, and depends on.
type SQLObjectInfo struct {
	Name        string   `json:"name"`
	Schema      string   `json:"schema"`
	Kind        string   `json:"kind"` // sql_table | sql_view | sql_procedure | sql_trigger | sql_function | sql_schema
	Labels      []string `json:"labels"`
	File        string   `json:"file,omitempty"`
	Line        string   `json:"line,omitempty"`
	Reads       []string `json:"reads,omitempty"`
	Writes      []string `json:"writes,omitempty"`
	DependsOn   []string `json:"depends_on,omitempty"`
	TriggersOn  string   `json:"triggers_on,omitempty"`
}

// GlueJobInfo describes one AWS Glue job plus its source/destination tables.
type GlueJobInfo struct {
	Name     string   `json:"name"`
	Repo     string   `json:"repo"`
	Labels   []string `json:"labels"`
	Script   string   `json:"script,omitempty"`
	Schedule string   `json:"schedule,omitempty"`
	Sources  []string `json:"sources,omitempty"`
	Targets  []string `json:"targets,omitempty"`
	File     string   `json:"file,omitempty"`
}

// RepositoryOverview is the aggregated onboarding snapshot for one repository.
// It is built entirely from the indexed graph in Neo4j (no source-file access)
// by aggregating a handful of focused queries plus the existing routes and
// dependencies query logic. The response is structured data, not prose — the
// caller is expected to turn it into natural-language onboarding.
type RepositoryOverview struct {
	Repository   RepoMetadata       `json:"repository"`
	Architecture ArchitectureInfo   `json:"architecture"`
	EntryPoints  []EntryPoint       `json:"entry_points"`
	Modules      []ModuleInfo       `json:"modules"`
	HTTPAPIs     HTTPAPISummary     `json:"http_apis"`
	Kafka        KafkaSummary       `json:"kafka"`
	SQL          SQLSummary         `json:"sql"`
	Dependencies DependencySummary  `json:"dependencies"`
	Components   []ComponentInfo    `json:"important_components"`
	ReadingOrder []ReadingStep      `json:"suggested_reading_order"`
}

// LabeledCount is a generic name→count pair used for label, language, method,
// and ecosystem distributions.
type LabeledCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// RepoMetadata is section 1: repository identity plus graph size and the
// label/language distributions that characterize it.
type RepoMetadata struct {
	Name              string         `json:"name"`
	NodeCount         int            `json:"node_count"`
	RelationshipCount int            `json:"relationship_count"`
	Languages         []LabeledCount `json:"languages"`
	NodeKinds         []LabeledCount `json:"node_kinds"`
}

// ArchitectureInfo is section 2: a synthesized high-level summary plus the
// major Graphify communities ordered by size (largest components first).
type ArchitectureInfo struct {
	Summary     string             `json:"summary"`
	Communities []CommunitySummary `json:"communities"`
}

// CommunitySummary describes one Graphify community/cluster. Label and
// DominantDir are synthesized from members because the importer stores an
// empty community_name.
type CommunitySummary struct {
	ID            int      `json:"id"`
	Size          int      `json:"size"`
	Label         string   `json:"label"`
	DominantDir   string   `json:"dominant_dir,omitempty"`
	SampleMembers []string `json:"sample_members"`
}

// EntryPoint is section 3: an executable main, server, or startup/bootstrap
// function. Kind is one of executable_main | server | bootstrap.
type EntryPoint struct {
	Name   string   `json:"name"`
	Kind   string   `json:"kind"`
	Path   string   `json:"path"`
	Line   string   `json:"line"`
	Labels []string `json:"labels"`
}

// ModuleInfo is section 4: one package/directory with its approximate size.
type ModuleInfo struct {
	Package   string `json:"package"`
	NodeCount int    `json:"node_count"`
	Functions int    `json:"functions"`
}

// HTTPAPISummary is section 5: route count plus method distribution and
// endpoints grouped by path prefix.
type HTTPAPISummary struct {
	RouteCount int            `json:"route_count"`
	Methods    []LabeledCount `json:"methods"`
	Groups     []RouteGroup   `json:"groups"`
}

// RouteGroup buckets routes sharing a leading path prefix.
type RouteGroup struct {
	Prefix  string   `json:"prefix"`
	Count   int      `json:"count"`
	Methods []string `json:"methods"`
}

// KafkaSummary is section 6: topics plus the producers and consumers wired to
// them within this repository.
type KafkaSummary struct {
	Topics    []string         `json:"topics"`
	Producers []string         `json:"producers"`
	Consumers []string         `json:"consumers"`
	ByTopic   []KafkaTopicInfo `json:"by_topic,omitempty"`
}

// SQLSummary is section 7: SQL objects grouped by kind.
type SQLSummary struct {
	Schemas    []string `json:"schemas"`
	Tables     []string `json:"tables"`
	Views      []string `json:"views"`
	Procedures []string `json:"procedures"`
	Functions  []string `json:"functions"`
	Triggers   []string `json:"triggers"`
}

// DependencySummary is section 8: internal (cross-repo) targets, external
// packages, and the ecosystems they cluster into.
type DependencySummary struct {
	InternalRepos []string         `json:"internal_repos"`
	External      []DependencyEdge `json:"external"`
	TopEcosystems []LabeledCount   `json:"top_ecosystems"`
}

// ComponentInfo is section 9: one highest-degree hub node (a god node), the
// core abstraction other code clusters around.
type ComponentInfo struct {
	Name      string   `json:"name"`
	Path      string   `json:"path"`
	Degree    int      `json:"degree"`
	Community int      `json:"community"`
	Labels    []string `json:"labels"`
}

// ReadingStep is one bucket of section 10's suggested reading order. Category
// is one of entry_points | core_packages | services | infrastructure |
// utilities. Items are graph-derived names to read in that phase.
type ReadingStep struct {
	Category string   `json:"category"`
	Why      string   `json:"why"`
	Items    []string `json:"items"`
}
