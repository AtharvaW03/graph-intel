package graphify

import (
	"crypto/sha1"
	"encoding/hex"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

var fileExtRe = regexp.MustCompile(`\.(go|kt|sh|py|md|ya?ml|json|toml)$`)

// typeToLabel resolves the Graphify-format `type` field of a node to a Neo4j
// label. This is the single extension point external extractors use to map
// their entities to the Neo4j data model. Any Graphify-emitted fragment can
// set node.type to one of these keys and the importer will pick the right
// label automatically.
//
// `package` was already used by Graphify's own manifest extractor; the other
// keys are introduced by the platform's extractor plugins.
var typeToLabel = map[string]string{
	"package":        "Package",
	"dependency":     "Package",
	"http_route":     "HttpRoute",
	"kafka_topic":    "KafkaTopic",
	"kafka_producer": "KafkaProducer",
	"kafka_consumer": "KafkaConsumer",
	"sql_schema":     "SqlSchema",
	"sql_table":      "SqlTable",
	"sql_view":       "SqlView",
	"sql_procedure":  "SqlProcedure",
	"sql_trigger":    "SqlTrigger",
	"sql_function":   "SqlFunction",
	"glue_job":       "GlueJob",
	"glue_schedule":  "GlueSchedule",
}

// InferLabel returns the Neo4j label for a node using first-match-wins rules.
// The explicit `type` field wins over all heuristic rules so fragments emitted
// by extractor plugins never get misclassified by the filename heuristics.
func InferLabel(n Node) string {
	if l, ok := typeToLabel[n.Type]; ok {
		return l
	}

	switch n.MetaString("kind") {
	case "file", "bash_entrypoint":
		return "File"
	case "bash_function":
		return "Function"
	}

	sf := strings.ToLower(n.SourceFile)
	if strings.HasSuffix(sf, ".md") || strings.HasSuffix(sf, ".mdx") || strings.HasSuffix(sf, ".rst") {
		return "DocSection"
	}

	if fileExtRe.MatchString(n.Label) {
		return "File"
	}

	if strings.HasSuffix(n.Label, "()") {
		return "Function"
	}

	if r, _ := utf8.DecodeRuneInString(n.Label); r != utf8.RuneError &&
		unicode.IsUpper(r) &&
		!strings.Contains(n.Label, "(") &&
		!strings.Contains(n.Label, " ") {
		return "Class"
	}

	return "Symbol"
}

// relationMap maps Graphify-format relation strings (the verbs extractors
// emit) to Neo4j relationship types (UPPER_SNAKE_CASE). New relations added
// here must also pass through ImportLinks' allowlist via this map — there is
// no separate allowlist to extend.
var relationMap = map[string]string{
	// graphify built-in code relations
	"calls":      "CALLS",
	"contains":   "CONTAINS",
	"references": "REFERENCES",
	"method":     "HAS_METHOD",
	"embeds":     "EMBEDS",
	"defines":    "DECLARES",

	// repository dependency extractor
	"depends_on":      "DEPENDS_ON",
	"depends_on_repo": "DEPENDS_ON_REPO",

	// http api extractor
	"exposes_route": "EXPOSES_ROUTE",
	"handled_by":    "HANDLED_BY",

	// kafka extractor
	"produces": "PRODUCES",
	"consumes": "CONSUMES",

	// sql server extractor
	"reads_table":  "READS_TABLE",
	"writes_table": "WRITES_TABLE",
	"triggers_on":  "TRIGGERS_ON",
	"depends_on_object": "DEPENDS_ON_OBJECT",
	"in_schema":    "IN_SCHEMA",

	// aws glue extractor
	"reads_source":      "READS_SOURCE",
	"writes_destination": "WRITES_DESTINATION",
	"scheduled":         "SCHEDULED",
}

// MapRelation maps a Graphify relation string to a Neo4j relationship type.
// Returns ("", false) for unknown relations — callers should skip those edges.
func MapRelation(relation string) (string, bool) {
	r, ok := relationMap[relation]
	return r, ok
}

// sharedIDPrefixes are the platform node-ID prefixes that denote org-global
// entities — one Neo4j node shared by every repo that references it. A Kafka
// topic, a package, a SQL object, or a repository hub is the same real-world
// thing no matter which repo's scan discovered it; keeping one node per
// entity is what makes cross-repo questions ("who consumes trade_executed?",
// "which repos depend on auth-service?") one-hop traversals.
//
// Shared nodes carry shared=true and NO repo property in Neo4j, and are
// excluded from the repo-scoped stale sweep (see neo4j.SweepStale).
var sharedIDPrefixes = []string{"topic::", "pkg::", "sql::", "repo::"}

// IsShared reports whether a platform-emitted node is an org-global entity.
func IsShared(n Node) bool {
	if n.Origin != "platform" {
		return false
	}
	for _, p := range sharedIDPrefixes {
		if strings.HasPrefix(n.ID, p) {
			return true
		}
	}
	return false
}

// StableKey returns the Neo4j node_key for a node.
//
// Platform-extractor nodes (_origin == "platform") use their extractor-
// assigned ID directly. Those IDs are globally unique by construction:
// repo-scoped entities embed the repo name (route::<repo>::...,
// glue::job::<repo>::...), while org-global entities deliberately share one
// ID across repos (topic::<name>, pkg::<eco>::<name>, sql::...,
// repo::<name>) so the same topic or package discovered in two repos merges
// into ONE node. Hashing these with the repo — as the AST branch below does —
// would split every shared entity into per-repo copies, leaving cross-repo
// edges pointing at phantom duplicates and breaking shortest-path /
// blast-radius traversal across repository boundaries.
//
// Graphify AST nodes hash repo + source_file + label + ID. The hash includes
// Node.ID (graphify's per-repo-stable id) because (source_file, label) alone
// is NOT unique in real code — a single Go source file typically defines many
// types that each implement the same method (e.g. 52 distinct types in
// vendor/.../redis/v9/command.go each declaring .String()). Graphify emits
// those as distinct nodes with distinct ids; omitting the id here collapses
// them into one Neo4j Entity via MERGE on node_key, silently losing the other
// 51 rows and their edges.
func StableKey(repo string, n Node) string {
	if n.Origin == "platform" {
		return "platform::" + n.ID
	}
	h := sha1.New()
	h.Write([]byte(repo + "::" + n.SourceFile + "::" + n.Label + "::" + n.ID))
	return hex.EncodeToString(h.Sum(nil))
}
