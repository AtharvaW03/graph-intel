package query

import (
	"context"
	"fmt"

	driver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// FindDependencies returns every package (and cross-repo target) the given
// repository declares a dependency on. Pass scope="" to include every scope
// (runtime, dev, indirect, peer, optional); pass a specific scope to filter.
//
// The source anchor is the deps extractor's per-repo hub Entity — an
// (:Entity:Package {graphify_id: "repo::<name>"}) node — NOT the importer's
// (:Repository {name}) node. The extractors emit their edges from the hub
// they create themselves; the (:Repository) node is only connected to
// entities via generic (:Repository)-[:CONTAINS]->(:Entity) edges from
// importNodeBatch, never via typed relations like DEPENDS_ON.
func (s *Service) FindDependencies(ctx context.Context, repo, scope string) ([]DependencyEdge, error) {
	if repo == "" {
		return []DependencyEdge{}, nil
	}
	const cypher = `
MATCH (r:Entity {graphify_id: 'repo::' + $repo})-[d:DEPENDS_ON|DEPENDS_ON_REPO]->(p:Entity)
WHERE $scope = '' OR d.context = $scope
RETURN p.name                          AS name,
       labels(p)                       AS labels,
       coalesce(p.ecosystem, '')        AS ecosystem,
       coalesce(p.version, '')          AS version,
       coalesce(d.context, '')          AS scope,
       type(d) = 'DEPENDS_ON_REPO'      AS cross
ORDER BY ecosystem, name
`
	return s.runDepQuery(ctx, cypher, map[string]any{"repo": repo, "scope": scope}, repo)
}

// FindDependents returns every repository whose manifest declares a
// dependency on dep. dep may be a package name (e.g. "github.com/foo/bar"
// or "lodash") or, when the deps extractor inferred a cross-repo edge, the
// short name of the depended-upon repository ("auth-service").
//
// r is the deps extractor's per-repo hub Entity — see FindDependencies for
// why we don't match on (:Repository) here.
func (s *Service) FindDependents(ctx context.Context, dep string) ([]DependencyEdge, error) {
	if dep == "" {
		return []DependencyEdge{}, nil
	}
	const cypher = `
MATCH (r:Entity)-[d:DEPENDS_ON|DEPENDS_ON_REPO]->(p:Entity {name: $dep})
WHERE r.graphify_id STARTS WITH 'repo::'
RETURN r.name                          AS name,
       labels(r)                       AS labels,
       coalesce(p.ecosystem, '')        AS ecosystem,
       coalesce(p.version, '')          AS version,
       coalesce(d.context, '')          AS scope,
       type(d) = 'DEPENDS_ON_REPO'      AS cross
ORDER BY name
`
	return s.runDepQuery(ctx, cypher, map[string]any{"dep": dep}, "")
}

func (s *Service) runDepQuery(ctx context.Context, cypher string, params map[string]any, repoBound string) ([]DependencyEdge, error) {
	out, err := s.read(ctx, func(tx driver.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, cypher, params)
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		edges := make([]DependencyEdge, 0, len(records))
		for _, r := range records {
			m := r.AsMap()
			edges = append(edges, DependencyEdge{
				Repo:      repoBound,
				Name:      asString(m["name"]),
				Labels:    asStringSlice(m["labels"]),
				Ecosystem: asString(m["ecosystem"]),
				Version:   asString(m["version"]),
				Scope:     asString(m["scope"]),
				Cross:     asBool(m["cross"]),
			})
		}
		return edges, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]DependencyEdge), nil
}

// FindRoutes returns HTTP routes matching the supplied filters. Empty filters
// mean "any". Results are ordered by repo then path.
//
// HttpRoute nodes carry the method + HTTP path in their `name` property
// (formatted "METHOD /path" by the extractor); the node's `path` property is
// the source FILE path, not the HTTP path. Method and path are parsed from
// `rt.name` — which works for data imported before metadata promotion — and
// the handler falls back to the promoted `handler` property when present.
//
// The query doesn't join through (:Repository) — each HttpRoute carries a
// `repo` property set by importNodeBatch (routes are repo-owned, not shared),
// so scoping by repo is a direct property filter with no join.
func (s *Service) FindRoutes(ctx context.Context, method, pathContains, repo string) ([]HTTPRoute, error) {
	const cypher = `
MATCH (rt:HttpRoute)
WITH rt,
     coalesce(rt.name, '')                                                             AS full,
     CASE WHEN rt.name CONTAINS ' ' THEN split(rt.name, ' ')[0] ELSE '' END              AS method_part,
     CASE WHEN rt.name CONTAINS ' ' THEN substring(rt.name, size(split(rt.name,' ')[0]) + 1) ELSE coalesce(rt.name, '') END AS path_part
WHERE ($method = '' OR toUpper(method_part) = toUpper($method))
  AND ($path = '' OR toLower(path_part) CONTAINS toLower($path))
  AND ($repo = '' OR coalesce(rt.repo, '') = $repo)
RETURN coalesce(rt.repo, '')     AS repo,
       method_part               AS method,
       path_part                 AS path,
       coalesce(rt.handler, '')  AS handler,
       labels(rt)                AS labels,
       coalesce(rt.path, '')     AS file_path,
       coalesce(rt.line, '')     AS line
ORDER BY repo, path
LIMIT 500
`
	out, err := s.read(ctx, func(tx driver.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, cypher, map[string]any{
			"method": method, "path": pathContains, "repo": repo,
		})
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]HTTPRoute, 0, len(records))
		for _, rec := range records {
			m := rec.AsMap()
			out = append(out, HTTPRoute{
				Repo:    asString(m["repo"]),
				Method:  asString(m["method"]),
				Path:    asString(m["path"]),
				Handler: asString(m["handler"]),
				Labels:  asStringSlice(m["labels"]),
				File:    asString(m["file_path"]),
				Line:    asString(m["line"]),
			})
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]HTTPRoute), nil
}

// FindKafkaTopic returns one topic plus all repositories that produce to or
// consume from it. topic="" is an error.
func (s *Service) FindKafkaTopic(ctx context.Context, topic string) (*KafkaTopicInfo, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic required")
	}
	// Kafka extractor emits PRODUCES/CONSUMES from its per-repo hub Entity
	// (graphify_id = "repo::<name>"), not from :Repository. Filter to those
	// hub nodes so we surface repo names, not arbitrary function nodes if
	// the extractor is later extended to emit finer-grained producers.
	//
	// Aggregation groups by t.name (not by node) so producers and consumers
	// are collected across every node carrying the topic's name — the current
	// unified shared node plus any legacy per-repo duplicates from imports
	// that predate cross-repo node unification.
	const cypher = `
MATCH (t:KafkaTopic {name: $topic})
OPTIONAL MATCH (rp:Entity)-[:PRODUCES]->(t) WHERE rp.graphify_id STARTS WITH 'repo::'
OPTIONAL MATCH (rc:Entity)-[:CONSUMES]->(t) WHERE rc.graphify_id STARTS WITH 'repo::'
RETURN t.name                             AS topic,
       collect(DISTINCT rp.name)          AS producers,
       collect(DISTINCT rc.name)          AS consumers
`
	out, err := s.read(ctx, func(tx driver.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, cypher, map[string]any{"topic": topic})
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		if len(records) == 0 {
			return (*KafkaTopicInfo)(nil), nil
		}
		m := records[0].AsMap()
		info := &KafkaTopicInfo{
			Topic:     asString(m["topic"]),
			Producers: filterEmpty(asStringSlice(m["producers"])),
			Consumers: filterEmpty(asStringSlice(m["consumers"])),
		}
		return info, nil
	})
	if err != nil {
		return nil, err
	}
	return out.(*KafkaTopicInfo), nil
}

// FindSQLObject returns matching SQL Server objects (by bare or fully-qualified
// name) plus the tables they read, write, depend on, or trigger on.
func (s *Service) FindSQLObject(ctx context.Context, schema, name string) ([]SQLObjectInfo, error) {
	if name == "" {
		return []SQLObjectInfo{}, nil
	}
	full := name
	if schema != "" {
		full = schema + "." + name
	}
	const cypher = `
MATCH (o:Entity)
WHERE any(l IN labels(o) WHERE l IN ['SqlTable','SqlView','SqlProcedure','SqlTrigger','SqlFunction','SqlSchema'])
  AND (
    o.name = $full
    OR ($schema = '' AND split(coalesce(o.name, ''), '.')[size(split(coalesce(o.name, ''), '.'))-1] = $name)
  )
OPTIONAL MATCH (o)-[:READS_TABLE]->(rt:SqlTable)
OPTIONAL MATCH (o)-[:WRITES_TABLE]->(wt:SqlTable)
OPTIONAL MATCH (o)-[:DEPENDS_ON_OBJECT]->(dep:Entity)
OPTIONAL MATCH (o)-[:TRIGGERS_ON]->(tt:SqlTable)
RETURN o.name                              AS name,
       labels(o)                           AS labels,
       coalesce(o.path, '')                 AS file,
       coalesce(o.line, '')                 AS line,
       collect(DISTINCT rt.name)            AS reads,
       collect(DISTINCT wt.name)            AS writes,
       collect(DISTINCT dep.name)           AS depends_on,
       coalesce(head(collect(DISTINCT tt.name)), '') AS triggers_on
ORDER BY name
`
	out, err := s.read(ctx, func(tx driver.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, cypher, map[string]any{"schema": schema, "name": name, "full": full})
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]SQLObjectInfo, 0, len(records))
		for _, rec := range records {
			m := rec.AsMap()
			labels := asStringSlice(m["labels"])
			kind := ""
			for _, l := range labels {
				switch l {
				case "SqlTable", "SqlView", "SqlProcedure", "SqlTrigger", "SqlFunction", "SqlSchema":
					kind = l
				}
			}
			fullName := asString(m["name"])
			sch, base := splitSchemaName(fullName)
			out = append(out, SQLObjectInfo{
				Name:       base,
				Schema:     sch,
				Kind:       kind,
				Labels:     labels,
				File:       asString(m["file"]),
				Line:       asString(m["line"]),
				Reads:      filterEmpty(asStringSlice(m["reads"])),
				Writes:     filterEmpty(asStringSlice(m["writes"])),
				DependsOn:  filterEmpty(asStringSlice(m["depends_on"])),
				TriggersOn: asString(m["triggers_on"]),
			})
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]SQLObjectInfo), nil
}

// FindGlueJobs returns Glue jobs filtered by source or destination table.
// Pass both arguments empty to list every Glue job.
func (s *Service) FindGlueJobs(ctx context.Context, source, target string) ([]GlueJobInfo, error) {
	const cypher = `
MATCH (j:GlueJob)
OPTIONAL MATCH (j)-[:READS_SOURCE]->(s:Entity)
OPTIONAL MATCH (j)-[:WRITES_DESTINATION]->(t:Entity)
OPTIONAL MATCH (r:Repository)-[:CONTAINS]->(j)
WITH j, r, collect(DISTINCT s.name) AS sources, collect(DISTINCT t.name) AS targets
WHERE ($source = '' OR $source IN sources)
  AND ($target = '' OR $target IN targets)
RETURN j.name                AS name,
       coalesce(r.name, '')   AS repo,
       labels(j)              AS labels,
       coalesce(j.path, '')    AS file,
       coalesce(j.script, '')  AS script,
       coalesce(j.schedule, '') AS schedule,
       sources                AS sources,
       targets                AS targets
ORDER BY repo, name
`
	out, err := s.read(ctx, func(tx driver.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, cypher, map[string]any{"source": source, "target": target})
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]GlueJobInfo, 0, len(records))
		for _, rec := range records {
			m := rec.AsMap()
			out = append(out, GlueJobInfo{
				Name:     asString(m["name"]),
				Repo:     asString(m["repo"]),
				Labels:   asStringSlice(m["labels"]),
				File:     asString(m["file"]),
				Script:   asString(m["script"]),
				Schedule: asString(m["schedule"]),
				Sources:  filterEmpty(asStringSlice(m["sources"])),
				Targets:  filterEmpty(asStringSlice(m["targets"])),
			})
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]GlueJobInfo), nil
}

func splitSchemaName(full string) (schema, name string) {
	for i := 0; i < len(full); i++ {
		if full[i] == '.' {
			return full[:i], full[i+1:]
		}
	}
	return "", full
}

func asBool(v any) bool {
	b, _ := v.(bool)
	return b
}

func filterEmpty(xs []string) []string {
	out := xs[:0]
	for _, x := range xs {
		if x != "" {
			out = append(out, x)
		}
	}
	return out
}
