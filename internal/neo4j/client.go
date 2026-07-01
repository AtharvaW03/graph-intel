package neo4j

import (
	"context"
	"fmt"

	"graph-platform/internal/graphify"

	driver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const batchSize = 500

// labelAllowlist enumerates every Neo4j label the importer is allowed to set
// on Entity nodes. Cypher does not permit parameterized labels, so labels are
// interpolated into the query string — this allowlist is the security gate.
// New labels added here must also be returned by graphify.InferLabel via the
// typeToLabel map (or one of the heuristic rules).
var labelAllowlist = map[string]bool{
	// graphify core
	"File": true, "Function": true, "Class": true,
	"Package": true, "DocSection": true, "Symbol": true,

	// extractor-plugin entities
	"HttpRoute":     true,
	"KafkaTopic":    true,
	"KafkaProducer": true,
	"KafkaConsumer": true,
	"SqlSchema":     true,
	"SqlTable":      true,
	"SqlView":       true,
	"SqlProcedure":  true,
	"SqlTrigger":    true,
	"SqlFunction":   true,
	"GlueJob":       true,
}

type Client struct {
	Driver driver.DriverWithContext
}

func New(uri, username, password string) (*Client, error) {
	d, err := driver.NewDriverWithContext(uri, driver.BasicAuth(username, password, ""))
	if err != nil {
		return nil, err
	}
	if err := d.VerifyConnectivity(context.Background()); err != nil {
		_ = d.Close(context.Background())
		return nil, err
	}
	return &Client{Driver: d}, nil
}

func (c *Client) Close() error {
	return c.Driver.Close(context.Background())
}

// VerifyConnectivity probes the driver. Useful for long-running daemons to
// pre-flight a session before each indexing cycle so a transient outage
// surfaces as a logged warning instead of a stage-3 import failure.
func (c *Client) VerifyConnectivity(ctx context.Context) error {
	return c.Driver.VerifyConnectivity(ctx)
}

// EnsureConstraints creates the unique constraint on Entity.node_key, the repo
// index, and the unique constraint on Repository.name — all idempotent.
func (c *Client) EnsureConstraints(ctx context.Context) error {
	session := c.Driver.NewSession(ctx, driver.SessionConfig{})
	defer session.Close(ctx)

	stmts := []string{
		`CREATE CONSTRAINT entity_key IF NOT EXISTS FOR (n:Entity) REQUIRE n.node_key IS UNIQUE`,
		`CREATE INDEX entity_repo IF NOT EXISTS FOR (n:Entity) ON (n.repo)`,
		`CREATE CONSTRAINT repo_name IF NOT EXISTS FOR (r:Repository) REQUIRE r.name IS UNIQUE`,
	}
	for _, q := range stmts {
		if _, err := session.Run(ctx, q, nil); err != nil {
			return fmt.Errorf("constraint %q: %w", q, err)
		}
	}
	return nil
}

// CountEntitiesForRepo returns the number of :Entity nodes currently in
// Neo4j scoped to repo. Called by the importer after its pipeline completes
// so Summary.NodesInGraph reflects Neo4j's actual state rather than the
// caller's input count — a divergence between the two is a silent
// data-loss signal (e.g. the StableKey collision bug fixed in v1.1).
func (c *Client) CountEntitiesForRepo(ctx context.Context, repo string) (int, error) {
	session := c.Driver.NewSession(ctx, driver.SessionConfig{AccessMode: driver.AccessModeRead})
	defer session.Close(ctx)
	res, err := session.Run(ctx, `MATCH (n:Entity {repo: $repo}) RETURN count(n) AS c`, map[string]any{"repo": repo})
	if err != nil {
		return 0, fmt.Errorf("count entities: %w", err)
	}
	rec, err := res.Single(ctx)
	if err != nil {
		return 0, fmt.Errorf("count entities (read): %w", err)
	}
	c64, _ := rec.AsMap()["c"].(int64)
	return int(c64), nil
}

// MergeRepository ensures a (:Repository {name}) node exists.
func (c *Client) MergeRepository(ctx context.Context, repo string) error {
	session := c.Driver.NewSession(ctx, driver.SessionConfig{})
	defer session.Close(ctx)
	_, err := session.Run(ctx, `MERGE (:Repository {name: $name})`, map[string]any{"name": repo})
	return err
}

// ImportNodes imports all nodes in label-grouped UNWIND batches. commit is
// stamped onto every node as last_commit so a later SweepStale can identify
// and remove nodes from prior commits; pass "" to skip the stamp (used by
// the legacy importer CLI for static graph.json runs).
// Returns the idToKey map (graphify ID → stable key) and per-label counts.
func (c *Client) ImportNodes(ctx context.Context, repo, commit string, nodes []graphify.Node) (map[string]string, map[string]int, error) {
	idToKey := make(map[string]string, len(nodes))
	labelGroups := make(map[string][]map[string]any)
	labelCounts := make(map[string]int)

	for _, n := range nodes {
		label := graphify.InferLabel(n)
		if !labelAllowlist[label] {
			label = "Symbol"
		}
		key := graphify.StableKey(repo, n)
		idToKey[n.ID] = key
		labelCounts[label]++

		labelGroups[label] = append(labelGroups[label], map[string]any{
			"key":            key,
			"graphify_id":    n.ID,
			"name":           n.Label,
			"norm_name":      n.NormLabel,
			"path":           n.SourceFile,
			"line":           n.SourceLocation,
			"language":       n.Metadata.Language,
			"file_type":      n.FileType,
			"community":      n.Community,
			"community_name": n.CommunityName,
		})
	}

	for label, rows := range labelGroups {
		for i := 0; i < len(rows); i += batchSize {
			end := i + batchSize
			if end > len(rows) {
				end = len(rows)
			}
			if err := c.importNodeBatch(ctx, label, repo, commit, rows[i:end]); err != nil {
				return nil, nil, fmt.Errorf("import nodes (%s): %w", label, err)
			}
		}
	}

	return idToKey, labelCounts, nil
}

// ImportLinks imports all links in relation-type-grouped UNWIND batches.
// commit, if non-empty, is stamped onto every edge as last_commit so stale
// edges (same endpoints but the relation was removed in a later commit)
// can be swept by SweepStale.
// Returns per-relation counts, skipped-unknown count, and skipped-dangling count.
func (c *Client) ImportLinks(ctx context.Context, commit string, links []graphify.Link, idToKey map[string]string) (map[string]int, int, int, error) {
	relGroups := make(map[string][]map[string]any)
	relCounts := make(map[string]int)
	skippedUnknown := 0
	skippedDangling := 0

	for _, l := range links {
		rel, ok := graphify.MapRelation(l.Relation)
		if !ok {
			skippedUnknown++
			continue
		}
		srcKey, ok1 := idToKey[l.Source]
		tgtKey, ok2 := idToKey[l.Target]
		if !ok1 || !ok2 {
			skippedDangling++
			continue
		}
		relCounts[rel]++
		relGroups[rel] = append(relGroups[rel], map[string]any{
			"s":          srcKey,
			"t":          tgtKey,
			"weight":     l.Weight,
			"confidence": l.Confidence,
			"cs":         l.ConfidenceScore,
			"context":    l.Context,
		})
	}

	for rel, rows := range relGroups {
		for i := 0; i < len(rows); i += batchSize {
			end := i + batchSize
			if end > len(rows) {
				end = len(rows)
			}
			if err := c.importLinkBatch(ctx, rel, commit, rows[i:end]); err != nil {
				return nil, 0, 0, fmt.Errorf("import links (%s): %w", rel, err)
			}
		}
	}

	return relCounts, skippedUnknown, skippedDangling, nil
}

// SweepStale removes Entity nodes and relationships for repo whose last_commit
// does not match the current commit. It is the cleanup step that keeps the
// graph in sync with the source tree on re-index — nodes/edges deleted in the
// new commit are removed instead of accumulating forever.
//
// commit must be non-empty; an empty commit would sweep everything for the
// repo, which is almost certainly an operator error and so is refused.
//
// Returns (nodesDeleted, relsDeleted). DETACH DELETE on a node also removes
// its relationships; the relsDeleted figure covers only edges between
// already-stamped endpoints that were stale but whose endpoints survived.
func (c *Client) SweepStale(ctx context.Context, repo, commit string) (int, int, error) {
	if commit == "" {
		return 0, 0, fmt.Errorf("sweep refused: commit is empty for repo %q", repo)
	}
	session := c.Driver.NewSession(ctx, driver.SessionConfig{})
	defer session.Close(ctx)

	// Step 1: sweep stale relationships whose endpoints are not (yet) being
	// removed by node sweep. Edges with NULL last_commit are legacy edges from
	// before stamping was added; they're treated as stale too.
	relRes, err := session.Run(ctx, `
MATCH (a:Entity {repo: $repo})-[r]->(b:Entity)
WHERE (r.last_commit IS NULL OR r.last_commit <> $commit)
DELETE r
RETURN count(r) AS deleted`, map[string]any{"repo": repo, "commit": commit})
	if err != nil {
		return 0, 0, fmt.Errorf("sweep stale relationships: %w", err)
	}
	relRec, err := relRes.Single(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("sweep stale relationships (read): %w", err)
	}
	relsDeleted := int(relRec.AsMap()["deleted"].(int64))

	// Step 2: sweep stale nodes. DETACH DELETE also removes Repository
	// containment edges, which is fine — they're re-created on the next import.
	nodeRes, err := session.Run(ctx, `
MATCH (n:Entity {repo: $repo})
WHERE (n.last_commit IS NULL OR n.last_commit <> $commit)
DETACH DELETE n
RETURN count(n) AS deleted`, map[string]any{"repo": repo, "commit": commit})
	if err != nil {
		return 0, 0, fmt.Errorf("sweep stale nodes: %w", err)
	}
	nodeRec, err := nodeRes.Single(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("sweep stale nodes (read): %w", err)
	}
	nodesDeleted := int(nodeRec.AsMap()["deleted"].(int64))

	return nodesDeleted, relsDeleted, nil
}

// importNodeBatch runs one UNWIND batch for a single label.
// label is validated against labelAllowlist before reaching here, so
// interpolating it into the query string is safe. commit, if non-empty, is
// stamped on every node as last_commit; the empty case preserves legacy
// behavior for the static-graph importer CLI.
func (c *Client) importNodeBatch(ctx context.Context, label, repo, commit string, batch []map[string]any) error {
	session := c.Driver.NewSession(ctx, driver.SessionConfig{})
	defer session.Close(ctx)

	commitClause := ""
	if commit != "" {
		commitClause = ",\n    last_commit:   $commit"
	}

	query := fmt.Sprintf(`
MATCH (repo:Repository {name: $repo})
UNWIND $batch AS row
MERGE (n:Entity {node_key: row.key})
SET n:%s
SET n += {
    graphify_id:   row.graphify_id,
    repo:          $repo,
    name:          row.name,
    norm_name:     row.norm_name,
    path:          row.path,
    line:          row.line,
    language:      row.language,
    file_type:     row.file_type,
    community:     row.community,
    community_name: row.community_name%s
}
MERGE (repo)-[:CONTAINS]->(n)`, label, commitClause)

	params := map[string]any{"repo": repo, "batch": batch}
	if commit != "" {
		params["commit"] = commit
	}
	_, err := session.Run(ctx, query, params)
	return err
}

// importLinkBatch runs one UNWIND batch for a single relationship type.
// rel is validated via MapRelation's allowlist map before reaching here.
// commit, if non-empty, stamps each edge so sweep can identify stale edges.
func (c *Client) importLinkBatch(ctx context.Context, rel, commit string, batch []map[string]any) error {
	session := c.Driver.NewSession(ctx, driver.SessionConfig{})
	defer session.Close(ctx)

	commitClause := ""
	if commit != "" {
		commitClause = ",\n    last_commit:      $commit"
	}

	query := fmt.Sprintf(`
UNWIND $batch AS row
MATCH (a:Entity {node_key: row.s})
MATCH (b:Entity {node_key: row.t})
MERGE (a)-[r:%s]->(b)
SET r += {
    weight:           row.weight,
    confidence:       row.confidence,
    confidence_score: row.cs,
    context:          row.context%s
}`, rel, commitClause)

	params := map[string]any{"batch": batch}
	if commit != "" {
		params["commit"] = commit
	}
	_, err := session.Run(ctx, query, params)
	return err
}
