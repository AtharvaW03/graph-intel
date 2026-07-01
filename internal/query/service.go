package query

import (
	"context"
	"errors"
	"fmt"

	"graph-platform/internal/neo4j"

	driver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	defaultBlastDepth   = 3
	maxBlastDepth       = 10
	shortestPathHopsMax = 15
	searchLimit         = 100
)

type Service struct {
	db *neo4j.Client
}

func NewService(db *neo4j.Client) *Service {
	return &Service{db: db}
}

func (s *Service) read(ctx context.Context, fn func(tx driver.ManagedTransaction) (any, error)) (any, error) {
	sess := s.db.Driver.NewSession(ctx, driver.SessionConfig{AccessMode: driver.AccessModeRead})
	defer sess.Close(ctx)
	return sess.ExecuteRead(ctx, fn)
}

// Search returns nodes whose name or norm_name contains q (case-insensitive),
// ordered by match quality (exact > prefix > contains) then by name length.
func (s *Service) Search(ctx context.Context, q string) ([]SearchResult, error) {
	if q == "" {
		return []SearchResult{}, nil
	}

	const cypher = `
WITH toLower($q) AS qlow
MATCH (n:Entity)
WHERE toLower(n.name) CONTAINS qlow OR toLower(n.norm_name) CONTAINS qlow
RETURN n.node_key      AS node_key,
       n.graphify_id   AS graphify_id,
       n.name          AS name,
       labels(n)       AS labels,
       n.repo          AS repo,
       n.path          AS path,
       n.line          AS line,
       CASE
         WHEN toLower(n.name) = qlow                  THEN 0
         WHEN toLower(n.name) STARTS WITH qlow        THEN 1
         WHEN toLower(n.norm_name) = qlow             THEN 2
         WHEN toLower(n.norm_name) STARTS WITH qlow   THEN 3
         ELSE 4
       END AS rank
ORDER BY rank, size(n.name), n.name
LIMIT $limit
`

	out, err := s.read(ctx, func(tx driver.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, cypher, map[string]any{"q": q, "limit": searchLimit})
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		results := make([]SearchResult, 0, len(records))
		for _, r := range records {
			results = append(results, SearchResult{
				NodeKey:    asString(r.AsMap()["node_key"]),
				GraphifyID: asString(r.AsMap()["graphify_id"]),
				Name:       asString(r.AsMap()["name"]),
				Labels:     asStringSlice(r.AsMap()["labels"]),
				Repo:       asString(r.AsMap()["repo"]),
				Path:       asString(r.AsMap()["path"]),
				Line:       asString(r.AsMap()["line"]),
			})
		}
		return results, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]SearchResult), nil
}

// FindSymbol returns every node whose name (or norm_name) exactly matches the
// supplied symbol, across all repositories. Case-insensitive.
func (s *Service) FindSymbol(ctx context.Context, symbol string) ([]SymbolResult, error) {
	if symbol == "" {
		return []SymbolResult{}, nil
	}

	const cypher = `
WITH toLower($s) AS slow
MATCH (n:Entity)
WHERE toLower(n.name) = slow OR toLower(n.norm_name) = slow
RETURN n.name           AS name,
       n.repo           AS repo,
       n.path           AS path,
       n.line           AS line,
       labels(n)        AS labels,
       n.community      AS community
ORDER BY n.repo, n.path, n.line
`

	out, err := s.read(ctx, func(tx driver.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, cypher, map[string]any{"s": symbol})
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		results := make([]SymbolResult, 0, len(records))
		for _, r := range records {
			m := r.AsMap()
			results = append(results, SymbolResult{
				Name:      asString(m["name"]),
				Repo:      asString(m["repo"]),
				Path:      asString(m["path"]),
				Line:      asString(m["line"]),
				Labels:    asStringSlice(m["labels"]),
				Community: asInt(m["community"]),
			})
		}
		return results, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]SymbolResult), nil
}

// FindCallers returns every function with a CALLS edge pointing at the symbol.
func (s *Service) FindCallers(ctx context.Context, symbol string) ([]CallEdge, error) {
	if symbol == "" {
		return []CallEdge{}, nil
	}

	const cypher = `
WITH toLower($s) AS slow
MATCH (caller:Entity)-[:CALLS]->(callee:Entity)
WHERE toLower(callee.name) = slow OR toLower(callee.norm_name) = slow
RETURN caller.name AS caller,
       caller.repo AS caller_repo,
       caller.path AS caller_path,
       caller.line AS caller_line,
       labels(caller) AS labels,
       callee.name AS callee,
       callee.repo AS callee_repo,
       callee.path AS callee_path
ORDER BY caller.repo, caller.path, caller.line
`
	return s.runCallEdgeQuery(ctx, cypher, symbol)
}

// FindCallees returns every function the supplied symbol calls.
func (s *Service) FindCallees(ctx context.Context, symbol string) ([]CallEdge, error) {
	if symbol == "" {
		return []CallEdge{}, nil
	}

	const cypher = `
WITH toLower($s) AS slow
MATCH (caller:Entity)-[:CALLS]->(callee:Entity)
WHERE toLower(caller.name) = slow OR toLower(caller.norm_name) = slow
RETURN caller.name AS caller,
       caller.repo AS caller_repo,
       caller.path AS caller_path,
       caller.line AS caller_line,
       labels(callee) AS labels,
       callee.name AS callee,
       callee.repo AS callee_repo,
       callee.path AS callee_path
ORDER BY callee.repo, callee.path, callee.line
`
	return s.runCallEdgeQuery(ctx, cypher, symbol)
}

func (s *Service) runCallEdgeQuery(ctx context.Context, cypher, symbol string) ([]CallEdge, error) {
	out, err := s.read(ctx, func(tx driver.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, cypher, map[string]any{"s": symbol})
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		edges := make([]CallEdge, 0, len(records))
		for _, r := range records {
			m := r.AsMap()
			edges = append(edges, CallEdge{
				Caller:     asString(m["caller"]),
				CallerRepo: asString(m["caller_repo"]),
				CallerPath: asString(m["caller_path"]),
				CallerLine: asString(m["caller_line"]),
				Callee:     asString(m["callee"]),
				CalleeRepo: asString(m["callee_repo"]),
				CalleePath: asString(m["callee_path"]),
				Labels:     asStringSlice(m["labels"]),
			})
		}
		return edges, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]CallEdge), nil
}

// BlastRadius walks outgoing edges up to depth and returns each reachable node
// with its minimum distance from the source symbol, ordered by distance asc.
// depth <= 0 falls back to defaultBlastDepth; depths above maxBlastDepth are
// clamped to prevent runaway traversals.
func (s *Service) BlastRadius(ctx context.Context, symbol string, depth int) ([]ImpactNode, error) {
	if symbol == "" {
		return []ImpactNode{}, nil
	}
	if depth <= 0 {
		depth = defaultBlastDepth
	}
	if depth > maxBlastDepth {
		depth = maxBlastDepth
	}

	// The variable-length path depth cannot be parameterized in Cypher.
	// depth is bounded by the clamp above, so string-formatting it is safe.
	cypher := fmt.Sprintf(`
WITH toLower($s) AS slow
MATCH (start:Entity)
WHERE toLower(start.name) = slow OR toLower(start.norm_name) = slow
MATCH p = (start)-[*1..%d]->(impacted:Entity)
WITH impacted, min(length(p)) AS distance
RETURN impacted.name  AS name,
       impacted.repo  AS repo,
       impacted.path  AS path,
       impacted.line  AS line,
       labels(impacted) AS labels,
       distance
ORDER BY distance, name
`, depth)

	out, err := s.read(ctx, func(tx driver.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, cypher, map[string]any{"s": symbol})
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		nodes := make([]ImpactNode, 0, len(records))
		for _, r := range records {
			m := r.AsMap()
			nodes = append(nodes, ImpactNode{
				Name:     asString(m["name"]),
				Repo:     asString(m["repo"]),
				Path:     asString(m["path"]),
				Line:     asString(m["line"]),
				Labels:   asStringSlice(m["labels"]),
				Distance: asInt(m["distance"]),
			})
		}
		return nodes, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]ImpactNode), nil
}

// ShortestPath returns one shortest undirected path between source and target
// symbols, as an ordered list of nodes. Each node carries the relationship
// type used to reach it from the previous node (empty on the first).
func (s *Service) ShortestPath(ctx context.Context, source, target string) ([]PathNode, error) {
	if source == "" || target == "" {
		return []PathNode{}, nil
	}

	cypher := fmt.Sprintf(`
WITH toLower($src) AS srclow, toLower($dst) AS dstlow
MATCH (src:Entity), (dst:Entity)
WHERE (toLower(src.name) = srclow OR toLower(src.norm_name) = srclow)
  AND (toLower(dst.name) = dstlow OR toLower(dst.norm_name) = dstlow)
WITH src, dst LIMIT 1
MATCH p = shortestPath((src)-[*..%d]-(dst))
WITH nodes(p) AS ns, relationships(p) AS rs
UNWIND range(0, size(ns)-1) AS i
RETURN ns[i].name  AS name,
       ns[i].repo  AS repo,
       ns[i].path  AS path,
       labels(ns[i]) AS labels,
       CASE WHEN i = 0 THEN '' ELSE type(rs[i-1]) END AS relationship,
       i AS idx
ORDER BY idx
`, shortestPathHopsMax)

	out, err := s.read(ctx, func(tx driver.ManagedTransaction) (any, error) {
		res, err := tx.Run(ctx, cypher, map[string]any{"src": source, "dst": target})
		if err != nil {
			return nil, err
		}
		records, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		path := make([]PathNode, 0, len(records))
		for _, r := range records {
			m := r.AsMap()
			path = append(path, PathNode{
				Name:         asString(m["name"]),
				Repo:         asString(m["repo"]),
				Path:         asString(m["path"]),
				Labels:       asStringSlice(m["labels"]),
				Relationship: asString(m["relationship"]),
			})
		}
		return path, nil
	})
	if err != nil {
		return nil, err
	}
	return out.([]PathNode), nil
}

// ErrNotImplemented is returned by stubs that depend on data the importer
// doesn't yet produce.
var ErrNotImplemented = errors.New("not implemented")

// FindRepositoryDependencies will eventually return cross-repo dependency
// edges. The importer doesn't emit those yet, so this is a deliberate stub.
func (s *Service) FindRepositoryDependencies(ctx context.Context, repo string) ([]SymbolResult, error) {
	return nil, ErrNotImplemented
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}

func asInt(v any) int {
	switch x := v.(type) {
	case int64:
		return int(x)
	case int:
		return x
	case float64:
		return int(x)
	}
	return 0
}

func asStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, x := range arr {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
