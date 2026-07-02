# code-intel — Org-Wide Code Intelligence Platform

A self-serve code intelligence service: it keeps every configured repository
cloned and indexed into a Neo4j knowledge graph, and answers questions like
*"where is `processPayment` defined?"*, *"which repos depend on
auth-service?"*, *"who consumes the `trade_executed` topic?"*, or *"which
procs write to the trades table?"* — across repositories and languages.

> The three documents under [`docs/graphify/`](docs/graphify/) describe
> **graphify**, the external AST-extraction CLI this platform shells out to.
> They are reference material for that tool, not documentation of this repo.

## Architecture

```
config/repos.yaml
      │
      ▼
┌──────────────┐   clone/fetch   ┌────────────┐
│   indexer    │────────────────▶│  workdir/  │  (shallow clones + state.json)
│ (cmd/indexer)│                 └────────────┘
│              │  1. git sync
│              │  2. graphify update <repo>      → graphify-out/graph.json (AST)
│              │  3. extractors (deps, http_api, kafka, mssql, glue)
│              │  4. merge fragments into graph.json
│              │  5. import into Neo4j + sweep stale + verify counts
└──────┬───────┘
       ▼
   ┌───────┐     ┌───────────────┐     ┌────────────┐
   │ Neo4j │◀────│ query-service │◀────│ mcp-server │◀── Claude / agents
   └───────┘     │  (REST, 8080) │     │  (stdio)   │
                 └───────▲───────┘     └────────────┘
                         └── engineers via curl / web UI
```

- **`cmd/indexer`** — the pipeline daemon. One-shot or continuous
  (`--interval 15m`). Per-repo failures never block other repos; state
  persists in `workdir/state.json`.
- **`cmd/query-service`** — read-only REST API over the graph.
- **`cmd/mcp-server`** — MCP stdio adapter exposing the query API as agent
  tools (`search_code`, `find_callers`, `blast_radius`,
  `repository_overview`, ...).
- **`cmd/importer`** — manual one-off import of a static `graph.json`.
- **`internal/extract/*`** — domain extractors that enrich the AST graph:
  dependency manifests (13 ecosystems), HTTP routes, Kafka topics, MS SQL
  objects, AWS Glue jobs.

### Graph model

Everything is an `:Entity` node keyed by `node_key`, with a specific label
(`Function`, `Class`, `HttpRoute`, `KafkaTopic`, `SqlProcedure`, `GlueJob`,
...). Code entities are **repo-owned** (`repo` property, swept when they
disappear from the source). Org-global entities — Kafka topics, packages, SQL
objects, repository hubs — are **shared** (`shared: true`, no `repo`
property): the same topic referenced by five repos is ONE node, which is what
makes cross-repo traversal work. Shared nodes are garbage-collected only when
no repo references them anymore.

Every node and edge is stamped with `last_commit` at import; re-indexing a
repo sweeps whatever the new commit no longer contains.

## Quickstart

Prereqs: Go 1.26+, a local `git`, Neo4j 5.x, and the `graphify` CLI on PATH.

```powershell
# 1. Neo4j (e.g. Docker)
docker run -d --name neo4j -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/localpass neo4j:5

# 2. Environment
$env:NEO4J_PASSWORD = "localpass"      # required by indexer + query-service
$env:QUERY_AUTH_TOKEN = "dev-token"    # optional; omit for open local dev

# 3. Index everything in config/repos.yaml
go run ./cmd/indexer --all                  # one-shot
go run ./cmd/indexer --all --interval 15m   # continuous daemon

# 4. Serve queries
go run ./cmd/query-service                  # listens on :8080

# 5. Ask questions
curl -H "Authorization: Bearer dev-token" "http://localhost:8080/search?q=processPayment"
curl -H "Authorization: Bearer dev-token" "http://localhost:8080/overview/my-repo"
```

### Environment variables

| Variable | Used by | Default | Purpose |
|---|---|---|---|
| `NEO4J_PASSWORD` | indexer, query-service, importer | *(required)* | Neo4j auth |
| `NEO4J_URI` | indexer, query-service | `neo4j://127.0.0.1:7687` | Neo4j endpoint |
| `NEO4J_USER` | indexer, query-service | `neo4j` | Neo4j user |
| `QUERY_PORT` | query-service | `8080` | listen port |
| `QUERY_AUTH_TOKEN` | query-service, mcp-server | *(empty = no auth)* | static bearer token |
| `QUERY_SERVICE_URL` | mcp-server | `http://localhost:8080` | query-service base URL |
| `QUERY_TIMEOUT` | mcp-server | `30s` | per-request timeout |

### REST endpoints

All read-only, all JSON. With `QUERY_AUTH_TOKEN` set, every endpoint except
`/health` requires `Authorization: Bearer <token>`.

| Endpoint | Answers |
|---|---|
| `GET /search?q=` | fuzzy symbol search, ranked exact > prefix > contains |
| `GET /symbol/{name}` | every definition of a symbol, all repos |
| `GET /callers/{symbol}` · `/callees/{symbol}` | direct call edges |
| `GET /blast-radius/{symbol}?depth=` | reachable nodes with distance |
| `GET /path?src=&dst=` | shortest path between two symbols |
| `GET /overview/{repo}` | structured onboarding snapshot for one repo |
| `GET /dependencies/{repo}?scope=` · `/dependents/{dep}` | manifest dependency edges |
| `GET /routes?method=&path=&repo=` | HTTP API inventory |
| `GET /kafka/topic/{name}` | producers/consumers of a topic, all repos |
| `GET /sql/object?schema=&name=` | SQL object + reads/writes/dependencies |
| `GET /glue/jobs?source=&target=` | Glue jobs by source/target table |

## Runbook

### Adding a repository

1. Add an entry to `config/repos.yaml`:
   ```yaml
   - name: payments-service        # unique; filesystem+graph identifier
     url: git@github.com:angel-one/payments-service.git
     branch: main
   ```
2. `go run ./cmd/indexer --repo payments-service` to index it immediately, or
   wait for the next continuous cycle. No other repos are re-indexed.

Git auth is whatever the local `git` is configured for (SSH keys, credential
helper). The indexer disables interactive prompts, so a missing credential
fails fast instead of hanging.

### Rotating the auth token

Set the new `QUERY_AUTH_TOKEN` on query-service and restart, then update
mcp-server / clients. There is one shared token; per-user tokens mapped to
repo permissions are the planned replacement (`api.WithAuth` is the seam).

### Forcing a re-index

`go run ./cmd/indexer --repo <name> --force` re-runs the pipeline even when
HEAD hasn't moved (e.g. after an extractor fix or a graph wipe).

### Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `another indexer is already running against workdir` | a second indexer holds the flock; stop it. (Windows: locking is a no-op — don't run two.) |
| `clone target ... is non-empty and not a git repo` | interrupted clone; delete `workdir/repos/<name>` and re-run |
| `remote mismatch at ...` | repo URL changed in config; delete the clone to re-clone from the new URL |
| `state file ... was malformed; quarantined` | state was recovered automatically; affected repos re-index on next cycle |
| indexer summary shows `[MISMATCH: N in graph]` | node_key collisions — imported count ≠ Neo4j count; investigate before trusting results |
| stale results after extractor changes | re-run with `--force`; old shared nodes are reaped only when orphaned |

### Operational invariants

- **One indexer per workdir.** Enforced by flock on unix; convention on Windows.
- `workdir/` is disposable *except* `state.json` (and even that self-heals —
  deleting it just forces a full re-index).
- The importer is idempotent: re-importing the same commit is a no-op upsert.

## Tests

```
go test ./...
```

Extractors are covered by table tests over fixture files (`internal/extract/*/
*_test.go`); the import pipeline is tested against a fake Neo4j client
(`internal/importer/importer_test.go`). No test needs a live database.

## Known limitations

- **Search is lexical**, not semantic: `/search` matches substrings of symbol
  names. "Where is JWT refresh handled?" only works if a symbol contains those
  words. An embedding-backed semantic layer is the next major feature.
- Extractors are heuristic (regex-level). Edges they emit carry
  `confidence: INFERRED` — treat them as leads, not proofs.
- Access control is a single shared bearer token, not per-user repo
  permissions.
- Windows hosts don't get workdir locking.
