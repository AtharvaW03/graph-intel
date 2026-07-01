package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"graph-platform/internal/extract"
	"graph-platform/internal/extract/deps"
	"graph-platform/internal/extract/glue"
	"graph-platform/internal/extract/httpapi"
	"graph-platform/internal/extract/kafka"
	"graph-platform/internal/extract/mssql"
	"graph-platform/internal/index"
	"graph-platform/internal/neo4j"
)

func main() {
	configPath := flag.String("config", "config/repos.yaml", "path to indexer YAML config")
	workDir := flag.String("workdir", "workdir", "directory for clones, graphify outputs, and state.json")
	repos := flag.String("repo", "", "comma-separated repository names to index (default: all)")
	all := flag.Bool("all", false, "explicit all-repositories mode; mutually exclusive with --repo")
	force := flag.Bool("force", false, "re-index even if HEAD matches the previously-indexed commit")
	interval := flag.Duration("interval", 0, "if > 0, run continuously every interval (e.g. 15m); otherwise one-shot")
	flag.Parse()

	logger := log.New(os.Stderr, "", log.LstdFlags|log.Lmsgprefix)

	if *all && strings.TrimSpace(*repos) != "" {
		logger.Fatal("--all and --repo are mutually exclusive")
	}

	cfg, err := index.LoadConfig(*configPath)
	if err != nil {
		logger.Fatal(err)
	}

	absWorkDir, err := filepath.Abs(*workDir)
	if err != nil {
		logger.Fatalf("resolve workdir: %v", err)
	}
	if err := os.MkdirAll(absWorkDir, 0o755); err != nil {
		logger.Fatalf("create workdir: %v", err)
	}

	// Acquire local resources first (lock, state) so config and disk errors
	// surface immediately. Neo4j connect — the only network dependency —
	// comes after, so a remote outage doesn't mask a typo'd config.
	lock, err := index.LockWorkDir(absWorkDir)
	if err != nil {
		logger.Fatal(err)
	}
	if lock != nil {
		defer lock.Close()
	}

	store, err := index.LoadJSONStateStore(filepath.Join(absWorkDir, "state.json"))
	if err != nil {
		logger.Fatalf("load state: %v", err)
	}

	password := os.Getenv("NEO4J_PASSWORD")
	if password == "" {
		logger.Fatal("NEO4J_PASSWORD not set")
	}
	uri := envOr("NEO4J_URI", "neo4j://127.0.0.1:7687")
	user := envOr("NEO4J_USER", "neo4j")

	client, err := neo4j.New(uri, user, password)
	if err != nil {
		logger.Fatalf("neo4j connect: %v", err)
	}
	defer client.Close()
	logger.Printf("connected to neo4j (%s)", uri)

	orch := &index.Orchestrator{
		Source:        index.NewConfigJobSource(cfg),
		Syncer:        index.NewGitSyncer(cfg.Git),
		Graphify:      index.NewExecGraphifier(cfg.Graphify, os.Stderr),
		Importer:      index.NewDefaultImportRunner(client),
		Store:         store,
		WorkDir:       absWorkDir,
		Log:           logger,
		HealthChecker: client,
		Extractors:    buildExtractorRunner(cfg, logger),
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := index.Options{
		Names: splitCSV(*repos),
		Force: *force,
	}

	if *interval > 0 {
		sched, err := index.NewIntervalScheduler(*interval)
		if err != nil {
			logger.Fatal(err)
		}
		logger.Printf("continuous mode: every %s", *interval)
		if err := orch.RunForever(ctx, opts, sched); err != nil && ctx.Err() == nil {
			logger.Fatal(err)
		}
		return
	}

	summary, err := orch.RunOnce(ctx, opts)
	if err != nil {
		logger.Fatal(err)
	}
	orch.LogSummary(summary)

	_, _, _, failed := summary.Counts()
	if failed > 0 {
		os.Exit(1)
	}
}

func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// buildExtractorRunner constructs the extract.Runner whose Extractors are the
// platform's domain extractors (deps, http_api, kafka, mssql, glue). Each
// extractor can be disabled via cfg.Extractors; the runner inherits the
// per-cycle context from the orchestrator. Returns nil if all extractors are
// disabled, which is a meaningful operator choice (the indexer reverts to
// pure-Graphify enrichment).
func buildExtractorRunner(cfg *index.Config, logger *log.Logger) *extract.Runner {
	var exs []extract.Extractor
	if cfg.Extractors.DepsEnabled() {
		exs = append(exs, deps.New(cfg.Org.Prefixes))
	}
	if cfg.Extractors.HTTPAPIEnabled() {
		exs = append(exs, httpapi.New())
	}
	if cfg.Extractors.KafkaEnabled() {
		exs = append(exs, kafka.New())
	}
	if cfg.Extractors.MSSQLEnabled() {
		exs = append(exs, mssql.New())
	}
	if cfg.Extractors.GlueEnabled() {
		exs = append(exs, glue.New())
	}
	if len(exs) == 0 {
		return nil
	}
	return &extract.Runner{
		Extractors:  exs,
		Log:         logger,
		MaxParallel: cfg.Extractors.MaxParallel,
	}
}
