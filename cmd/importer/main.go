package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"graph-platform/internal/importer"
	"graph-platform/internal/neo4j"
)

func main() {
	repo := flag.String("repo", "golang-gin-realworld-example-app", "repository name to scope the graph")
	graphPath := flag.String("graph", "sample-data/graph.json", "path to graph.json")
	commit := flag.String("commit", "", "source commit SHA to stamp on imported nodes/edges; non-empty enables sweep of stale data from prior commits")
	flag.Parse()

	password := os.Getenv("NEO4J_PASSWORD")
	if password == "" {
		log.Fatal("NEO4J_PASSWORD not set")
	}

	client, err := neo4j.New("neo4j://127.0.0.1:7687", "neo4j", password)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	fmt.Println("Connected to Neo4j")

	ctx := context.Background()

	progress := func(stage string) {
		switch stage {
		case importer.StageLoad:
			// graph.json is parsed inside importer.Run; print after success
		case importer.StageConstraints:
			fmt.Println("Ensuring constraints/indexes")
		case importer.StageNodes:
			fmt.Println("Importing nodes")
		case importer.StageLinks:
			fmt.Println("Importing links")
		case importer.StageSweep:
			fmt.Println("Sweeping stale data")
		case importer.StageVerify:
			fmt.Println("Verifying Neo4j count")
		}
	}

	summary, err := importer.Run(ctx, client, importer.Options{
		Repo:      *repo,
		Commit:    *commit,
		GraphPath: *graphPath,
		Progress:  progress,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Loaded graph: %d nodes, %d links\n", summary.NodesTotal, summary.LinksTotal)
	fmt.Printf("Imported %d nodes\n", summary.NodesTotal)
	fmt.Printf("Imported %d links\n", summary.LinksImported)

	fmt.Println("\n--- Import Summary ---")
	fmt.Println("Nodes by label:")
	for _, l := range summary.SortedLabels() {
		fmt.Printf("  %-12s %d\n", l, summary.LabelCounts[l])
	}
	fmt.Println("Links by relation:")
	for _, r := range summary.SortedRelations() {
		fmt.Printf("  %-16s %d\n", r, summary.RelationCounts[r])
	}
	fmt.Printf("Skipped (unknown relation): %d\n", summary.SkippedUnknown)
	fmt.Printf("Skipped (dangling edge):    %d\n", summary.SkippedDangling)
	if summary.SkippedHyperedges > 0 {
		fmt.Printf("Skipped (hyperedges):       %d\n", summary.SkippedHyperedges)
	}
	if summary.Commit != "" {
		fmt.Printf("Swept (stale nodes):        %d\n", summary.NodesSwept)
		fmt.Printf("Swept (stale relations):    %d\n", summary.EdgesSwept)
	}
	fmt.Printf("Neo4j :Entity count (repo): %d\n", summary.NodesInGraph)
	if summary.NodesMismatch() {
		fmt.Printf("\nWARNING: input %d nodes but Neo4j holds %d for repo %q (delta %d).\n",
			summary.NodesTotal, summary.NodesInGraph, summary.Repo,
			summary.NodesTotal-summary.NodesInGraph)
		fmt.Println("This indicates silent data loss during import (e.g. node_key collisions). Investigate.")
	}
}
