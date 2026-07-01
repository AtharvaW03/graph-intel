package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"graph-platform/internal/api"
	"graph-platform/internal/neo4j"
	"graph-platform/internal/query"
)

func main() {
	password := os.Getenv("NEO4J_PASSWORD")
	if password == "" {
		log.Fatal("NEO4J_PASSWORD not set")
	}

	uri := envOr("NEO4J_URI", "neo4j://127.0.0.1:7687")
	user := envOr("NEO4J_USER", "neo4j")
	port := envOr("QUERY_PORT", "8080")

	client, err := neo4j.New(uri, user, password)
	if err != nil {
		log.Fatalf("neo4j connect: %v", err)
	}
	defer client.Close()

	svc := query.NewService(client)
	server := api.NewServer(svc)

	addr := ":" + port
	log.Printf("query-service listening on %s", addr)

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           server.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	if err := httpServer.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
