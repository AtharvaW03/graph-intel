package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"graph-platform/internal/mcp"
)

func main() {
	// stdio is reserved for the MCP transport; logs go to stderr.
	log.SetOutput(os.Stderr)

	baseURL := envOr("QUERY_SERVICE_URL", "http://localhost:8080")

	timeout := 30 * time.Second
	if t := os.Getenv("QUERY_TIMEOUT"); t != "" {
		parsed, err := time.ParseDuration(t)
		if err != nil {
			log.Fatalf("invalid QUERY_TIMEOUT %q: %v", t, err)
		}
		timeout = parsed
	}

	client := mcp.NewQueryClient(baseURL, timeout, os.Getenv("QUERY_AUTH_TOKEN"))
	server := mcp.NewServer(client)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("mcp-server starting (query_service=%s, timeout=%s)", baseURL, timeout)
	if err := server.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
