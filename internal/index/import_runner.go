package index

import (
	"context"

	"graph-platform/internal/importer"
)

// DefaultImportRunner adapts internal/importer.Run to the orchestrator's
// ImportRunner interface. It exists so future variants (mock, alternative
// graph sink) can be swapped without changing the orchestrator.
type DefaultImportRunner struct {
	Client importer.Neo4jClient
}

func NewDefaultImportRunner(client importer.Neo4jClient) *DefaultImportRunner {
	return &DefaultImportRunner{Client: client}
}

func (r *DefaultImportRunner) Run(ctx context.Context, repo, commit, graphPath string) (*importer.Summary, error) {
	return importer.Run(ctx, r.Client, importer.Options{Repo: repo, Commit: commit, GraphPath: graphPath})
}
