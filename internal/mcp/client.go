package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultTimeout = 30 * time.Second

// QueryClient is a thin HTTP client over the Query Service's REST API. It owns
// URL encoding, JSON decoding, and HTTP error surfacing; it never speaks
// Cypher or duplicates query logic.
type QueryClient struct {
	baseURL string
	token   string // bearer token; empty means the service runs unauthenticated
	http    *http.Client
}

// NewQueryClient returns a QueryClient pointing at baseURL with the given
// per-request timeout. timeout <= 0 falls back to defaultTimeout. token, if
// non-empty, is sent as an Authorization bearer header on every request.
func NewQueryClient(baseURL string, timeout time.Duration, token string) *QueryClient {
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	return &QueryClient{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		token:   token,
		http:    &http.Client{Timeout: timeout},
	}
}

// get issues GET baseURL+path?query and returns the response body verbatim.
// Non-2xx responses surface as descriptive errors that include the path,
// status code, and trimmed body so MCP clients see the underlying failure.
func (c *QueryClient) get(ctx context.Context, path string, query url.Values) (json.RawMessage, error) {
	full := c.baseURL + path
	if len(query) > 0 {
		full += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, full, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("call query service: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read query service response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("query service %s returned %d: %s",
			path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func (c *QueryClient) RepositoryOverview(ctx context.Context, repo string) (json.RawMessage, error) {
	return c.get(ctx, "/overview/"+url.PathEscape(repo), nil)
}

func (c *QueryClient) Search(ctx context.Context, q string) (json.RawMessage, error) {
	return c.get(ctx, "/search", url.Values{"q": {q}})
}

func (c *QueryClient) FindSymbol(ctx context.Context, name string) (json.RawMessage, error) {
	return c.get(ctx, "/symbol/"+url.PathEscape(name), nil)
}

func (c *QueryClient) FindCallers(ctx context.Context, symbol string) (json.RawMessage, error) {
	return c.get(ctx, "/callers/"+url.PathEscape(symbol), nil)
}

func (c *QueryClient) FindCallees(ctx context.Context, symbol string) (json.RawMessage, error) {
	return c.get(ctx, "/callees/"+url.PathEscape(symbol), nil)
}

func (c *QueryClient) BlastRadius(ctx context.Context, symbol string, depth int) (json.RawMessage, error) {
	q := url.Values{}
	if depth > 0 {
		q.Set("depth", strconv.Itoa(depth))
	}
	return c.get(ctx, "/blast-radius/"+url.PathEscape(symbol), q)
}

func (c *QueryClient) ShortestPath(ctx context.Context, source, target string) (json.RawMessage, error) {
	return c.get(ctx, "/path", url.Values{"src": {source}, "dst": {target}})
}

func (c *QueryClient) FindDependencies(ctx context.Context, repo, scope string) (json.RawMessage, error) {
	q := url.Values{}
	if scope != "" {
		q.Set("scope", scope)
	}
	return c.get(ctx, "/dependencies/"+url.PathEscape(repo), q)
}

func (c *QueryClient) FindDependents(ctx context.Context, dep string) (json.RawMessage, error) {
	return c.get(ctx, "/dependents/"+url.PathEscape(dep), nil)
}

func (c *QueryClient) FindRoutes(ctx context.Context, method, pathContains, repo string) (json.RawMessage, error) {
	q := url.Values{}
	if method != "" {
		q.Set("method", method)
	}
	if pathContains != "" {
		q.Set("path", pathContains)
	}
	if repo != "" {
		q.Set("repo", repo)
	}
	return c.get(ctx, "/routes", q)
}

func (c *QueryClient) FindKafkaTopic(ctx context.Context, topic string) (json.RawMessage, error) {
	return c.get(ctx, "/kafka/topic/"+url.PathEscape(topic), nil)
}

func (c *QueryClient) FindSQLObject(ctx context.Context, schema, name string) (json.RawMessage, error) {
	q := url.Values{"name": {name}}
	if schema != "" {
		q.Set("schema", schema)
	}
	return c.get(ctx, "/sql/object", q)
}

func (c *QueryClient) FindGlueJobs(ctx context.Context, source, target string) (json.RawMessage, error) {
	q := url.Values{}
	if source != "" {
		q.Set("source", source)
	}
	if target != "" {
		q.Set("target", target)
	}
	return c.get(ctx, "/glue/jobs", q)
}
