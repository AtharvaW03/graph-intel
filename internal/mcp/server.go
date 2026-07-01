package mcp

import (
	"context"
	"encoding/json"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	serverName    = "graph-platform"
	serverVersion = "0.1.0"
)

// Server is a stateless MCP protocol adapter. It exposes graph-query tools
// over stdio and translates each tool call into one HTTP request against the
// Query Service. It owns no graph logic.
type Server struct {
	sdk    *sdk.Server
	client *QueryClient
}

// NewServer wires the six query tools to the supplied QueryClient.
func NewServer(client *QueryClient) *Server {
	s := sdk.NewServer(&sdk.Implementation{
		Name:    serverName,
		Version: serverVersion,
	}, nil)

	srv := &Server{sdk: s, client: client}
	srv.registerTools()
	return srv
}

// Run drives the server over stdio until ctx is canceled or the transport
// closes.
func (s *Server) Run(ctx context.Context) error {
	return s.sdk.Run(ctx, &sdk.StdioTransport{})
}

// -------- tool input shapes --------

type RepositoryOverviewInput struct {
	Repo string `json:"repo" jsonschema:"the repository to summarize (e.g. 'go-kafka-example')"`
}

type SearchCodeInput struct {
	Query string `json:"query" jsonschema:"search text — partial, case-insensitive match against symbol name and norm_name across all imported repositories"`
}

type FindSymbolInput struct {
	Name string `json:"name" jsonschema:"exact symbol name to look up across every imported repository"`
}

type FindCallersInput struct {
	Symbol string `json:"symbol" jsonschema:"function or method to find callers of (functions are typically suffixed with parentheses, e.g. UserService())"`
}

type FindCalleesInput struct {
	Symbol string `json:"symbol" jsonschema:"function or method whose outgoing CALLS edges to list"`
}

type BlastRadiusInput struct {
	Symbol string `json:"symbol" jsonschema:"symbol whose downstream impact (outgoing reachability) to compute"`
	Depth  int    `json:"depth,omitempty" jsonschema:"max traversal depth; default 3, capped at 10"`
}

type ShortestPathInput struct {
	Source string `json:"source" jsonschema:"starting symbol"`
	Target string `json:"target" jsonschema:"destination symbol"`
}

type FindDependenciesInput struct {
	Repo  string `json:"repo" jsonschema:"the repository whose declared dependencies to list"`
	Scope string `json:"scope,omitempty" jsonschema:"optional scope filter — runtime, dev, indirect, peer, optional, test, build"`
}

type FindDependentsInput struct {
	Dep string `json:"dep" jsonschema:"the package or repository name to find dependents of (e.g. 'github.com/foo/bar' or 'auth-service')"`
}

type FindRoutesInput struct {
	Method string `json:"method,omitempty" jsonschema:"HTTP method filter (GET, POST, PUT, DELETE, ...). Empty = any."`
	Path   string `json:"path,omitempty" jsonschema:"case-insensitive substring of the route path. Empty = any."`
	Repo   string `json:"repo,omitempty" jsonschema:"limit to a single repository. Empty = all."`
}

type FindKafkaTopicInput struct {
	Topic string `json:"topic" jsonschema:"exact Kafka topic name"`
}

type FindSQLObjectInput struct {
	Schema string `json:"schema,omitempty" jsonschema:"optional schema scope (e.g. 'dbo'). Empty matches every schema."`
	Name   string `json:"name" jsonschema:"object name to look up — schemas, tables, views, procedures, triggers, functions"`
}

type FindGlueJobsInput struct {
	Source string `json:"source,omitempty" jsonschema:"filter to jobs that READ from this Glue/SQL table (schema.table). Empty = any."`
	Target string `json:"target,omitempty" jsonschema:"filter to jobs that WRITE to this Glue/SQL table (schema.table). Empty = any."`
}

// -------- registration --------

func (s *Server) registerTools() {
	readOnly := &sdk.ToolAnnotations{ReadOnlyHint: true}

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "repository_overview",
		Description: "PRIMARY ONBOARDING ENTRY POINT for a repository. Returns a single structured architectural summary — metadata (node/relationship counts, languages), Graphify communities, entry points (mains/servers/bootstrap), major modules with sizes, HTTP APIs, Kafka topics/producers/consumers, SQL objects, dependencies, highest-degree hub components, and a suggested reading order. Call this ONCE before making many search_code/find_routes/find_dependencies/find_symbol calls; follow up with those targeted tools only for specifics.",
		Annotations: readOnly,
	}, s.handleRepositoryOverview)

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "search_code",
		Description: "Search code symbols by partial name across all imported repositories. Returns up to 100 matches ordered by relevance (exact > prefix > contains).",
		Annotations: readOnly,
	}, s.handleSearchCode)

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "find_symbol",
		Description: "Find every occurrence of an exact symbol name across all repositories. Use search_code for partial matches.",
		Annotations: readOnly,
	}, s.handleFindSymbol)

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "find_callers",
		Description: "List every function that directly CALLS the supplied symbol.",
		Annotations: readOnly,
	}, s.handleFindCallers)

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "find_callees",
		Description: "List every function the supplied symbol directly CALLS.",
		Annotations: readOnly,
	}, s.handleFindCallees)

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "blast_radius",
		Description: "Traverse outgoing relationships from the supplied symbol up to N hops and return reachable nodes with their minimum distance.",
		Annotations: readOnly,
	}, s.handleBlastRadius)

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "shortest_path",
		Description: "Return one shortest undirected path between two symbols, as an ordered list of nodes plus the relationship type used to enter each.",
		Annotations: readOnly,
	}, s.handleShortestPath)

	// Extractor-backed tools

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "find_dependencies",
		Description: "List every package the supplied repository declares as a dependency, plus inferred cross-repo (DEPENDS_ON_REPO) targets. Optional scope filter narrows to runtime, dev, indirect, etc.",
		Annotations: readOnly,
	}, s.handleFindDependencies)

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "find_dependents",
		Description: "List every repository whose manifest declares a dependency on the supplied package or repository name. Use for blast-radius questions across repos — \"who depends on auth-service?\".",
		Annotations: readOnly,
	}, s.handleFindDependents)

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "find_routes",
		Description: "Search the HTTP API inventory across all repositories. Filter by method, path substring, and/or repo. Returns repo, method, path, handler, source file, source line.",
		Annotations: readOnly,
	}, s.handleFindRoutes)

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "find_kafka_topic",
		Description: "Look up one Kafka topic by exact name and return the repositories that produce to and consume from it.",
		Annotations: readOnly,
	}, s.handleFindKafkaTopic)

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "find_sql_object",
		Description: "Look up a Microsoft SQL Server object (schema, table, view, procedure, trigger, function) and return the tables it reads, writes, depends on, and (for triggers) the table it fires on. Pass schema='' to match across schemas.",
		Annotations: readOnly,
	}, s.handleFindSQLObject)

	sdk.AddTool(s.sdk, &sdk.Tool{
		Name:        "find_glue_jobs",
		Description: "Search AWS Glue jobs by source or destination table. Returns the job, its repository, script, schedule, and the full source/destination table lists.",
		Annotations: readOnly,
	}, s.handleFindGlueJobs)
}

// -------- handlers --------

func (s *Server) handleRepositoryOverview(ctx context.Context, _ *sdk.CallToolRequest, in RepositoryOverviewInput) (*sdk.CallToolResult, any, error) {
	if in.Repo == "" {
		return errResult("repo must not be empty"), nil, nil
	}
	body, err := s.client.RepositoryOverview(ctx, in.Repo)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleSearchCode(ctx context.Context, _ *sdk.CallToolRequest, in SearchCodeInput) (*sdk.CallToolResult, any, error) {
	if in.Query == "" {
		return errResult("query must not be empty"), nil, nil
	}
	body, err := s.client.Search(ctx, in.Query)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleFindSymbol(ctx context.Context, _ *sdk.CallToolRequest, in FindSymbolInput) (*sdk.CallToolResult, any, error) {
	if in.Name == "" {
		return errResult("name must not be empty"), nil, nil
	}
	body, err := s.client.FindSymbol(ctx, in.Name)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleFindCallers(ctx context.Context, _ *sdk.CallToolRequest, in FindCallersInput) (*sdk.CallToolResult, any, error) {
	if in.Symbol == "" {
		return errResult("symbol must not be empty"), nil, nil
	}
	body, err := s.client.FindCallers(ctx, in.Symbol)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleFindCallees(ctx context.Context, _ *sdk.CallToolRequest, in FindCalleesInput) (*sdk.CallToolResult, any, error) {
	if in.Symbol == "" {
		return errResult("symbol must not be empty"), nil, nil
	}
	body, err := s.client.FindCallees(ctx, in.Symbol)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleBlastRadius(ctx context.Context, _ *sdk.CallToolRequest, in BlastRadiusInput) (*sdk.CallToolResult, any, error) {
	if in.Symbol == "" {
		return errResult("symbol must not be empty"), nil, nil
	}
	body, err := s.client.BlastRadius(ctx, in.Symbol, in.Depth)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleShortestPath(ctx context.Context, _ *sdk.CallToolRequest, in ShortestPathInput) (*sdk.CallToolResult, any, error) {
	if in.Source == "" || in.Target == "" {
		return errResult("source and target must not be empty"), nil, nil
	}
	body, err := s.client.ShortestPath(ctx, in.Source, in.Target)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleFindDependencies(ctx context.Context, _ *sdk.CallToolRequest, in FindDependenciesInput) (*sdk.CallToolResult, any, error) {
	if in.Repo == "" {
		return errResult("repo must not be empty"), nil, nil
	}
	body, err := s.client.FindDependencies(ctx, in.Repo, in.Scope)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleFindDependents(ctx context.Context, _ *sdk.CallToolRequest, in FindDependentsInput) (*sdk.CallToolResult, any, error) {
	if in.Dep == "" {
		return errResult("dep must not be empty"), nil, nil
	}
	body, err := s.client.FindDependents(ctx, in.Dep)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleFindRoutes(ctx context.Context, _ *sdk.CallToolRequest, in FindRoutesInput) (*sdk.CallToolResult, any, error) {
	body, err := s.client.FindRoutes(ctx, in.Method, in.Path, in.Repo)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleFindKafkaTopic(ctx context.Context, _ *sdk.CallToolRequest, in FindKafkaTopicInput) (*sdk.CallToolResult, any, error) {
	if in.Topic == "" {
		return errResult("topic must not be empty"), nil, nil
	}
	body, err := s.client.FindKafkaTopic(ctx, in.Topic)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleFindSQLObject(ctx context.Context, _ *sdk.CallToolRequest, in FindSQLObjectInput) (*sdk.CallToolResult, any, error) {
	if in.Name == "" {
		return errResult("name must not be empty"), nil, nil
	}
	body, err := s.client.FindSQLObject(ctx, in.Schema, in.Name)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

func (s *Server) handleFindGlueJobs(ctx context.Context, _ *sdk.CallToolRequest, in FindGlueJobsInput) (*sdk.CallToolResult, any, error) {
	body, err := s.client.FindGlueJobs(ctx, in.Source, in.Target)
	if err != nil {
		return errResult(err.Error()), nil, nil
	}
	return jsonResult(body), nil, nil
}

// -------- result helpers --------

func jsonResult(body json.RawMessage) *sdk.CallToolResult {
	return &sdk.CallToolResult{
		Content: []sdk.Content{&sdk.TextContent{Text: string(body)}},
	}
}

func errResult(msg string) *sdk.CallToolResult {
	return &sdk.CallToolResult{
		IsError: true,
		Content: []sdk.Content{&sdk.TextContent{Text: msg}},
	}
}
