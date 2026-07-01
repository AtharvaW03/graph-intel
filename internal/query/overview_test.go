package query

import "testing"

func TestRoutePrefix(t *testing.T) {
	cases := map[string]string{
		"/":                "/",
		"/health":          "/health",
		"/api/v1/users":    "/api/v1",
		"/api/v1":          "/api/v1",
		"api/v1/users":     "/api/v1",
		"/users/{id}/edit": "/users/{id}",
	}
	for in, want := range cases {
		if got := routePrefix(in); got != want {
			t.Errorf("routePrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSummarizeRoutes(t *testing.T) {
	routes := []HTTPRoute{
		{Method: "GET", Path: "/api/v1/users"},
		{Method: "POST", Path: "/api/v1/users"},
		{Method: "GET", Path: "/api/v1/roles"},
		{Method: "GET", Path: "/health"},
	}
	got := summarizeRoutes(routes)
	if got.RouteCount != 4 {
		t.Fatalf("RouteCount = %d, want 4", got.RouteCount)
	}
	// Most common group is /api/v1 with 3 routes and should sort first.
	if len(got.Groups) == 0 || got.Groups[0].Prefix != "/api/v1" || got.Groups[0].Count != 3 {
		t.Fatalf("top group = %+v, want /api/v1 count 3", got.Groups)
	}
	// GET is the most common method.
	if got.Methods[0].Name != "GET" || got.Methods[0].Count != 3 {
		t.Errorf("top method = %+v, want GET 3", got.Methods[0])
	}
}

func TestSummarizeDependencies(t *testing.T) {
	deps := []DependencyEdge{
		{Name: "github.com/gin-gonic/gin", Ecosystem: "go"},
		{Name: "github.com/segmentio/kafka-go", Ecosystem: "go"},
		{Name: "lodash", Ecosystem: "npm"},
		{Name: "auth-service", Cross: true},
	}
	got := summarizeDependencies(deps)
	if len(got.InternalRepos) != 1 || got.InternalRepos[0] != "auth-service" {
		t.Errorf("InternalRepos = %v, want [auth-service]", got.InternalRepos)
	}
	if len(got.External) != 3 {
		t.Errorf("External count = %d, want 3", len(got.External))
	}
	if got.TopEcosystems[0].Name != "go" || got.TopEcosystems[0].Count != 2 {
		t.Errorf("top ecosystem = %+v, want go 2", got.TopEcosystems[0])
	}
}

func TestClassifyEntryPoint(t *testing.T) {
	cases := map[string]string{
		"main()":            "executable_main",
		"NewServer()":       "server",
		"ListenAndServe()":  "server",
		"StartWorker()":     "bootstrap",
		"bootstrap()":       "bootstrap",
	}
	for name, want := range cases {
		if got := classifyEntryPoint(name); got != want {
			t.Errorf("classifyEntryPoint(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestClassifyModuleAndNoise(t *testing.T) {
	if classifyModule("consumer/internal/config") != "infrastructure" {
		t.Error("config dir should be infrastructure")
	}
	if classifyModule("consumer/internal/postgres") != "infrastructure" {
		t.Error("postgres dir should be infrastructure")
	}
	if classifyModule("consumer/graphql_models") != "utility" {
		t.Error("graphql_models should be utility")
	}
	if classifyModule("consumer/internal/service") != "core" {
		t.Error("service dir should be core")
	}
	if !isNoiseModule("vendor/github.com/x") {
		t.Error("vendor should be noise")
	}
	if isNoiseModule("consumer/internal/service") {
		t.Error("normal dir should not be noise")
	}
}

func TestDominantDir(t *testing.T) {
	paths := []string{
		"consumer/daos/roles.go",
		"consumer/daos/users.go",
		"consumer/internal/config/config.go",
	}
	if got := dominantDir(paths); got != "consumer/daos" {
		t.Errorf("dominantDir = %q, want consumer/daos", got)
	}
}

func TestBuildReadingOrder(t *testing.T) {
	ov := &RepositoryOverview{
		EntryPoints: []EntryPoint{{Name: "main()", Kind: "executable_main", Path: "cmd/server/main.go"}},
		Modules: []ModuleInfo{
			{Package: "consumer/internal/service", NodeCount: 40},
			{Package: "consumer/internal/config", NodeCount: 20},
			{Package: "consumer/graphql_models", NodeCount: 15},
			{Package: "vendor/github.com/x", NodeCount: 500},
		},
		HTTPAPIs: HTTPAPISummary{Groups: []RouteGroup{{Prefix: "/api/v1", Count: 3}}},
		Kafka:    KafkaSummary{Topics: []string{"users"}},
	}
	steps := buildReadingOrder(ov)
	byCat := map[string][]string{}
	for _, s := range steps {
		byCat[s.Category] = s.Items
	}
	if _, ok := byCat["entry_points"]; !ok {
		t.Error("expected entry_points step")
	}
	if got := byCat["core_packages"]; len(got) == 0 || got[0] != "consumer/internal/service" {
		t.Errorf("core_packages = %v, want service first", got)
	}
	if got := byCat["infrastructure"]; len(got) == 0 || got[0] != "consumer/internal/config" {
		t.Errorf("infrastructure = %v, want config", got)
	}
	// vendor must be filtered out entirely.
	for _, items := range byCat {
		for _, it := range items {
			if it == "vendor/github.com/x" {
				t.Error("vendor leaked into reading order")
			}
		}
	}
}
