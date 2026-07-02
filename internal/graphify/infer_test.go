package graphify

import (
	"strings"
	"testing"
)

func TestInferLabel(t *testing.T) {
	cases := []struct {
		name string
		node Node
		want string
	}{
		{"explicit type wins", Node{Type: "kafka_topic", Label: "orders.md"}, "KafkaTopic"},
		{"glue schedule type", Node{Type: "glue_schedule", Label: "cron(0 2 * * ? *)"}, "GlueSchedule"},
		{"kind file", Node{Metadata: map[string]any{"kind": "file"}, Label: "x"}, "File"},
		{"kind bash function", Node{Metadata: map[string]any{"kind": "bash_function"}, Label: "x"}, "Function"},
		{"markdown source", Node{SourceFile: "docs/README.md", Label: "Intro"}, "DocSection"},
		{"file extension label", Node{Label: "main.go"}, "File"},
		{"function parens", Node{Label: "processPayment()"}, "Function"},
		{"class capitalized", Node{Label: "UserService"}, "Class"},
		{"symbol fallback", Node{Label: "some symbol"}, "Symbol"},
	}
	for _, c := range cases {
		if got := InferLabel(c.node); got != c.want {
			t.Errorf("%s: InferLabel = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestMapRelation(t *testing.T) {
	if r, ok := MapRelation("produces"); !ok || r != "PRODUCES" {
		t.Errorf("produces -> %q,%v", r, ok)
	}
	if _, ok := MapRelation("made_up_relation"); ok {
		t.Error("unknown relation should not map")
	}
}

func TestStableKeyPlatformNodesUseGlobalID(t *testing.T) {
	topic := Node{ID: "topic::trade_executed", Label: "trade_executed", Origin: "platform"}
	keyA := StableKey("repo-a", topic)
	keyB := StableKey("repo-b", topic)
	if keyA != keyB {
		t.Errorf("shared topic split across repos: %q vs %q", keyA, keyB)
	}
	if !strings.HasPrefix(keyA, "platform::") {
		t.Errorf("platform key = %q, want platform:: prefix", keyA)
	}

	// Repo hubs must unify too: the phantom hub emitted by a dependent repo
	// has to land on the real hub's node.
	hub := Node{ID: "repo::auth-service", Label: "auth-service", Origin: "platform"}
	if StableKey("auth-service", hub) != StableKey("consumer-repo", hub) {
		t.Error("repo hub key differs between owning and referencing repo")
	}
}

func TestStableKeyASTNodesAreRepoScoped(t *testing.T) {
	n := Node{ID: "func::main", Label: "main()", SourceFile: "cmd/main.go"}
	if StableKey("repo-a", n) == StableKey("repo-b", n) {
		t.Error("AST node keys must be repo-scoped")
	}
	// Same file+label but different graphify IDs must stay distinct
	// (the redis command.go .String() collision case).
	a := Node{ID: "id-1", Label: ".String()", SourceFile: "command.go"}
	b := Node{ID: "id-2", Label: ".String()", SourceFile: "command.go"}
	if StableKey("r", a) == StableKey("r", b) {
		t.Error("distinct graphify IDs collapsed into one key")
	}
}

func TestIsShared(t *testing.T) {
	cases := []struct {
		node Node
		want bool
	}{
		{Node{ID: "topic::x", Origin: "platform"}, true},
		{Node{ID: "pkg::go::github.com/x", Origin: "platform"}, true},
		{Node{ID: "sql::sql_table::dbo.trades", Origin: "platform"}, true},
		{Node{ID: "repo::auth-service", Origin: "platform"}, true},
		{Node{ID: "route::svc::GET::/x::main.go", Origin: "platform"}, false},
		{Node{ID: "glue::job::svc::daily", Origin: "platform"}, false},
		{Node{ID: "topic::x", Origin: "ast"}, false}, // only platform nodes share
	}
	for _, c := range cases {
		if got := IsShared(c.node); got != c.want {
			t.Errorf("IsShared(%s/%s) = %v, want %v", c.node.Origin, c.node.ID, got, c.want)
		}
	}
}
