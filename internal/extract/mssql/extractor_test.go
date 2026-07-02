package mssql

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

const fixtureSQL = `CREATE SCHEMA trade
GO

CREATE TABLE trade.orders (
    id INT PRIMARY KEY,
    qty INT
)
GO

CREATE PROCEDURE trade.usp_cleanup AS
BEGIN
    DELETE FROM trade.orders WHERE qty = 0
END
GO

CREATE OR ALTER PROCEDURE trade.usp_report AS
BEGIN
    SELECT o.id FROM trade.orders o
    JOIN dbo.users u ON u.id = o.id
    INSERT INTO trade.audit (id) VALUES (1)
END
GO

CREATE VIEW trade.v_open AS
    SELECT id FROM trade.orders WHERE qty > 0
GO

CREATE TRIGGER trade.trg_orders ON trade.orders AFTER INSERT AS
BEGIN
    INSERT INTO trade.audit (id) SELECT id FROM inserted
END
`

type sqlGraph struct {
	nodes map[string]string          // id -> type
	edges map[string]map[string]bool // relation -> "source|target"
}

func runExtract(t *testing.T) sqlGraph {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "schema.sql"), []byte(fixtureSQL), 0o644); err != nil {
		t.Fatal(err)
	}
	frag, err := New().Extract(context.Background(), dir, "db-repo")
	if err != nil {
		t.Fatal(err)
	}
	g := sqlGraph{nodes: map[string]string{}, edges: map[string]map[string]bool{}}
	for _, n := range frag.Nodes {
		g.nodes[n.ID] = n.Type
	}
	for _, e := range frag.Edges {
		if g.edges[e.Relation] == nil {
			g.edges[e.Relation] = map[string]bool{}
		}
		g.edges[e.Relation][e.Source+"|"+e.Target] = true
	}
	return g
}

func TestObjectsExtracted(t *testing.T) {
	g := runExtract(t)
	wantNodes := map[string]string{
		"sql::schema::trade":                  "sql_schema",
		"sql::sql_table::trade.orders":        "sql_table",
		"sql::sql_procedure::trade.usp_cleanup": "sql_procedure",
		"sql::sql_procedure::trade.usp_report":  "sql_procedure",
		"sql::sql_view::trade.v_open":         "sql_view",
		"sql::sql_trigger::trade.trg_orders":  "sql_trigger",
	}
	for id, typ := range wantNodes {
		if g.nodes[id] != typ {
			t.Errorf("node %s: type = %q, want %q", id, g.nodes[id], typ)
		}
	}
}

// TestDeleteIsWriteNotRead is the regression test for the DELETE FROM
// double-count: a proc that only deletes from a table must get writes_table
// but NOT reads_table for it.
func TestDeleteIsWriteNotRead(t *testing.T) {
	g := runExtract(t)
	cleanup := "sql::sql_procedure::trade.usp_cleanup"
	orders := "sql::sql_table::trade.orders"
	if !g.edges["writes_table"][cleanup+"|"+orders] {
		t.Error("DELETE FROM did not produce writes_table edge")
	}
	if g.edges["reads_table"][cleanup+"|"+orders] {
		t.Error("DELETE FROM incorrectly counted as reads_table")
	}
}

func TestProcReadsAndWrites(t *testing.T) {
	g := runExtract(t)
	report := "sql::sql_procedure::trade.usp_report"
	for _, want := range []string{
		report + "|sql::sql_table::trade.orders",
		report + "|sql::sql_table::dbo.users",
	} {
		if !g.edges["reads_table"][want] {
			t.Errorf("missing reads_table %s; got %v", want, g.edges["reads_table"])
		}
	}
	if !g.edges["writes_table"][report+"|sql::sql_table::trade.audit"] {
		t.Errorf("missing writes_table to trade.audit; got %v", g.edges["writes_table"])
	}
}

func TestViewAndTriggerEdges(t *testing.T) {
	g := runExtract(t)
	if !g.edges["depends_on_object"]["sql::sql_view::trade.v_open|sql::sql_table::trade.orders"] {
		t.Errorf("view dependency missing; got %v", g.edges["depends_on_object"])
	}
	if !g.edges["triggers_on"]["sql::sql_trigger::trade.trg_orders|sql::sql_table::trade.orders"] {
		t.Errorf("trigger target missing; got %v", g.edges["triggers_on"])
	}
	if !g.edges["in_schema"]["sql::sql_table::trade.orders|sql::schema::trade"] {
		t.Errorf("in_schema missing; got %v", g.edges["in_schema"])
	}
}
