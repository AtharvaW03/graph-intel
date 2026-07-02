// Package mssql extracts Microsoft SQL Server schema objects from a repo's
// .sql files: schemas, tables, views, stored procedures, triggers, and
// functions (scalar + table-valued). Each object becomes a typed node and
// the relationships between them — view DEPENDS_ON table, procedure
// READS_TABLE / WRITES_TABLE / triggers TRIGGERS_ON table — are inferred
// from the bodies of CREATE/ALTER statements.
//
// The extractor is regex-based; it does NOT parse T-SQL grammar. It is
// sufficient for inventory and dependency graphs but not for query analysis.
// Confidence on inferred dependency edges is INFERRED; structural edges
// (object → schema) are EXTRACTED.
package mssql

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"graph-platform/internal/extract"
)

type Extractor struct {
	MaxFileBytes int64
}

func New() *Extractor { return &Extractor{MaxFileBytes: 8 * 1024 * 1024} }

func (e *Extractor) Name() string { return "mssql" }

// objectKind enumerates the T-SQL object types we surface.
type objectKind string

const (
	kindSchema    objectKind = "sql_schema"
	kindTable     objectKind = "sql_table"
	kindView      objectKind = "sql_view"
	kindProcedure objectKind = "sql_procedure"
	kindTrigger   objectKind = "sql_trigger"
	kindFunction  objectKind = "sql_function"
)

var (
	createSchemaRe   = regexp.MustCompile(`(?i)CREATE\s+SCHEMA\s+\[?([A-Za-z0-9_]+)\]?`)
	createTableRe    = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?`)
	createViewRe     = regexp.MustCompile(`(?i)CREATE\s+(?:OR\s+ALTER\s+)?VIEW\s+(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?`)
	createProcedureRe = regexp.MustCompile(`(?i)CREATE\s+(?:OR\s+ALTER\s+)?PROC(?:EDURE)?\s+(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?`)
	createTriggerRe  = regexp.MustCompile(`(?i)CREATE\s+(?:OR\s+ALTER\s+)?TRIGGER\s+(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?[\s\S]+?ON\s+(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?`)
	createFunctionRe = regexp.MustCompile(`(?i)CREATE\s+(?:OR\s+ALTER\s+)?FUNCTION\s+(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?`)

	// Body scans for cross-object references.
	bodySelectRe = regexp.MustCompile(`(?is)\bFROM\s+(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?`)
	bodyJoinRe   = regexp.MustCompile(`(?is)\bJOIN\s+(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?`)
	bodyInsertRe = regexp.MustCompile(`(?is)\bINSERT\s+INTO\s+(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?`)
	bodyUpdateRe = regexp.MustCompile(`(?is)\bUPDATE\s+(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?`)
	bodyDeleteRe = regexp.MustCompile(`(?is)\bDELETE\s+(?:FROM\s+)?(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?`)
	bodyExecRe   = regexp.MustCompile(`(?is)\bEXEC(?:UTE)?\s+(?:\[?([A-Za-z0-9_]+)\]?\.)?\[?([A-Za-z0-9_]+)\]?`)
)

// objectStmt is one CREATE statement parsed from a SQL file. We need the body
// substring after the header so the dependency-inference regexes only scan
// inside the object's definition (not the next object's).
type objectStmt struct {
	kind   objectKind
	schema string
	name   string
	body   string
	file   string
	line   int
	// extra metadata
	triggerTarget [2]string // (schema, table) for triggers
}

func (e *Extractor) Extract(ctx context.Context, repoPath, repoName string) (*extract.Fragment, error) {
	frag := extract.NewFragment(e.Name())

	walk := func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			if d != nil && d.IsDir() && shouldSkipDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".sql" {
			return nil
		}
		info, statErr := d.Info()
		if statErr != nil || info.Size() > e.MaxFileBytes {
			return nil
		}
		rel, _ := filepath.Rel(repoPath, path)
		rel = filepath.ToSlash(rel)

		body, rerr := os.ReadFile(path)
		if rerr != nil {
			frag.Warn(fmt.Sprintf("%s: %v", rel, rerr))
			return nil
		}
		stmts := splitObjects(string(body), rel)
		for _, s := range stmts {
			emit(frag, repoName, s)
		}
		return nil
	}

	if err := filepath.WalkDir(repoPath, walk); err != nil {
		return frag, fmt.Errorf("walk repo: %w", err)
	}
	return frag, nil
}

// splitObjects scans a SQL file for CREATE/ALTER statements, identifies the
// object kind, captures schema + name, and returns the body that follows up
// to the next CREATE/ALTER or end-of-file. T-SQL doesn't have a clean
// statement separator without proper parsing; "GO" batch separators and the
// next CREATE keyword serve as good-enough delimiters here.
func splitObjects(text, file string) []objectStmt {
	var out []objectStmt
	// Find all top-level CREATE positions in order.
	type hit struct {
		idx  int
		end  int
		stmt objectStmt
	}
	var hits []hit

	addHit := func(idx, end int, s objectStmt) { hits = append(hits, hit{idx: idx, end: end, stmt: s}) }

	for _, m := range createSchemaRe.FindAllStringSubmatchIndex(text, -1) {
		groups := createSchemaRe.FindStringSubmatch(text[m[0]:m[1]])
		addHit(m[0], m[1], objectStmt{kind: kindSchema, name: groups[1], file: file, line: lineNum(text, m[0])})
	}
	collect := func(re *regexp.Regexp, kind objectKind) {
		for _, m := range re.FindAllStringSubmatchIndex(text, -1) {
			groups := re.FindStringSubmatch(text[m[0]:m[1]])
			schema, name := "dbo", ""
			if len(groups) >= 3 {
				if groups[1] != "" {
					schema = groups[1]
				}
				name = groups[2]
			}
			addHit(m[0], m[1], objectStmt{
				kind:   kind,
				schema: schema,
				name:   name,
				file:   file,
				line:   lineNum(text, m[0]),
			})
		}
	}
	collect(createTableRe, kindTable)
	collect(createViewRe, kindView)
	collect(createProcedureRe, kindProcedure)
	collect(createFunctionRe, kindFunction)

	// Triggers carry an extra ON <target>.
	for _, m := range createTriggerRe.FindAllStringSubmatchIndex(text, -1) {
		groups := createTriggerRe.FindStringSubmatch(text[m[0]:m[1]])
		schema, name := "dbo", ""
		if groups[1] != "" {
			schema = groups[1]
		}
		name = groups[2]
		targetSchema, targetTable := "dbo", ""
		if groups[3] != "" {
			targetSchema = groups[3]
		}
		targetTable = groups[4]
		addHit(m[0], m[1], objectStmt{
			kind:          kindTrigger,
			schema:        schema,
			name:          name,
			file:          file,
			line:          lineNum(text, m[0]),
			triggerTarget: [2]string{targetSchema, targetTable},
		})
	}

	// Sort hits by start index ascending — establishes statement boundaries.
	sort.Slice(hits, func(i, j int) bool { return hits[i].idx < hits[j].idx })
	for i, h := range hits {
		bodyStart := h.end
		bodyEnd := len(text)
		if i+1 < len(hits) {
			bodyEnd = hits[i+1].idx
		}
		s := h.stmt
		s.body = text[bodyStart:bodyEnd]
		out = append(out, s)
	}
	return out
}

func emit(frag *extract.Fragment, repoName string, s objectStmt) {
	// For CREATE SCHEMA statements, the schema name is in s.name (the parser
	// sets it there); for all other CREATE statements, s.schema holds the
	// schema and we default to "dbo" when none is qualified.
	schema := s.schema
	if s.kind == kindSchema {
		schema = s.name
	}
	if schema == "" {
		schema = "dbo"
	}
	schemaNodeID := "sql::schema::" + schema
	frag.AddNode(extract.FragmentNode{
		ID:    schemaNodeID,
		Label: schema,
		Type:  string(kindSchema),
		Metadata: map[string]any{
			"discovered_in_repo": repoName,
		},
	})

	if s.kind == kindSchema {
		return
	}

	objectID := fmt.Sprintf("sql::%s::%s.%s", s.kind, schema, s.name)
	frag.AddNode(extract.FragmentNode{
		ID:             objectID,
		Label:          schema + "." + s.name,
		Type:           string(s.kind),
		SourceFile:     s.file,
		SourceLocation: fmt.Sprintf("L%d", s.line),
		Metadata: map[string]any{
			"schema":             schema,
			"object_name":        s.name,
			"discovered_in_repo": repoName,
		},
	})
	frag.AddEdge(extract.FragmentEdge{
		Source:         objectID,
		Target:         schemaNodeID,
		Relation:       "in_schema",
		Confidence:     extract.ConfidenceExtracted,
		SourceFile:     s.file,
		SourceLocation: fmt.Sprintf("L%d", s.line),
	})

	if s.kind == kindTrigger {
		targetID := fmt.Sprintf("sql::%s::%s.%s", kindTable, s.triggerTarget[0], s.triggerTarget[1])
		// Forward-declare the target table node so the edge has both endpoints
		// even if the table was defined in another file.
		frag.AddNode(extract.FragmentNode{
			ID:    targetID,
			Label: s.triggerTarget[0] + "." + s.triggerTarget[1],
			Type:  string(kindTable),
			Metadata: map[string]any{
				"schema":      s.triggerTarget[0],
				"object_name": s.triggerTarget[1],
			},
		})
		frag.AddEdge(extract.FragmentEdge{
			Source:         objectID,
			Target:         targetID,
			Relation:       "triggers_on",
			Confidence:     extract.ConfidenceExtracted,
			SourceFile:     s.file,
			SourceLocation: fmt.Sprintf("L%d", s.line),
		})
	}

	// Inferred read/write edges from the body.
	emitBodyRefs(frag, objectID, s)
}

// deleteFromRe locates the FROM keyword inside DELETE FROM statements so the
// reads scan (bodySelectRe, which matches every FROM) can skip them — a proc
// that only deletes from a table must not also get a reads_table edge for it.
var deleteFromRe = regexp.MustCompile(`(?is)\bDELETE\s+(FROM)\b`)

func emitBodyRefs(frag *extract.Fragment, sourceID string, s objectStmt) {
	deleteFroms := map[int]bool{}
	for _, m := range deleteFromRe.FindAllStringSubmatchIndex(s.body, -1) {
		deleteFroms[m[2]] = true // offset of the FROM keyword (group 1 start)
	}

	addRef := func(re *regexp.Regexp, relation string) {
		seen := map[string]bool{}
		for _, idx := range re.FindAllStringSubmatchIndex(s.body, -1) {
			if re == bodySelectRe && deleteFroms[idx[0]] {
				continue // this FROM belongs to a DELETE FROM, not a read
			}
			group := func(i int) string {
				if idx[2*i] < 0 {
					return ""
				}
				return s.body[idx[2*i]:idx[2*i+1]]
			}
			tSchema, tName := "dbo", group(2)
			if group(1) != "" {
				tSchema = group(1)
			}
			if tName == "" {
				continue
			}
			tid := fmt.Sprintf("sql::%s::%s.%s", kindTable, tSchema, tName)
			if seen[relation+":"+tid] {
				continue
			}
			seen[relation+":"+tid] = true
			frag.AddNode(extract.FragmentNode{
				ID:    tid,
				Label: tSchema + "." + tName,
				Type:  string(kindTable),
				Metadata: map[string]any{
					"schema":      tSchema,
					"object_name": tName,
				},
			})
			frag.AddEdge(extract.FragmentEdge{
				Source:     sourceID,
				Target:     tid,
				Relation:   relation,
				Confidence: extract.ConfidenceInferred,
				SourceFile: s.file,
			})
		}
	}
	switch s.kind {
	case kindView:
		addRef(bodySelectRe, "depends_on_object")
		addRef(bodyJoinRe, "depends_on_object")
	case kindProcedure, kindFunction:
		addRef(bodySelectRe, "reads_table")
		addRef(bodyJoinRe, "reads_table")
		addRef(bodyInsertRe, "writes_table")
		addRef(bodyUpdateRe, "writes_table")
		addRef(bodyDeleteRe, "writes_table")
		addRef(bodyExecRe, "depends_on_object")
	case kindTrigger:
		addRef(bodyInsertRe, "writes_table")
		addRef(bodyUpdateRe, "writes_table")
		addRef(bodyDeleteRe, "writes_table")
		addRef(bodySelectRe, "reads_table")
	}
}

func lineNum(text string, offset int) int {
	if offset > len(text) {
		offset = len(text)
	}
	return 1 + strings.Count(text[:offset], "\n")
}

func shouldSkipDir(name string) bool {
	switch name {
	case ".git", "node_modules", "vendor", "target", "build", "dist",
		"__pycache__", ".venv", "venv", ".tox", ".gradle", ".idea",
		".vs", "bin", "obj", ".mvn":
		return true
	}
	return false
}
