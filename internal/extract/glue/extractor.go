// Package glue extracts AWS Glue job definitions from a repository. Glue jobs
// can be declared in several places — Terraform (aws_glue_job), CloudFormation
// (AWS::Glue::Job), CDK code, or directly created with boto3 calls. This
// extractor handles the three declarative shapes (Terraform HCL, CloudFormation
// YAML/JSON) plus a heuristic Python scan for `glueContext.create_dynamic_frame`
// and `glueContext.write_dynamic_frame` calls to infer source/destination
// tables when the job script lives in the same repo.
//
// Each discovered Glue job becomes a (:GlueJob {name}) node attached to its
// repository, with READS_SOURCE/WRITES_DESTINATION edges to inferred table
// nodes and an optional SCHEDULED edge to a schedule expression node when one
// is declared.
package glue

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"graph-platform/internal/extract"

	"gopkg.in/yaml.v3"
)

type Extractor struct {
	MaxFileBytes int64
}

func New() *Extractor { return &Extractor{MaxFileBytes: 4 * 1024 * 1024} }

func (e *Extractor) Name() string { return "glue" }

type discoveredJob struct {
	name        string
	script      string
	sources     []string
	dests       []string
	schedule    string
	file        string
	line        int
}

func (e *Extractor) Extract(ctx context.Context, repoPath, repoName string) (*extract.Fragment, error) {
	frag := extract.NewFragment(e.Name())
	repoNodeID := "repo::" + repoName

	var jobs []discoveredJob

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
		info, statErr := d.Info()
		if statErr != nil || info.Size() > e.MaxFileBytes {
			return nil
		}
		name := d.Name()
		ext := strings.ToLower(filepath.Ext(name))
		rel, _ := filepath.Rel(repoPath, path)
		rel = filepath.ToSlash(rel)

		body, rerr := os.ReadFile(path)
		if rerr != nil {
			frag.Warn(fmt.Sprintf("%s: %v", rel, rerr))
			return nil
		}

		switch {
		case ext == ".tf" || ext == ".hcl" || ext == ".tfvars":
			jobs = append(jobs, parseTerraform(string(body), rel)...)
		case (ext == ".yaml" || ext == ".yml") && looksLikeCloudFormation(string(body)):
			jobs = append(jobs, parseCloudFormationYAML(string(body), rel, frag)...)
		case ext == ".json" && looksLikeCloudFormation(string(body)):
			jobs = append(jobs, parseCloudFormationJSON(string(body), rel, frag)...)
		case ext == ".py" && looksLikeGlueScript(string(body)):
			jobs = append(jobs, parseGlueScript(string(body), rel)...)
		}
		return nil
	}

	if err := filepath.WalkDir(repoPath, walk); err != nil {
		return frag, fmt.Errorf("walk repo: %w", err)
	}

	for _, j := range jobs {
		emitJob(frag, repoNodeID, repoName, j)
	}
	return frag, nil
}

// --- Terraform aws_glue_job resource ---

var (
	tfGlueJobRe = regexp.MustCompile(`(?s)resource\s+"aws_glue_job"\s+"([^"]+)"\s*\{(.*?)\n\}`)
	tfNameRe    = regexp.MustCompile(`name\s*=\s*"([^"]+)"`)
	tfScriptRe  = regexp.MustCompile(`script_location\s*=\s*"([^"]+)"`)
	tfTriggerRe = regexp.MustCompile(`(?s)resource\s+"aws_glue_trigger"\s+"([^"]+)"\s*\{(.*?)\n\}`)
	tfScheduleRe = regexp.MustCompile(`schedule\s*=\s*"([^"]+)"`)
	tfJobsList   = regexp.MustCompile(`job_name\s*=\s*"([^"]+)"`)
)

func parseTerraform(body, file string) []discoveredJob {
	var out []discoveredJob
	for _, m := range tfGlueJobRe.FindAllStringSubmatch(body, -1) {
		resourceName, block := m[1], m[2]
		j := discoveredJob{file: file}
		if mm := tfNameRe.FindStringSubmatch(block); mm != nil {
			j.name = mm[1]
		} else {
			j.name = resourceName
		}
		if mm := tfScriptRe.FindStringSubmatch(block); mm != nil {
			j.script = mm[1]
		}
		out = append(out, j)
	}
	// Bind triggers to their jobs.
	for _, m := range tfTriggerRe.FindAllStringSubmatch(body, -1) {
		block := m[2]
		schedule := ""
		if mm := tfScheduleRe.FindStringSubmatch(block); mm != nil {
			schedule = mm[1]
		}
		for _, jm := range tfJobsList.FindAllStringSubmatch(block, -1) {
			for i := range out {
				if out[i].name == jm[1] && out[i].schedule == "" {
					out[i].schedule = schedule
				}
			}
		}
	}
	return out
}

// --- CloudFormation ---

func looksLikeCloudFormation(s string) bool {
	return strings.Contains(s, "AWSTemplateFormatVersion") ||
		strings.Contains(s, "AWS::Glue::Job") ||
		strings.Contains(s, "AWS::CloudFormation::")
}

func parseCloudFormationYAML(body, file string, frag *extract.Fragment) []discoveredJob {
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(body), &doc); err != nil {
		frag.Warn(fmt.Sprintf("%s: yaml: %v", file, err))
		return nil
	}
	return walkCFResources(doc, file)
}

func parseCloudFormationJSON(body, file string, frag *extract.Fragment) []discoveredJob {
	var doc map[string]any
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		frag.Warn(fmt.Sprintf("%s: json: %v", file, err))
		return nil
	}
	return walkCFResources(doc, file)
}

func walkCFResources(doc map[string]any, file string) []discoveredJob {
	var out []discoveredJob
	resources, _ := doc["Resources"].(map[string]any)
	if resources == nil {
		return out
	}
	for logicalID, raw := range resources {
		res, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		typ, _ := res["Type"].(string)
		switch typ {
		case "AWS::Glue::Job":
			props, _ := res["Properties"].(map[string]any)
			name, _ := props["Name"].(string)
			if name == "" {
				name = logicalID
			}
			script := ""
			if cmd, ok := props["Command"].(map[string]any); ok {
				script, _ = cmd["ScriptLocation"].(string)
			}
			out = append(out, discoveredJob{name: name, script: script, file: file})
		case "AWS::Glue::Trigger":
			props, _ := res["Properties"].(map[string]any)
			schedule, _ := props["Schedule"].(string)
			actions, _ := props["Actions"].([]any)
			for _, a := range actions {
				action, _ := a.(map[string]any)
				jobName, _ := action["JobName"].(string)
				if jobName == "" {
					continue
				}
				for i := range out {
					if out[i].name == jobName {
						out[i].schedule = schedule
					}
				}
			}
		}
	}
	return out
}

// --- Python Glue scripts ---

func looksLikeGlueScript(s string) bool {
	return strings.Contains(s, "awsglue") || strings.Contains(s, "GlueContext") || strings.Contains(s, "create_dynamic_frame")
}

var (
	glueReadCatalog  = regexp.MustCompile(`create_dynamic_frame\.from_catalog\s*\([^)]*database\s*=\s*["']([^"']+)["']\s*,\s*table_name\s*=\s*["']([^"']+)["']`)
	glueWriteCatalog = regexp.MustCompile(`write_dynamic_frame\.from_catalog\s*\([^)]*database\s*=\s*["']([^"']+)["']\s*,\s*table_name\s*=\s*["']([^"']+)["']`)
	glueJobNameRe    = regexp.MustCompile(`args\s*=\s*getResolvedOptions\s*\([^)]*\[\s*["']JOB_NAME["']\s*\]`)
	glueJobInitRe    = regexp.MustCompile(`Job\(glueContext\)`)
)

func parseGlueScript(body, file string) []discoveredJob {
	if !glueJobInitRe.MatchString(body) && !glueJobNameRe.MatchString(body) {
		// Bare boto3 call or unrelated module — skip rather than guess.
		return nil
	}
	// We don't have the job name in the script itself in general — use the
	// script filename as a best-effort label. The Terraform/CFN parsers will
	// override this if the same script is referenced by a declared job.
	name := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
	j := discoveredJob{name: name, script: file, file: file}
	for _, m := range glueReadCatalog.FindAllStringSubmatch(body, -1) {
		j.sources = append(j.sources, m[1]+"."+m[2])
	}
	for _, m := range glueWriteCatalog.FindAllStringSubmatch(body, -1) {
		j.dests = append(j.dests, m[1]+"."+m[2])
	}
	return []discoveredJob{j}
}

// --- Emit ---

func emitJob(frag *extract.Fragment, repoNodeID, repoName string, j discoveredJob) {
	if j.name == "" {
		return
	}
	jobID := "glue::job::" + repoName + "::" + j.name
	frag.AddNode(extract.FragmentNode{
		ID:    jobID,
		Label: j.name,
		Type:  "glue_job",
		SourceFile: j.file,
		SourceLocation: fmt.Sprintf("L%d", j.line),
		Metadata: map[string]any{
			"repo":     repoName,
			"script":   j.script,
			"schedule": j.schedule,
			"sources":  j.sources,
			"dests":    j.dests,
		},
	})
	frag.AddEdge(extract.FragmentEdge{
		Source:     repoNodeID,
		Target:     jobID,
		Relation:   "contains",
		Confidence: extract.ConfidenceExtracted,
		SourceFile: j.file,
	})
	for _, src := range j.sources {
		tid := "sql::sql_table::" + src
		frag.AddNode(extract.FragmentNode{
			ID:    tid,
			Label: src,
			Type:  "sql_table",
		})
		frag.AddEdge(extract.FragmentEdge{
			Source:     jobID,
			Target:     tid,
			Relation:   "reads_source",
			Confidence: extract.ConfidenceInferred,
			SourceFile: j.file,
		})
	}
	for _, dst := range j.dests {
		tid := "sql::sql_table::" + dst
		frag.AddNode(extract.FragmentNode{
			ID:    tid,
			Label: dst,
			Type:  "sql_table",
		})
		frag.AddEdge(extract.FragmentEdge{
			Source:     jobID,
			Target:     tid,
			Relation:   "writes_destination",
			Confidence: extract.ConfidenceInferred,
			SourceFile: j.file,
		})
	}
	if j.schedule != "" {
		schedID := "glue::schedule::" + j.name
		frag.AddNode(extract.FragmentNode{
			ID:    schedID,
			Label: j.schedule,
			Type:  "glue_job",
			Metadata: map[string]any{
				"is_schedule": true,
				"expression":  j.schedule,
			},
		})
		frag.AddEdge(extract.FragmentEdge{
			Source:     schedID,
			Target:     jobID,
			Relation:   "scheduled",
			Confidence: extract.ConfidenceExtracted,
			SourceFile: j.file,
			Context:    j.schedule,
		})
	}
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
