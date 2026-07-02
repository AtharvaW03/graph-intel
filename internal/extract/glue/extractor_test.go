package glue

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"graph-platform/internal/extract"
)

const fixtureTF = `resource "aws_glue_job" "daily" {
  name            = "daily-recon"
  role_arn        = aws_iam_role.glue.arn
  script_location = "s3://bucket/scripts/etl_daily.py"
}

resource "aws_glue_trigger" "daily_trigger" {
  name     = "daily-trigger"
  schedule = "cron(0 2 * * ? *)"
  actions {
    job_name = "daily-recon"
  }
}
`

const fixtureScript = `import sys
from awsglue.context import GlueContext
from awsglue.job import Job

job = Job(glueContext)
df = glueContext.create_dynamic_frame.from_catalog(database="raw", table_name="trades")
glueContext.write_dynamic_frame.from_catalog(database="mart", table_name="daily_recon")
`

func runExtract(t *testing.T, files map[string]string) *extract.Fragment {
	t.Helper()
	dir := t.TempDir()
	for name, contents := range files {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(contents), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	frag, err := New().Extract(context.Background(), dir, "data-repo")
	if err != nil {
		t.Fatal(err)
	}
	return frag
}

func nodeByID(frag *extract.Fragment, id string) *extract.FragmentNode {
	for i := range frag.Nodes {
		if frag.Nodes[i].ID == id {
			return &frag.Nodes[i]
		}
	}
	return nil
}

func TestTerraformJobWithScheduleAndScriptMerge(t *testing.T) {
	frag := runExtract(t, map[string]string{
		"main.tf":            fixtureTF,
		"jobs/etl_daily.py":  fixtureScript,
	})

	job := nodeByID(frag, "glue::job::data-repo::daily-recon")
	if job == nil {
		t.Fatalf("declared job missing; nodes: %+v", frag.Nodes)
	}
	if job.SourceLocation == "" || job.SourceLocation == "L0" {
		t.Errorf("job source location = %q, want a real line number", job.SourceLocation)
	}
	if job.Metadata["schedule"] != "cron(0 2 * * ? *)" {
		t.Errorf("schedule not bound from trigger: %v", job.Metadata["schedule"])
	}

	// The script-derived job must have been folded into the declared one:
	// its catalog reads/writes belong to daily-recon, and no standalone
	// etl_daily job node exists.
	if n := nodeByID(frag, "glue::job::data-repo::etl_daily"); n != nil {
		t.Error("script-derived job not merged into declared job")
	}
	sources, _ := job.Metadata["sources"].([]string)
	dests, _ := job.Metadata["dests"].([]string)
	if len(sources) != 1 || sources[0] != "raw.trades" {
		t.Errorf("sources = %v, want [raw.trades]", sources)
	}
	if len(dests) != 1 || dests[0] != "mart.daily_recon" {
		t.Errorf("dests = %v, want [mart.daily_recon]", dests)
	}

	// Schedule node: repo-scoped ID and glue_schedule type (its own label,
	// not GlueJob).
	sched := nodeByID(frag, "glue::schedule::data-repo::daily-recon")
	if sched == nil {
		t.Fatal("schedule node missing")
	}
	if sched.Type != "glue_schedule" {
		t.Errorf("schedule type = %q, want glue_schedule", sched.Type)
	}

	if nodeByID(frag, "repo::data-repo") == nil {
		t.Error("repo hub node missing — CONTAINS edges dangle when deps extractor is disabled")
	}
}

func TestStandaloneScriptJobSurvives(t *testing.T) {
	frag := runExtract(t, map[string]string{
		"jobs/adhoc_backfill.py": fixtureScript,
	})
	job := nodeByID(frag, "glue::job::data-repo::adhoc_backfill")
	if job == nil {
		t.Fatalf("standalone script job missing; nodes: %+v", frag.Nodes)
	}
}

func TestCloudFormationJob(t *testing.T) {
	frag := runExtract(t, map[string]string{
		"stack.yaml": `AWSTemplateFormatVersion: "2010-09-09"
Resources:
  ReconJob:
    Type: AWS::Glue::Job
    Properties:
      Name: cfn-recon
      Command:
        ScriptLocation: s3://bucket/recon.py
  ReconTrigger:
    Type: AWS::Glue::Trigger
    Properties:
      Schedule: cron(0 3 * * ? *)
      Actions:
        - JobName: cfn-recon
`,
	})
	job := nodeByID(frag, "glue::job::data-repo::cfn-recon")
	if job == nil {
		t.Fatalf("CFN job missing; nodes: %+v", frag.Nodes)
	}
	if job.Metadata["schedule"] != "cron(0 3 * * ? *)" {
		t.Errorf("CFN trigger schedule not bound: %v", job.Metadata["schedule"])
	}
}
