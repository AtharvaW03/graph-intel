package extract

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// Runner executes a collection of Extractors for one repository. Extractors
// run concurrently because they are pure file readers — there is no shared
// state. The runner enforces panic recovery per-extractor so one buggy
// plugin can't take down a whole repo's enrichment pass.
type Runner struct {
	Extractors []Extractor
	Log        *log.Logger
	// MaxParallel caps concurrent extractors per repo. Zero or negative
	// means run all extractors in parallel.
	MaxParallel int
}

// Result is the aggregate of running every configured extractor against one
// repository. Errors are returned per-extractor rather than aggregated so the
// orchestrator can record them on RepoResult without conflating extractor
// failures with sync/import failures.
type Result struct {
	Fragments []*Fragment
	Errors    map[string]error
	Warnings  map[string][]string
}

// Run executes every extractor against repoPath and gathers their fragments.
// One extractor failing never aborts the others; failures appear in the
// returned Result.Errors keyed by extractor Name.
func (r *Runner) Run(ctx context.Context, repoPath, repoName string) Result {
	res := Result{
		Errors:   map[string]error{},
		Warnings: map[string][]string{},
	}
	if len(r.Extractors) == 0 {
		return res
	}

	type partial struct {
		frag *Fragment
		err  error
		name string
	}
	results := make(chan partial, len(r.Extractors))

	parallel := r.MaxParallel
	if parallel <= 0 {
		parallel = len(r.Extractors)
	}
	sem := make(chan struct{}, parallel)

	var wg sync.WaitGroup
	for _, ex := range r.Extractors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			defer func() {
				if p := recover(); p != nil {
					results <- partial{name: ex.Name(), err: fmt.Errorf("panic: %v", p)}
				}
			}()

			frag, err := ex.Extract(ctx, repoPath, repoName)
			if err != nil {
				results <- partial{name: ex.Name(), err: err}
				return
			}
			if frag != nil {
				if vErr := frag.Validate(); vErr != nil {
					results <- partial{name: ex.Name(), err: fmt.Errorf("validate: %w", vErr)}
					return
				}
				frag.Extractor = ex.Name()
			}
			results <- partial{name: ex.Name(), frag: frag}
		}()
	}

	wg.Wait()
	close(results)

	for p := range results {
		if p.err != nil {
			res.Errors[p.name] = p.err
			if r.Log != nil {
				r.Log.Printf("extractor %q failed: %v", p.name, p.err)
			}
			continue
		}
		if p.frag == nil || p.frag.Empty() {
			continue
		}
		if len(p.frag.Warnings) > 0 {
			res.Warnings[p.name] = p.frag.Warnings
			if r.Log != nil {
				for _, w := range p.frag.Warnings {
					r.Log.Printf("extractor %q warning: %s", p.name, w)
				}
			}
		}
		res.Fragments = append(res.Fragments, p.frag)
	}
	return res
}
