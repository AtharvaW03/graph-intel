package index

import (
	"context"
	"fmt"
	"time"
)

// Scheduler decides when the next indexing cycle should start. The contract
// is intentionally narrow — one Wait method — so future cron-based or
// channel-triggered (webhook fire-now) variants drop in without touching
// the orchestrator. Wait MUST honor ctx cancellation.
type Scheduler interface {
	// Wait blocks until the next cycle should start, or returns ctx.Err()
	// when ctx is canceled. Returning nil means "fire now"; any non-nil
	// non-context error aborts RunForever.
	Wait(ctx context.Context) error
}

// IntervalScheduler fires on a fixed period. It is the default scheduler
// used when the operator passes --interval on the CLI.
type IntervalScheduler struct {
	d time.Duration
}

func NewIntervalScheduler(d time.Duration) (*IntervalScheduler, error) {
	if d <= 0 {
		return nil, fmt.Errorf("interval must be > 0")
	}
	return &IntervalScheduler{d: d}, nil
}

func (s *IntervalScheduler) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.d):
		return nil
	}
}
