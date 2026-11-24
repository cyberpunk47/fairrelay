package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
)

type Scheduler struct {
	policy Policy
	jobs   chan<- domain.ClaimedEvent
	wake   <-chan struct{} // a worker finished a delivery: a cap slot may be free again
	idle   time.Duration
}

// New builds a scheduler. Workers signal on wake after each delivery, so the
// scheduler doesn't sit out its idle wait while a freed slot could be used.
func New(p Policy, jobs chan<- domain.ClaimedEvent, wake <-chan struct{}) *Scheduler {
	return &Scheduler{policy: p, jobs: jobs, wake: wake, idle: 200 * time.Millisecond}
}

func (s *Scheduler) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		events, err := s.policy.Next(ctx)
		if err != nil && ctx.Err() == nil {
			log.Printf("scheduler (%s): %v", s.policy.Name(), err)
		}

		for _, ev := range events {
			select {
			case s.jobs <- ev: // waits while every worker is busy
			case <-ctx.Done():
				return // unhanded events: their leases expire and recovery re-queues them
			}
		}

		// Nothing claimable right now (no due events, or every tenant at its cap).
		// Wait for a delivery to finish, or the idle timeout for new events / retries.
		if len(events) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-s.wake:
			case <-time.After(s.idle):
			}
		}
	}
}
