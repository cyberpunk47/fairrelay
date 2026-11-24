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
	idle   time.Duration
}

func New(p Policy, jobs chan<- domain.ClaimedEvent) *Scheduler {
	return &Scheduler{policy: p, jobs: jobs, idle: 200 * time.Millisecond}
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

		if len(events) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.idle):
			}
		}
	}
}
