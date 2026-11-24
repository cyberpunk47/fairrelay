package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

type Scheduler struct {
	store store.Store
	jobs  chan<- domain.ClaimedEvent
	lease time.Duration
	idle  time.Duration
}

// New creates a new Scheduler that claims events from the store and sends them to the jobs channel.
func New(s store.Store, jobs chan<- domain.ClaimedEvent, lease time.Duration) *Scheduler {
	return &Scheduler{
		store: s,
		jobs:  jobs,
		lease: lease,
		idle:  200 * time.Millisecond,
	}
}
// Run keeps doing rounds until ctx is cancelled (Ctrl+C).
func (s *Scheduler) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		claimed := s.round(ctx)
		if claimed == 0{
			// Nothing was handed out: pause briefly instead of hammering the database
			select{
				case <-ctx.Done():
					return
				case <-time.After(s.idle):
			}

		}
	}
}

func (s *Scheduler) round(ctx context.Context) int {
	tenants , err := s.store.DueTenants(ctx)
	if err != nil{
		if ctx.Err() == nil{
			log.Printf("Scheduler: due tenants %v", err)
		}
	return 0
	}
	claimed := 0
	for _, tenantID := range tenants{
		ev , found , err := s.store.ClaimNext(ctx , tenantID,s.lease)
		if err != nil{
			log.Printf("Scheduler: claim tenant %d : %v", tenantID , err)
		}
		if !found{ // nothing to claim (cap full or nothing due yet)
			continue
		}
		select{
		case s.jobs <- ev: //waits here while all workers are busy(backpressure)
			claimed++
		case <-ctx.Done():
			// Shutting down before a worker took , its lease will expire and lease recover will put it back into the queue
			return claimed
		}
	}
	return claimed
}