package dispatcher

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/config"
	"github.com/cyberpunk47/fairrelay/internal/delivery"
	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/scheduler"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

// Run starts the workers, lease recovery and the scheduler, and keeps going
// until ctx is cancelled. Then it shuts down cleanly and returns.
func Run(ctx context.Context, cfg config.Config) error {
	pg, err := store.NewPostgres(context.Background(), cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("database: %w", err)
	}
	defer pg.Close()

	sender := delivery.NewSender(cfg.ConnectTimeout, cfg.RequestTimeout)
	breaker := delivery.Breaker{Failures: cfg.BreakerFailures, Cooldown: cfg.BreakerCooldown}
	worker := delivery.NewWorker(pg, sender, cfg.MaxAttempts, cfg.BackoffBase, cfg.BackoffMax, breaker)

	// The policy is chosen first: workers report delivery times to it if it wants them (DRR).
	policy := choosePolicy(cfg, pg)
	observer, _ := policy.(scheduler.Observer) // nil for fifo and roundrobin

	jobs := make(chan domain.ClaimedEvent) // unbuffered: hand-off only when a worker is free
	wake := make(chan struct{}, 1)         // "a delivery finished" signal for the scheduler
	var wg sync.WaitGroup

	// 1. Workers
	workers := cfg.Workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ev := range jobs {
				// Background, not ctx: a delivery in progress may finish on shutdown
				started := time.Now()
				outcome := worker.Handle(context.Background(), ev)
				if observer != nil {
					observer.Observe(ev, time.Since(started))
				}
				select { // non-blocking: one pending signal is enough
				case wake <- struct{}{}:
				default:
				}
				log.Printf("tenant=%d event=%s attempt=%d → %s", ev.TenantID, ev.ExternalID, ev.Attempts, outcome)
			}
		}()
	}

	// 2. Maintenance every 5 s: lease recovery + circuit-breaker probes
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n, err := pg.ReclaimExpired(ctx, cfg.MaxAttempts)
				if err != nil && ctx.Err() == nil {
					log.Printf("reclaim: %v", err)
				}
				if n > 0 {
					log.Printf("reclaimed %d expired leases", n)
				}
				p, err := pg.ProbeDueEndpoints(ctx)
				if err != nil && ctx.Err() == nil {
					log.Printf("breaker probes: %v", err)
				}
				if p > 0 {
					log.Printf("breaker HALF_OPEN for %d endpoint(s): sending one test delivery", p)
				}
			}
		}
	}()

	// 3. Scheduler: runs until ctx is cancelled
	log.Printf("dispatcher running: %d workers, policy=%s", workers, policy.Name())
	scheduler.New(policy, jobs, wake).Run(ctx)

	// 4. Clean shutdown
	log.Println("shutting down: finishing in-flight deliveries...")
	close(jobs)
	wg.Wait()
	log.Println("stopped cleanly")
	return nil
}

// choosePolicy picks the rule card from config (Strategy pattern).
func choosePolicy(cfg config.Config, s store.Store) scheduler.Policy {
	if cfg.Policy == "fifo" {
		return scheduler.NewFIFO(s, cfg.LeaseTTL)
	}
	if cfg.Policy == "roundrobin" {
		return scheduler.NewRoundRobin(s, cfg.LeaseTTL)
	}
	return scheduler.NewDRR(s, cfg.LeaseTTL) // config validation allows only these three

}
