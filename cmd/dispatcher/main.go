package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/config"
	"github.com/cyberpunk47/fairrelay/internal/delivery"
	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/scheduler"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	// ctx is cancelled when you press Ctrl+C
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	pg, err := store.NewPostgres(context.Background(), cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("database error: %v", err)
	}
	defer pg.Close()

	sender := delivery.NewSender(cfg.ConnectTimeout, cfg.RequestTimeout)
	worker := delivery.NewWorker(pg, sender, cfg.MaxAttempts, cfg.BackoffBase, cfg.BackoffMax)

	jobs := make(chan domain.ClaimedEvent) // unbuffered: hand-off only when a worker is free
	var wg sync.WaitGroup

	// 1. Start the workers
	workers := cfg.WorkersFast + cfg.WorkersSlow
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ev := range jobs { // ends when jobs is closed
				// context.Background(), not ctx: on Ctrl+C a delivery already in progress
				// is allowed to finish (it's still capped by the 10 s timeout).
				outcome := worker.Handle(context.Background(), ev)
				log.Printf("tenant=%d event=%s attempt=%d → %s", ev.TenantID, ev.ExternalID, ev.Attempts, outcome)
			}
		}()
	}

	// 2. Lease recovery every 5 s
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
			}
		}
	}()

	// 3. The scheduler runs here until Ctrl+C
	log.Printf("dispatcher running: %d workers, round robin", workers)
	scheduler.New(pg, jobs, cfg.LeaseTTL).Run(ctx)

	// 4. Clean shutdown
	log.Println("shutting down: finishing in-flight deliveries...")
	close(jobs) // no more events: workers finish their current one and exit
	wg.Wait()   // wait for every worker and the recovery goroutine
	log.Println("stopped cleanly")
}