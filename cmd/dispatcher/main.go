package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/config"
	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx := context.Background()
	pg, err := store.NewPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("database error: %v", err)
	}
	defer pg.Close()

	// Lease recovery: cfg.MaxAttempts goes into the maxAttempts slot
	n, err := pg.ReclaimExpired(ctx, cfg.MaxAttempts)
	if err != nil {
		log.Fatalf("reclaim error: %v", err)
	}
	fmt.Println("reclaimed:", n)

	// Test A: claim one event and mark it delivered
	ev, found, err := pg.ClaimNext(ctx, 1, cfg.LeaseTTL)
	if err != nil {
		log.Fatalf("claim error: %v", err)
	}
	if !found {
		fmt.Println("nothing to claim for test A")
		return
	}
	now := time.Now()
	res := domain.AttemptResult{
		EventID:       ev.ID,
		LeaseToken:    ev.LeaseToken,
		AttemptNumber: ev.Attempts,
		StartedAt:     now,
		FinishedAt:    now,
		DurationMs:    50,
		HTTPStatus:    200,
	}
	fmt.Println("A deliver:", pg.MarkDelivered(ctx, res))

	// Test B: the same call again → must be "lease lost"
	fmt.Println("B deliver again:", pg.MarkDelivered(ctx, res))

	// Test C: claim another event, then save a result with an OLD token (frozen worker)
	ev2, found, err := pg.ClaimNext(ctx, 1, cfg.LeaseTTL)
	if err != nil {
		log.Fatalf("claim error: %v", err)
	}
	if !found {
		fmt.Println("nothing to claim for test C")
		return
	}
	stale := domain.AttemptResult{
		EventID:       ev2.ID,
		LeaseToken:    ev2.LeaseToken - 1, // the old token
		AttemptNumber: ev2.Attempts,
		StartedAt:     now,
		FinishedAt:    now,
		DurationMs:    10000,
		HTTPStatus:    500,
		Error:         "server error",
	}
	fmt.Println("C stale retry:", pg.MarkRetry(ctx, stale, time.Now().Add(5*time.Second)))
}
