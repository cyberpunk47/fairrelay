package main

import (
	"context"
	"fmt"
	"log"

	"github.com/cyberpunk47/fairrelay/internal/config"
	"github.com/cyberpunk47/fairrelay/internal/delivery"
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

	sender := delivery.NewSender(cfg.ConnectTimeout, cfg.RequestTimeout)
	worker := delivery.NewWorker(pg, sender, cfg.MaxAttempts, cfg.BackoffBase, cfg.BackoffMax)

	ev, found, err := pg.ClaimNext(ctx, 1, cfg.LeaseTTL)
	if err != nil {
		log.Fatalf("claim error: %v", err)
	}
	if !found {
		fmt.Println("nothing to claim (cap full or nothing due yet)")
		return
	}
	fmt.Printf("event=%s attempt=%d → outcome: %s\n", ev.ExternalID, ev.Attempts, worker.Handle(ctx, ev))
}