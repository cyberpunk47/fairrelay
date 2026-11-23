package main

import (
	"context"
	"fmt"
	"log"

	"github.com/cyberpunk47/fairrelay/internal/config"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

func main() {
	// 1. Load config
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	// 2. Connect to Postgres
	ctx := context.Background()
	pg, err := store.NewPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("database error: %v", err)
	}
	defer pg.Close()
	// 4. Reclaim expired events
	count, err := pg.ReclaimExpired(ctx)
	if err != nil {
		log.Fatalf("reclaim expired error: %v", err)
	}
	fmt.Printf("reclaimed %d expired events\n", count)

	// 3. Try to claim 3 times for tenant 1
	for i := 1; i <= 3; i++ {
		ev, found, err := pg.ClaimNext(ctx, 1, cfg.LeaseTTL)
		if err != nil {
			log.Fatalf("claim %d error: %v", i, err)
		}
		if !found {
			fmt.Printf("claim %d: nothing claimed (cap full or no due event)\n", i)
			continue
		}
		fmt.Printf("claim %d: id=%d external_id=%s attempts=%d lease_token=%d\n",
			i, ev.ID, ev.ExternalID, ev.Attempts, ev.LeaseToken)
	}
	
}