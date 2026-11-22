package main

import (
	"context"
	"fmt"
	"os"

	"github.com/cyberpunk47/fairrelay/internal/config"
	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Println("config error:")
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := context.Background()
	pg, err := store.NewPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		fmt.Println("database error:")
		fmt.Println(err)
		os.Exit(1)
	}
	defer pg.Close()

	id, created, err := pg.InsertEvent(ctx, domain.NewEvent{
		TenantID:   1,
		EndpointID: 1,
		ExternalID: "evt_test_1",
		EventType:  "test_event",
		Payload:    `{"hello":"world"}`,
	})
	if err != nil {
		fmt.Println("insert error:")
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("id:", id, "| created:", created)
}