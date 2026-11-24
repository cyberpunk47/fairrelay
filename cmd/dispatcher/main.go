package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cyberpunk47/fairrelay/internal/config"
	"github.com/cyberpunk47/fairrelay/internal/dispatcher"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	// ctx is cancelled on Ctrl+C (os.Interrupt) or when Docker stops the container (SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := dispatcher.Run(ctx, cfg); err != nil {
		log.Fatalf("dispatcher: %v", err)
	}
}
