package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/apidocs"
	"github.com/cyberpunk47/fairrelay/internal/config"
	"github.com/cyberpunk47/fairrelay/internal/ingest"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	// ctx is cancelled on Ctrl+C (os.Interrupt) or when Docker stops the container (SIGTERM)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pg, err := store.NewPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("database error: %v", err)
	}
	defer pg.Close()

	h := ingest.NewHandler(pg, cfg.ProducerAPIKey)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", h.CreateEvent)
	mux.HandleFunc("GET /events/{id}", h.GetEvent)
	mux.HandleFunc("GET /healthz", h.Health)
	apidocs.Register(mux) // GET /docs (Swagger UI) and GET /openapi.yaml

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Run the server in the background so main can wait for the stop signal
	go func() {
		log.Printf("ingest API listening on %s (docs at /docs)", cfg.HTTPAddr)
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server stopped: %v", err)
		}
	}()

	<-ctx.Done() // wait here until Ctrl+C or SIGTERM

	// Stop taking new requests; give requests already running up to 10 s to finish
	log.Println("shutting down: finishing open requests...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	log.Println("stopped cleanly")
}
