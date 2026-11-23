package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/config"
	"github.com/cyberpunk47/fairrelay/internal/delivery"
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

	sender := delivery.NewSender(cfg.ConnectTimeout, cfg.RequestTimeout)

	// 1. Claim
	ev, found, err := pg.ClaimNext(ctx, 1, cfg.LeaseTTL)
	if err != nil {
		log.Fatalf("claim error: %v", err)
	}
	if !found {
		fmt.Println("nothing to claim")
		return
	}

	// 2. Look up where to send it
	ep, err := pg.GetEndpoint(ctx, ev.EndpointID)
	if err != nil {
		log.Fatalf("endpoint error: %v", err)
	}

	// 3. Send
	res := sender.Send(ctx, ep, ev)
	fmt.Printf("event=%s status=%d err=%v took=%v retryAfter=%v\n",
		ev.ExternalID, res.StatusCode, res.Err, res.FinishedAt.Sub(res.StartedAt), res.RetryAfter)

	// 4. Record the result
	errText := ""
	if res.Err != nil {
		errText = res.Err.Error()
	} else if res.StatusCode >= 300 {
		errText = fmt.Sprintf("http %d", res.StatusCode)
	}
	attempt := domain.AttemptResult{
		EventID:       ev.ID,
		LeaseToken:    ev.LeaseToken,
		AttemptNumber: ev.Attempts,
		StartedAt:     res.StartedAt,
		FinishedAt:    res.FinishedAt,
		DurationMs:    int(res.FinishedAt.Sub(res.StartedAt).Milliseconds()),
		HTTPStatus:    res.StatusCode,
		Error:         errText,
	}
	if res.Err == nil && res.StatusCode >= 200 && res.StatusCode < 300 {
		fmt.Println("mark delivered:", pg.MarkDelivered(ctx, attempt))
	} else {
		fmt.Println("mark retry:", pg.MarkRetry(ctx, attempt, time.Now()))
	}
}