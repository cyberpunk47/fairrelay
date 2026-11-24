package delivery

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/retry"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

// Breaker holds the circuit-breaker settings: after Failures failures in a row an
// endpoint gets nothing for Cooldown, then one test delivery decides what happens.
type Breaker struct {
	Failures int
	Cooldown time.Duration
}

type Worker struct {
	store       store.Store
	sender      *Sender
	maxAttempts int
	backOffBase time.Duration
	backOffMax  time.Duration
	breaker     Breaker
}

func NewWorker(s store.Store, sender *Sender, maxAttempts int, backOffBase, backOffMax time.Duration, breaker Breaker) *Worker {
	return &Worker{
		store:       s,
		sender:      sender,
		maxAttempts: maxAttempts,
		backOffBase: backOffBase,
		backOffMax:  backOffMax,
		breaker:     breaker,
	}
}

// Handle makes one delivery try and records it. Returns a short outcome for logs:
// delivered | retry | dead | lease_lost | error
func (w *Worker) Handle(ctx context.Context, ev domain.ClaimedEvent) string {
	ep, err := w.store.GetEndpoint(ctx, ev.EndpointID)
	if err != nil {
		// Leave it IN_FLIGHT: the lease expires and lease recovery puts it back.
		log.Printf("event %s: get endpoint: %v", ev.ExternalID, err)
		return "error"
	}

	res := w.sender.Send(ctx, ep, ev)
	w.updateBreaker(ctx, ep.ID, res)

	outcome, err := w.record(ctx, ev, res, toAttempt(ev, res))
	if errors.Is(err, store.ErrLeaseLost) {
		return "lease_lost" // someone else owns the event now; our result was kept only as history
	}
	if err != nil {
		log.Printf("event %s: record result: %v", ev.ExternalID, err)
		return "error"
	}
	return outcome
}

// updateBreaker feeds the result into the endpoint's circuit breaker.
// Only "the endpoint is broken" results count as failures: no reply (timeout,
// refused) or a 5xx. A 4xx means the endpoint is up and answering.
func (w *Worker) updateBreaker(ctx context.Context, endpointID int64, res Result) {
	if isSuccess(res) {
		if err := w.store.EndpointSucceeded(ctx, endpointID); err != nil {
			log.Printf("breaker: endpoint %d: %v", endpointID, err)
		}
		return
	}
	if res.Err == nil && res.StatusCode < 500 {
		return
	}
	state, err := w.store.EndpointFailed(ctx, endpointID, w.breaker.Failures, w.breaker.Cooldown)
	if err != nil {
		log.Printf("breaker: endpoint %d: %v", endpointID, err)
		return
	}
	if state == domain.BreakerOpen {
		log.Printf("breaker OPEN for endpoint %d: pausing deliveries for %v", endpointID, w.breaker.Cooldown)
	}
}

func (w *Worker) record(ctx context.Context, ev domain.ClaimedEvent, res Result, attempt domain.AttemptResult) (string, error) {
	if isSuccess(res) {
		return "delivered", w.store.MarkDelivered(ctx, attempt)
	}
	if ev.Attempts >= w.maxAttempts {
		return "dead", w.store.MarkDead(ctx, attempt)
	}

	// Wait before the next try: exponential backoff with jitter,
	// or longer if the endpoint asked for it (Retry-After), never above backOffMax.
	wait := retry.Next(ev.Attempts, w.backOffBase, w.backOffMax)
	if res.RetryAfter > wait {
		wait = res.RetryAfter
	}
	if wait > w.backOffMax {
		wait = w.backOffMax
	}
	return "retry", w.store.MarkRetry(ctx, attempt, time.Now().Add(wait))
}

func isSuccess(res Result) bool {
	return res.Err == nil && res.StatusCode >= 200 && res.StatusCode < 300
}

func toAttempt(ev domain.ClaimedEvent, res Result) domain.AttemptResult {
	errText := ""
	if res.Err != nil {
		errText = res.Err.Error()
	} else if res.StatusCode >= 300 || res.StatusCode < 200 {
		errText = fmt.Sprintf("http %d", res.StatusCode)
	}
	return domain.AttemptResult{
		EventID:       ev.ID,
		LeaseToken:    ev.LeaseToken,
		AttemptNumber: ev.Attempts,
		StartedAt:     res.StartedAt,
		FinishedAt:    res.FinishedAt,
		DurationMs:    int(res.FinishedAt.Sub(res.StartedAt).Milliseconds()),
		HTTPStatus:    res.StatusCode,
		Error:         errText,
	}
}
