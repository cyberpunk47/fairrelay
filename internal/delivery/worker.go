package delivery

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/retry"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

type Worker struct {
	store store.Store
	sender *Sender
	maxAttempts int
	backOffBase time.Duration
	backOffMax time.Duration
}

func NewWorker(s store.Store , sender *Sender , maxAttempts int , backOffBase time.Duration , backOffMax time.Duration ) *Worker {
	return &Worker{
		store: s,
		sender: sender,
		maxAttempts: maxAttempts,
		backOffBase: backOffBase,
		backOffMax: backOffMax,
	}
}
func (w* Worker) Handle(ctx context.Context, ev domain.ClaimedEvent) string{
	ep , err := w.store.GetEndpoint(ctx, ev.EndpointID)
	if err != nil{
		log.Printf("event %s: get endpoint: %v",ev.ExternalID, err)
		return "get endpoint error"
	}
	res := w.sender.Send(ctx, ep , ev)
	attempt := toAttempt(ev, res)

	outcome , err := w.record(ctx, ev , res ,attempt)
	if err != nil{
		log.Printf("event %s: record result: %v",ev.ExternalID, err)
		return "record error"
	}
	return outcome
}

func (w* Worker) record(ctx context.Context, ev domain.ClaimedEvent , res Result , attempt domain.AttemptResult) (string , error){
	if isSuccess(res){
		return "delivered" , w.store.MarkDelivered(ctx, attempt)
	}
	if ev.Attempts >= w.maxAttempts{
		return "dead" , w.store.MarkDead(ctx, attempt)
	}
	wait := retry.Next(ev.Attempts , w.backOffBase, w.backOffMax)
	if res.RetryAfter > wait{
		wait = res.RetryAfter
	}
	if wait > w.backOffBase{
		wait = w.backOffBase
	}
	return "retry" , w.store.MarkRetry(ctx, attempt, time.Now().Add(wait))
}

func isSuccess(res Result) bool{
	return res.Err == nil && res.StatusCode >= 200 && res.StatusCode < 300
}

func toAttempt(ev domain.ClaimedEvent , res Result) domain.AttemptResult{
	errText := ""
	if res.Err != nil{
		errText = res.Err.Error()
	}else if res.StatusCode >= 300 || res.StatusCode < 200{
		errText = fmt.Sprintf("http %d", res.StatusCode)
	}
	return domain.AttemptResult{
		EventID: ev.ID,
		LeaseToken: ev.LeaseToken,
		AttemptNumber: ev.Attempts,
		StartedAt: res.StartedAt,
		FinishedAt: res.FinishedAt,
		DurationMs: int(res.FinishedAt.Sub(res.StartedAt).Milliseconds()),
		HTTPStatus: res.StatusCode,
		Error: errText,
	}
}
