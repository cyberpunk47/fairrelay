package scheduler

import (
	"context"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
)

// Policy decides which events to claim next (Strategy pattern):
// fifo, roundrobin and drr are interchangeable behind this interface.
type Policy interface {
	Next(ctx context.Context) ([]domain.ClaimedEvent, error)
	Name() string
}

// Observer is optional. A Policy that also implements it is told how long each
// delivery took (DRR uses this to charge tenants for real worker-time).
type Observer interface {
	Observe(ev domain.ClaimedEvent, took time.Duration)
}
