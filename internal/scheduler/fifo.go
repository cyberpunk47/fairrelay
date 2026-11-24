package scheduler

import (
	"context"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

type FIFO struct {
	store store.Store
	lease time.Duration
}

func NewFIFO(s store.Store, lease time.Duration) *FIFO {
	return &FIFO{
		store: s,
		lease: lease,
	}
}

func (f *FIFO) Name() string {
	return "fifo"
}

func (f *FIFO) Next(ctx context.Context) ([]domain.ClaimedEvent, error) {
	ev, found, err := f.store.ClaimOldest(ctx, f.lease)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return []domain.ClaimedEvent{ev}, nil
}
