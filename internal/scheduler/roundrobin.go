package scheduler

import (
	"context"
	"log"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

type RoundRobin struct {
	store store.Store
	lease time.Duration
}

func NewRoundRobin(s store.Store, lease time.Duration) *RoundRobin {
	return &RoundRobin{
		store: s,
		lease: lease,
	}
}

func (rr *RoundRobin) Name() string {
	return "roundrobin"
}

func (rr *RoundRobin) Next(ctx context.Context) ([]domain.ClaimedEvent, error) {
	tenants, err := rr.store.DueTenants(ctx)
	if err != nil {
		return nil, err
	}
	var out []domain.ClaimedEvent
	for _, tenantID := range tenants {
		ev, found, err := rr.store.ClaimNext(ctx, tenantID, rr.lease)
		if err != nil {
			log.Printf("roundrobin: claim tenant %d: %v", tenantID, err)
			continue
		}
		if found {
			out = append(out, ev)
		}
	}
	return out, nil
}
