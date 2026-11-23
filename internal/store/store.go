package store

import (
	"context"
	"errors"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
)

type Store interface {
	InsertEvent(ctx context.Context, e domain.NewEvent) (id int64, created bool, err error)

//ClaimNext Claims the Oldest Pending Event While Respecting its Capacity and returns it. If no event is available, returns nil.
// Found = false means(no due event or no free slot)
	ClaimNext(ctx context.Context, tenantID int64 , lease time.Duration ) (ev domain.ClaimedEvent, found bool,err error)

	ReclaimExpired(ctx context.Context) (int64, error)
}

var ErrUnknownEndpoint = errors.New("endpoint not found for this tenant")

