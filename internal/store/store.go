package store

import (
	"context"
	"errors"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
)

// Store is the contract. The rest of the app depends on this, not on Postgres.
type Store interface {
	// InsertEvent saves a new event. created = false means it was a duplicate (not an error).
	InsertEvent(ctx context.Context, e domain.NewEvent) (id int64, created bool, err error)

	// ClaimNext claims the oldest due event for one tenant, respecting its cap.
	// found = false means nothing to do (no free slot, or no due event).
	ClaimNext(ctx context.Context, tenantID int64, lease time.Duration) (ev domain.ClaimedEvent, found bool, err error)

	// ReclaimExpired puts IN_FLIGHT events with an expired lease back to PENDING,
	// or DEAD if they have used up maxAttempts. Returns how many rows changed.
	ReclaimExpired(ctx context.Context, maxAttempts int) (int64, error)

	// MarkDelivered, MarkRetry and MarkDead record the result of one HTTP try.
	// They return ErrLeaseLost if this worker's token is no longer current.
	MarkDelivered(ctx context.Context, r domain.AttemptResult) error
	MarkRetry(ctx context.Context, r domain.AttemptResult, nextAttemptAt time.Time) error
	MarkDead(ctx context.Context, r domain.AttemptResult) error
	GetEndpoint(ctx context.Context, id int64) (domain.Endpoint, error)
	DueTenants(ctx context.Context)([]int64, error)
}

// ErrUnknownEndpoint: the endpoint doesn't exist or belongs to another tenant.
var ErrUnknownEndpoint = errors.New("endpoint not found for this tenant")

// ErrLeaseLost: this worker's lease expired and the event was reclaimed.
// The worker must throw away its result.
var ErrLeaseLost = errors.New("lease lost: event was reclaimed by another worker")
