package store

import (
	"context"
	"errors"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
)

// Store is the contract. The rest of the app depends on this, not on Postgres.
type Store interface {
	// ---- ingest ----

	// InsertEvent saves a new event. created = false means it was a duplicate (not an error).
	InsertEvent(ctx context.Context, e domain.NewEvent) (id int64, created bool, err error)

	// GetEvent returns one event with its delivery history. ErrNotFound if it doesn't exist.
	GetEvent(ctx context.Context, id int64) (domain.EventView, error)

	// Ping checks the database is reachable (for /healthz).
	Ping(ctx context.Context) error

	// ---- claiming ----

	// DueTenants lists tenants with at least one event ready to send now.
	DueTenants(ctx context.Context) ([]int64, error)

	// DueTenantsWeighted is DueTenants plus each tenant's DRR weight.
	DueTenantsWeighted(ctx context.Context) ([]domain.DueTenant, error)

	// ClaimNext claims the oldest due event for one tenant, respecting its cap and
	// skipping endpoints whose breaker is open. found = false means nothing to do.
	ClaimNext(ctx context.Context, tenantID int64, lease time.Duration) (ev domain.ClaimedEvent, found bool, err error)

	// ClaimOldest claims the oldest due event of ANY tenant, with no cap. FIFO baseline only.
	ClaimOldest(ctx context.Context, lease time.Duration) (domain.ClaimedEvent, bool, error)

	// ---- results ----

	// MarkDelivered, MarkRetry and MarkDead record the result of one HTTP try.
	// They return ErrLeaseLost if this worker's token is no longer current.
	MarkDelivered(ctx context.Context, r domain.AttemptResult) error
	MarkRetry(ctx context.Context, r domain.AttemptResult, nextAttemptAt time.Time) error
	MarkDead(ctx context.Context, r domain.AttemptResult) error

	GetEndpoint(ctx context.Context, id int64) (domain.Endpoint, error)

	// ---- circuit breaker ----

	// EndpointSucceeded closes the endpoint's breaker and resets its failure count.
	EndpointSucceeded(ctx context.Context, endpointID int64) error

	// EndpointFailed counts one failure. When the count reaches threshold (or a
	// half-open probe fails) the breaker opens until now + cooldown.
	// Returns the breaker state after the update.
	EndpointFailed(ctx context.Context, endpointID int64, threshold int, cooldown time.Duration) (state string, err error)

	// ---- background maintenance (run every few seconds) ----

	// ReclaimExpired puts IN_FLIGHT events with an expired lease back to PENDING,
	// or DEAD if they have used up maxAttempts. Returns how many rows changed.
	ReclaimExpired(ctx context.Context, maxAttempts int) (int64, error)

	// ProbeDueEndpoints moves OPEN breakers whose cooldown is over to HALF_OPEN,
	// which lets exactly one test delivery through. Returns how many changed.
	ProbeDueEndpoints(ctx context.Context) (int64, error)
}

// ErrUnknownEndpoint: the endpoint doesn't exist or belongs to another tenant.
var ErrUnknownEndpoint = errors.New("endpoint not found for this tenant")

// ErrLeaseLost: this worker's lease expired and the event was reclaimed.
// The worker must throw away its result.
var ErrLeaseLost = errors.New("lease lost: event was reclaimed by another worker")

// ErrNotFound: no row with that id.
var ErrNotFound = errors.New("not found")
