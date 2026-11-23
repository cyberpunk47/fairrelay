package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is the real Store, backed by a pool of database connections.
type Postgres struct {
	pool *pgxpool.Pool
}

// Compile-time check: *Postgres must have every method that Store requires.
var _ Store = (*Postgres)(nil)

// Postgres error code for "foreign key violation".
const pgForeignKeyViolation = "23503"

func NewPostgres(ctx context.Context, url string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close() {
	p.pool.Close()
}

const insertEventSQL = `
INSERT INTO events (tenant_id, endpoint_id, external_id, event_type, payload)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, external_id) DO NOTHING
RETURNING id`

func (p *Postgres) InsertEvent(ctx context.Context, e domain.NewEvent) (int64, bool, error) {
	var id int64
	err := p.pool.QueryRow(ctx, insertEventSQL,
		e.TenantID, e.EndpointID, e.ExternalID, e.EventType, e.Payload,
	).Scan(&id)

	// 1. Duplicate: ON CONFLICT DO NOTHING returned no row. Not an error.
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}

	// 2. The tenant or endpoint doesn't exist, or they don't belong together.
	//    Postgres reports code 23503; turn it into our own named error.
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgForeignKeyViolation {
		return 0, false, ErrUnknownEndpoint
	}

	// 3. Anything else is a real failure.
	if err != nil {
		return 0, false, fmt.Errorf("insert event: %w", err)
	}
	return id, true, nil
}

const lockTenantSQL = `
SELECT max_inflight FROM tenants WHERE id = $1 FOR UPDATE`

const countInFlightSQL = `
SELECT count(*) FROM events
WHERE tenant_id = $1 AND status = 'IN_FLIGHT' AND lease_until > now()`

// The inner SELECT picks one row; the outer UPDATE claims it.
const claimSQL = `
UPDATE events
SET status       = 'IN_FLIGHT',
    lease_until  = now() + make_interval(secs => $2),
    lease_token  = lease_token + 1,
    attempts     = attempts + 1
WHERE id = (
    SELECT id FROM events
    WHERE tenant_id = $1
      AND status = 'PENDING'
      AND next_attempt_at <= now()
    ORDER BY next_attempt_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, tenant_id, endpoint_id, external_id, event_type, payload, attempts, lease_token`

func (p *Postgres) ClaimNext(ctx context.Context, tenantID int64, lease time.Duration) (domain.ClaimedEvent, bool, error) {
	// 0. Prepare return value
	var ev domain.ClaimedEvent

	// 1. BEGIN
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return ev, false, fmt.Errorf("begin: %w", err)
	}
	// If we return early, this undoes everything and releases the locks.
	// After a successful Commit it does nothing.
	defer tx.Rollback(ctx)

	// 2. Lock the tenant row and read its cap
	var maxInflight int
	err = tx.QueryRow(ctx, lockTenantSQL, tenantID).Scan(&maxInflight)
	if errors.Is(err, pgx.ErrNoRows) {
		return ev, false, nil // tenant doesn't exist: nothing to do
	}
	if err != nil {
		return ev, false, fmt.Errorf("lock tenant: %w", err)
	}

	// 3. Count events in flight
	var inFlight int
	if err := tx.QueryRow(ctx, countInFlightSQL, tenantID).Scan(&inFlight); err != nil {
		return ev, false, fmt.Errorf("count in-flight: %w", err)
	}

	// 4. Cap full?
	if inFlight >= maxInflight {
		return ev, false, nil
	}

	// 5. Claim the oldest due event
	err = tx.QueryRow(ctx, claimSQL, tenantID, lease.Seconds()).Scan(
		&ev.ID, &ev.TenantID, &ev.EndpointID, &ev.ExternalID,
		&ev.EventType, &ev.Payload, &ev.Attempts, &ev.LeaseToken,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ev, false, nil // 6. nothing due
	}
	if err != nil {
		return ev, false, fmt.Errorf("claim: %w", err)
	}

	// 7. COMMIT
	if err := tx.Commit(ctx); err != nil {
		return ev, false, fmt.Errorf("commit: %w", err)
	}
	return ev, true, nil
}