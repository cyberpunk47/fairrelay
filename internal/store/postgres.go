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

// ---------------- InsertEvent ----------------

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

	// 2. Tenant or endpoint doesn't exist, or they don't belong together.
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

// ---------------- ClaimNext ----------------

const lockTenantSQL = `
SELECT max_inflight FROM tenants WHERE id = $1 FOR UPDATE`

const countInFlightSQL = `
SELECT count(*) FROM events
WHERE tenant_id = $1 AND status = 'IN_FLIGHT' AND lease_until > now()`

// The inner SELECT picks one row; the outer UPDATE claims it.
const claimSQL = `
UPDATE events
SET status      = 'IN_FLIGHT',
    lease_until = now() + make_interval(secs => $2),
    lease_token = lease_token + 1,
    attempts    = attempts + 1
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
	var ev domain.ClaimedEvent

	// 1. BEGIN
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return ev, false, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx) // undoes everything on an early return; does nothing after Commit

	// 2. Lock the tenant row and read its cap
	var maxInflight int
	err = tx.QueryRow(ctx, lockTenantSQL, tenantID).Scan(&maxInflight)
	if errors.Is(err, pgx.ErrNoRows) {
		return ev, false, nil // tenant doesn't exist
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

// ---------------- ReclaimExpired ----------------

const reclaimExpiredSQL = `
UPDATE events
SET status      = CASE WHEN attempts >= $1 THEN 'DEAD' ELSE 'PENDING' END,
    lease_until = NULL,
    last_error  = 'lease expired'
WHERE status = 'IN_FLIGHT' AND lease_until < now()`

func (p *Postgres) ReclaimExpired(ctx context.Context, maxAttempts int) (int64, error) {
	tag, err := p.pool.Exec(ctx, reclaimExpiredSQL, maxAttempts)
	if err != nil {
		return 0, fmt.Errorf("reclaim expired: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ---------------- Results: delivered / retry / dead ----------------

const insertAttemptSQL = `
INSERT INTO delivery_attempts
    (event_ref, attempt_number, lease_token, started_at, finished_at, http_status, error, duration_ms)
VALUES ($1, $2, $3, $4, $5, NULLIF($6, 0), NULLIF($7, ''), $8)
ON CONFLICT (event_ref, lease_token) DO NOTHING`

// All three end with the same WHERE: the token check (fencing).
const markDeliveredSQL = `
UPDATE events
SET status       = 'DELIVERED',
    delivered_at = now(),
    lease_until  = NULL,
    last_error   = NULL
WHERE id = $1 AND lease_token = $2 AND status = 'IN_FLIGHT'`

const markRetrySQL = `
UPDATE events
SET status          = 'PENDING',
    next_attempt_at = $3,
    last_error      = $4,
    lease_until     = NULL
WHERE id = $1 AND lease_token = $2 AND status = 'IN_FLIGHT'`

const markDeadSQL = `
UPDATE events
SET status      = 'DEAD',
    last_error  = $3,
    lease_until = NULL
WHERE id = $1 AND lease_token = $2 AND status = 'IN_FLIGHT'`

// insertAttempt writes the history row inside an existing transaction.
func insertAttempt(ctx context.Context, tx pgx.Tx, r domain.AttemptResult) error {
	_, err := tx.Exec(ctx, insertAttemptSQL,
		r.EventID, r.AttemptNumber, r.LeaseToken, r.StartedAt, r.FinishedAt,
		r.HTTPStatus, r.Error, r.DurationMs,
	)
	if err != nil {
		return fmt.Errorf("insert attempt: %w", err)
	}
	return nil
}

func (p *Postgres) MarkDelivered(ctx context.Context, r domain.AttemptResult) error {
	// 1. BEGIN
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	// 2. Undo on early return
	defer tx.Rollback(ctx)

	// 3. Record the attempt (it really happened, even if our lease was lost)
	if err := insertAttempt(ctx, tx, r); err != nil {
		return err
	}

	// 4. Update the event only if our token is still current
	tag, err := tx.Exec(ctx, markDeliveredSQL, r.EventID, r.LeaseToken)
	if err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}

	// 5. COMMIT
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// 6. 0 rows changed = someone else holds the event now
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (p *Postgres) MarkRetry(ctx context.Context, r domain.AttemptResult, nextAttemptAt time.Time) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := insertAttempt(ctx, tx, r); err != nil {
		return err
	}

	tag, err := tx.Exec(ctx, markRetrySQL, r.EventID, r.LeaseToken, nextAttemptAt, r.Error)
	if err != nil {
		return fmt.Errorf("mark retry: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (p *Postgres) MarkDead(ctx context.Context, r domain.AttemptResult) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := insertAttempt(ctx, tx, r); err != nil {
		return err
	}

	tag, err := tx.Exec(ctx, markDeadSQL, r.EventID, r.LeaseToken, r.Error)
	if err != nil {
		return fmt.Errorf("mark dead: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

const getEndpointSQL = `
SELECT id , tenant_id, url, secret from 
endpoints WHERE id = $1`

func (p *Postgres) GetEndpoint(ctx context.Context, id int64) (domain.Endpoint, error) {
	var ep domain.Endpoint
	err := p.pool.QueryRow(ctx, getEndpointSQL , id).Scan(&ep.ID , &ep.TenantID , &ep.URL, &ep.Secret)
	if err != nil {
		return ep, fmt.Errorf("get endpoint %d: %w", id, err)
	}
	return ep, nil
}

