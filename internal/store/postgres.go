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

// Is at least one of this tenant's endpoints accepting deliveries?
// Cheap early exit: if every endpoint's breaker is OPEN we skip the events scan entirely.
const anyEndpointUsableSQL = `
SELECT EXISTS (
    SELECT 1 FROM endpoints
    WHERE tenant_id = $1 AND breaker_state IN ('CLOSED', 'HALF_OPEN')
)`

const countInFlightSQL = `
SELECT count(*) FROM events
WHERE tenant_id = $1 AND status = 'IN_FLIGHT' AND lease_until > now()`

// breakerAllows is the circuit-breaker rule, shared by both claim queries:
//
//	CLOSED    -> send
//	HALF_OPEN -> send only if nothing is in flight to this endpoint (one probe at a time)
//	OPEN      -> don't send
const breakerAllows = `
      AND (ep.breaker_state = 'CLOSED'
           OR (ep.breaker_state = 'HALF_OPEN'
               AND NOT EXISTS (SELECT 1 FROM events f
                               WHERE f.tenant_id = e.tenant_id
                                 AND f.endpoint_id = ep.id
                                 AND f.status = 'IN_FLIGHT'
                                 AND f.lease_until > now())))`

// The inner SELECT picks one row; the outer UPDATE claims it.
const claimSQL = `
UPDATE events
SET status      = 'IN_FLIGHT',
    lease_until = now() + make_interval(secs => $2),
    lease_token = lease_token + 1,
    attempts    = attempts + 1
WHERE id = (
    SELECT e.id FROM events e
    JOIN endpoints ep ON ep.id = e.endpoint_id
    WHERE e.tenant_id = $1
      AND e.status = 'PENDING'
      AND e.next_attempt_at <= now()` + breakerAllows + `
    ORDER BY e.next_attempt_at
    LIMIT 1
    FOR UPDATE OF e SKIP LOCKED
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

	// 3. Any endpoint with its breaker not OPEN? If not, nothing can be sent.
	var usable bool
	if err := tx.QueryRow(ctx, anyEndpointUsableSQL, tenantID).Scan(&usable); err != nil {
		return ev, false, fmt.Errorf("check endpoints: %w", err)
	}
	if !usable {
		return ev, false, nil
	}

	// 4. Count events in flight
	var inFlight int
	if err := tx.QueryRow(ctx, countInFlightSQL, tenantID).Scan(&inFlight); err != nil {
		return ev, false, fmt.Errorf("count in-flight: %w", err)
	}

	// 5. Cap full?
	if inFlight >= maxInflight {
		return ev, false, nil
	}

	// 6. Claim the oldest due event whose endpoint's breaker allows it
	err = tx.QueryRow(ctx, claimSQL, tenantID, lease.Seconds()).Scan(
		&ev.ID, &ev.TenantID, &ev.EndpointID, &ev.ExternalID,
		&ev.EventType, &ev.Payload, &ev.Attempts, &ev.LeaseToken,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ev, false, nil // 7. nothing due
	}
	if err != nil {
		return ev, false, fmt.Errorf("claim: %w", err)
	}

	// 8. COMMIT
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
SELECT id, tenant_id, url, secret FROM endpoints WHERE id = $1`

func (p *Postgres) GetEndpoint(ctx context.Context, id int64) (domain.Endpoint, error) {
	var ep domain.Endpoint
	err := p.pool.QueryRow(ctx, getEndpointSQL, id).Scan(&ep.ID, &ep.TenantID, &ep.URL, &ep.Secret)
	if err != nil {
		return ep, fmt.Errorf("get endpoint %d: %w", id, err)
	}
	return ep, nil
}

const dueTenantsSQL = `SELECT DISTINCT tenant_id FROM events
WHERE status = 'PENDING' AND next_attempt_at <= now()
ORDER BY tenant_id`

func (p *Postgres) DueTenants(ctx context.Context) ([]int64, error) {
	rows, err := p.pool.Query(ctx, dueTenantsSQL)
	if err != nil {
		return nil, fmt.Errorf("due tenants: %w", err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, fmt.Errorf("due tenants: %w", err)
	}
	return ids, nil
}

const claimOldestSQL = `
UPDATE events
SET status      = 'IN_FLIGHT',
    lease_until = now() + make_interval(secs => $1),
    lease_token = lease_token + 1,
    attempts    = attempts + 1
WHERE id = (
    SELECT e.id FROM events e
    JOIN endpoints ep ON ep.id = e.endpoint_id
    WHERE e.status = 'PENDING'
      AND e.next_attempt_at <= now()` + breakerAllows + `
    ORDER BY e.next_attempt_at
    LIMIT 1
    FOR UPDATE OF e SKIP LOCKED
)
RETURNING id, tenant_id, endpoint_id, external_id, event_type, payload, attempts, lease_token`

func (p *Postgres) ClaimOldest(ctx context.Context, lease time.Duration) (domain.ClaimedEvent, bool, error) {
	var ev domain.ClaimedEvent
	err := p.pool.QueryRow(ctx, claimOldestSQL, lease.Seconds()).Scan(
		&ev.ID, &ev.TenantID, &ev.EndpointID, &ev.ExternalID,
		&ev.EventType, &ev.Payload, &ev.Attempts, &ev.LeaseToken,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ev, false, nil // nothing due
	}
	if err != nil {
		return ev, false, fmt.Errorf("claim oldest: %w", err)
	}
	return ev, true, nil
}

// ---------------- DueTenantsWeighted (for DRR) ----------------

const dueTenantsWeightedSQL = `
SELECT t.id, t.weight FROM tenants t
WHERE EXISTS (
    SELECT 1 FROM events e
    WHERE e.tenant_id = t.id AND e.status = 'PENDING' AND e.next_attempt_at <= now()
)
ORDER BY t.id`

func (p *Postgres) DueTenantsWeighted(ctx context.Context) ([]domain.DueTenant, error) {
	rows, err := p.pool.Query(ctx, dueTenantsWeightedSQL)
	if err != nil {
		return nil, fmt.Errorf("due tenants weighted: %w", err)
	}
	defer rows.Close()

	var out []domain.DueTenant
	for rows.Next() {
		var t domain.DueTenant
		if err := rows.Scan(&t.ID, &t.Weight); err != nil {
			return nil, fmt.Errorf("due tenants weighted: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ---------------- Circuit breaker ----------------

// Only writes when something actually changes, so the normal "healthy endpoint,
// another success" case costs no row update.
const endpointSucceededSQL = `
UPDATE endpoints
SET breaker_state = 'CLOSED', consecutive_failures = 0, next_probe_at = NULL
WHERE id = $1 AND (breaker_state <> 'CLOSED' OR consecutive_failures <> 0)`

func (p *Postgres) EndpointSucceeded(ctx context.Context, endpointID int64) error {
	if _, err := p.pool.Exec(ctx, endpointSucceededSQL, endpointID); err != nil {
		return fmt.Errorf("endpoint succeeded: %w", err)
	}
	return nil
}

// Every column on the right-hand side is the OLD value (before this UPDATE).
// The breaker opens when:
//   - this failure reaches the threshold, or
//   - the endpoint was HALF_OPEN (its one probe just failed).
//
// Opening sets next_probe_at = now + cooldown.
const endpointFailedSQL = `
UPDATE endpoints
SET consecutive_failures = consecutive_failures + 1,
    breaker_state = CASE
        WHEN breaker_state = 'HALF_OPEN' OR consecutive_failures + 1 >= $2 THEN 'OPEN'
        ELSE breaker_state END,
    next_probe_at = CASE
        WHEN breaker_state = 'HALF_OPEN' OR consecutive_failures + 1 >= $2
            THEN now() + make_interval(secs => $3)
        ELSE next_probe_at END
WHERE id = $1
RETURNING breaker_state`

func (p *Postgres) EndpointFailed(ctx context.Context, endpointID int64, threshold int, cooldown time.Duration) (string, error) {
	var state string
	err := p.pool.QueryRow(ctx, endpointFailedSQL, endpointID, threshold, cooldown.Seconds()).Scan(&state)
	if err != nil {
		return "", fmt.Errorf("endpoint failed: %w", err)
	}
	return state, nil
}

const probeDueEndpointsSQL = `
UPDATE endpoints SET breaker_state = 'HALF_OPEN'
WHERE breaker_state = 'OPEN' AND next_probe_at <= now()`

func (p *Postgres) ProbeDueEndpoints(ctx context.Context) (int64, error) {
	tag, err := p.pool.Exec(ctx, probeDueEndpointsSQL)
	if err != nil {
		return 0, fmt.Errorf("probe due endpoints: %w", err)
	}
	return tag.RowsAffected(), nil
}

// ---------------- GetEvent / Ping ----------------

const getEventSQL = `
SELECT id, tenant_id, endpoint_id, external_id, event_type, status, attempts,
       next_attempt_at, last_error, delivered_at, created_at
FROM events WHERE id = $1`

const getAttemptsSQL = `
SELECT attempt_number, started_at, finished_at, duration_ms, http_status, error
FROM delivery_attempts WHERE event_ref = $1
ORDER BY started_at`

func (p *Postgres) GetEvent(ctx context.Context, id int64) (domain.EventView, error) {
	var v domain.EventView
	err := p.pool.QueryRow(ctx, getEventSQL, id).Scan(
		&v.ID, &v.TenantID, &v.EndpointID, &v.EventID, &v.EventType, &v.Status, &v.Attempts,
		&v.NextAttemptAt, &v.LastError, &v.DeliveredAt, &v.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, ErrNotFound
	}
	if err != nil {
		return v, fmt.Errorf("get event: %w", err)
	}

	rows, err := p.pool.Query(ctx, getAttemptsSQL, id)
	if err != nil {
		return v, fmt.Errorf("get attempts: %w", err)
	}
	defer rows.Close()

	v.History = []domain.AttemptView{} // [] in JSON, not null, when there are no tries yet
	for rows.Next() {
		var a domain.AttemptView
		if err := rows.Scan(&a.AttemptNumber, &a.StartedAt, &a.FinishedAt, &a.DurationMs, &a.HTTPStatus, &a.Error); err != nil {
			return v, fmt.Errorf("get attempts: %w", err)
		}
		v.History = append(v.History, a)
	}
	return v, rows.Err()
}

func (p *Postgres) Ping(ctx context.Context) error {
	return p.pool.Ping(ctx)
}
