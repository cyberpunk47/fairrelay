package store

import (
	"context"
	"errors"
	"fmt"

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
