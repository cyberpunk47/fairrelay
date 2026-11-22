package store 
import (
	"context"
	"fmt"
	"errors"

	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)
type Postgres struct{
	pool *pgxpool.Pool
}
var _ Store = (*Postgres)(nil)
func NewPostgres(ctx context.Context, url string) (*Postgres, error) {
	pool , err := pgxpool.New(ctx, url)
	if err != nil {
		return nil , fmt.Errorf("open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil , fmt.Errorf("ping db : %w", err)
	}
	return &Postgres{pool: pool}, nil
}
func (p *Postgres) Close(){
	p.pool.Close()
}
func (p *Postgres) InsertEvent(ctx context.Context , e domain.NewEvent) (id int64 ,created bool, err error){
	const sql = `
	INSERT INTO events(tenant_id , endpoint_id , external_id , event_type , payload) values($1,$2,$3,$4,$5)
ON CONFLICT(tenant_id , external_id) DO NOTHING	RETURNING id
	`
	err = p.pool.QueryRow(ctx, sql, e.TenantID , e.EndpointID , e.ExternalID , e.EventType , e.Payload).Scan(&id)
	if errors.Is(err , pgx.ErrNoRows){
		return 0 , false , nil
	}
	if err != nil {
		return 0 , false , fmt.Errorf("insert event: %w", err)
	}
	return id , true , nil
}