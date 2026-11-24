package store

// Integration tests: they run against a real PostgreSQL with the migration applied.
// Skipped unless TEST_DATABASE_URL is set, e.g.
//   $env:TEST_DATABASE_URL = "postgres://fairrelay:fairrelay@localhost:5433/fairrelay?sslmode=disable"
//   go test ./internal/store/ -v

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
)

func testDB(t *testing.T) *Postgres {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	pg, err := NewPostgres(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)
	return pg
}

// newTenant creates a tenant (with the given cap) and one endpoint, both unique to this test.
func newTenant(t *testing.T, pg *Postgres, maxInflight int) (tenantID, endpointID int64) {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())
	err := pg.pool.QueryRow(ctx,
		`INSERT INTO tenants (name, max_inflight) VALUES ($1, $2) RETURNING id`, name, maxInflight).Scan(&tenantID)
	if err != nil {
		t.Fatal(err)
	}
	err = pg.pool.QueryRow(ctx,
		`INSERT INTO endpoints (tenant_id, url, secret) VALUES ($1, 'http://127.0.0.1:1/x', 's') RETURNING id`, tenantID).Scan(&endpointID)
	if err != nil {
		t.Fatal(err)
	}
	return tenantID, endpointID
}

func addEvents(t *testing.T, pg *Postgres, tenantID, endpointID int64, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		_, _, err := pg.InsertEvent(context.Background(), domain.NewEvent{
			TenantID: tenantID, EndpointID: endpointID,
			ExternalID: fmt.Sprintf("evt_%d", i), EventType: "test", Payload: `{}`,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestInsertEventIsIdempotent(t *testing.T) {
	pg := testDB(t)
	tid, eid := newTenant(t, pg, 2)
	e := domain.NewEvent{TenantID: tid, EndpointID: eid, ExternalID: "same", EventType: "x", Payload: `{}`}

	id1, created1, err := pg.InsertEvent(context.Background(), e)
	if err != nil || !created1 {
		t.Fatalf("first insert: created=%v err=%v", created1, err)
	}
	_, created2, err := pg.InsertEvent(context.Background(), e)
	if err != nil || created2 {
		t.Fatalf("second insert should be a duplicate: created=%v err=%v", created2, err)
	}

	_, _, err = pg.InsertEvent(context.Background(), domain.NewEvent{TenantID: tid, EndpointID: eid + 999999, ExternalID: "x", EventType: "x", Payload: `{}`})
	if !errors.Is(err, ErrUnknownEndpoint) {
		t.Fatalf("unknown endpoint: got %v", err)
	}
	t.Logf("event %d stored once", id1)
}

func TestClaimRespectsTenantCap(t *testing.T) {
	pg := testDB(t)
	tid, eid := newTenant(t, pg, 2)
	addEvents(t, pg, tid, eid, 5)

	for i := 1; i <= 2; i++ {
		if _, found, err := pg.ClaimNext(context.Background(), tid, time.Minute); err != nil || !found {
			t.Fatalf("claim %d: found=%v err=%v", i, found, err)
		}
	}
	if _, found, _ := pg.ClaimNext(context.Background(), tid, time.Minute); found {
		t.Fatal("third claim succeeded; cap of 2 not enforced")
	}
}

func TestStaleWorkerIsFenced(t *testing.T) {
	pg := testDB(t)
	ctx := context.Background()
	tid, eid := newTenant(t, pg, 1)
	addEvents(t, pg, tid, eid, 1)

	old, _, _ := pg.ClaimNext(ctx, tid, time.Minute)

	// The first worker stalls: its lease expires and recovery hands the event to someone else.
	if _, err := pg.pool.Exec(ctx, `UPDATE events SET lease_until = now() - interval '1 second' WHERE id = $1`, old.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pg.ReclaimExpired(ctx, 8); err != nil {
		t.Fatal(err)
	}
	cur, found, _ := pg.ClaimNext(ctx, tid, time.Minute)
	if !found || cur.ID != old.ID || cur.LeaseToken != old.LeaseToken+1 {
		t.Fatalf("re-claim: found=%v id=%d token=%d", found, cur.ID, cur.LeaseToken)
	}

	result := func(ev domain.ClaimedEvent) domain.AttemptResult {
		now := time.Now()
		return domain.AttemptResult{EventID: ev.ID, LeaseToken: ev.LeaseToken, AttemptNumber: ev.Attempts, StartedAt: now, FinishedAt: now, HTTPStatus: 200}
	}
	if err := pg.MarkDelivered(ctx, result(old)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale worker: got %v, want ErrLeaseLost", err)
	}
	if err := pg.MarkDelivered(ctx, result(cur)); err != nil {
		t.Fatalf("current worker: %v", err)
	}
}

func breakerState(t *testing.T, pg *Postgres, endpointID int64) string {
	t.Helper()
	var s string
	if err := pg.pool.QueryRow(context.Background(), `SELECT breaker_state FROM endpoints WHERE id = $1`, endpointID).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestBreakerOpensProbesAndCloses(t *testing.T) {
	pg := testDB(t)
	ctx := context.Background()
	tid, eid := newTenant(t, pg, 2)
	addEvents(t, pg, tid, eid, 5)

	// 1. Three failures in a row (threshold 3) open the breaker
	for i := 1; i <= 3; i++ {
		state, err := pg.EndpointFailed(ctx, eid, 3, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		want := domain.BreakerClosed
		if i == 3 {
			want = domain.BreakerOpen
		}
		if state != want {
			t.Fatalf("after failure %d: state %s, want %s", i, state, want)
		}
	}

	// 2. OPEN: nothing is claimed
	if _, found, _ := pg.ClaimNext(ctx, tid, time.Minute); found {
		t.Fatal("claimed while breaker OPEN")
	}

	// 3. Cooldown not over yet: no probe
	if n, _ := pg.ProbeDueEndpoints(ctx); n != 0 {
		t.Fatalf("probed %d endpoints before cooldown", n)
	}

	// 4. Cooldown over -> HALF_OPEN -> exactly one probe, even though the cap is 2
	if _, err := pg.pool.Exec(ctx, `UPDATE endpoints SET next_probe_at = now() - interval '1 second' WHERE id = $1`, eid); err != nil {
		t.Fatal(err)
	}
	if n, _ := pg.ProbeDueEndpoints(ctx); n != 1 {
		t.Fatalf("probe due: %d, want 1", n)
	}
	if _, found, _ := pg.ClaimNext(ctx, tid, time.Minute); !found {
		t.Fatal("probe not claimed in HALF_OPEN")
	}
	if _, found, _ := pg.ClaimNext(ctx, tid, time.Minute); found {
		t.Fatal("second event claimed while the probe is in flight")
	}

	// 5. Probe fails -> OPEN again
	if state, _ := pg.EndpointFailed(ctx, eid, 3, time.Hour); state != domain.BreakerOpen {
		t.Fatalf("failed probe: state %s, want OPEN", state)
	}

	// 6. A success closes it and claims flow again
	if err := pg.EndpointSucceeded(ctx, eid); err != nil {
		t.Fatal(err)
	}
	if s := breakerState(t, pg, eid); s != domain.BreakerClosed {
		t.Fatalf("after success: %s, want CLOSED", s)
	}
	if _, found, _ := pg.ClaimNext(ctx, tid, time.Minute); !found {
		t.Fatal("no claim after breaker closed")
	}
}

func TestGetEventShowsHistory(t *testing.T) {
	pg := testDB(t)
	ctx := context.Background()
	tid, eid := newTenant(t, pg, 1)
	addEvents(t, pg, tid, eid, 1)
	ev, _, _ := pg.ClaimNext(ctx, tid, time.Minute)

	now := time.Now()
	r := domain.AttemptResult{EventID: ev.ID, LeaseToken: ev.LeaseToken, AttemptNumber: ev.Attempts, StartedAt: now, FinishedAt: now, Error: "http 500", HTTPStatus: 500}
	if err := pg.MarkRetry(ctx, r, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	v, err := pg.GetEvent(ctx, ev.ID)
	if err != nil {
		t.Fatal(err)
	}
	if v.Status != domain.StatusPending || v.Attempts != 1 || len(v.History) != 1 || *v.History[0].HTTPStatus != 500 {
		t.Fatalf("unexpected view: %+v", v)
	}
	if _, err := pg.GetEvent(ctx, -1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing event: got %v, want ErrNotFound", err)
	}
}
