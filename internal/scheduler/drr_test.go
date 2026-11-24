package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

// fakeStore gives DRR an endless supply of events per tenant.
// Embedding store.Store satisfies the interface; only the two methods DRR uses are real.
type fakeStore struct {
	store.Store
	tenants []domain.DueTenant
	nextID  int64
	cap     int           // max claims per tenant per round (stands in for max_inflight)
	inRound map[int64]int // claims this round
}

func (f *fakeStore) DueTenantsWeighted(ctx context.Context) ([]domain.DueTenant, error) {
	f.inRound = map[int64]int{}
	return f.tenants, nil
}

func (f *fakeStore) ClaimNext(ctx context.Context, tenantID int64, lease time.Duration) (domain.ClaimedEvent, bool, error) {
	if f.inRound[tenantID] >= f.cap {
		return domain.ClaimedEvent{}, false, nil
	}
	f.inRound[tenantID]++
	f.nextID++
	return domain.ClaimedEvent{ID: f.nextID, TenantID: tenantID, LeaseToken: 1}, true, nil
}

// run plays rounds: every claimed event "takes" its tenant's cost, then reports back.
func run(t *testing.T, f *fakeStore, cost map[int64]time.Duration, rounds int) (claims map[int64]int, workTime map[int64]time.Duration) {
	t.Helper()
	d := NewDRR(f, time.Minute)
	claims = map[int64]int{}
	workTime = map[int64]time.Duration{}
	for i := 0; i < rounds; i++ {
		evs, err := d.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, ev := range evs {
			claims[ev.TenantID]++
			workTime[ev.TenantID] += cost[ev.TenantID]
			d.Observe(ev, cost[ev.TenantID])
		}
	}
	return claims, workTime
}

func ratio(a, b time.Duration) float64 { return float64(a) / float64(b) }

// A slow tenant (1 s per delivery) and a fast one (100 ms) with equal weight
// should get about the same WORKER-TIME, so the fast one gets ~10x the deliveries.
func TestDRRSharesWorkerTimeNotTurns(t *testing.T) {
	f := &fakeStore{tenants: []domain.DueTenant{{ID: 1, Weight: 1}, {ID: 2, Weight: 1}}, cap: 100}
	cost := map[int64]time.Duration{1: time.Second, 2: 100 * time.Millisecond}

	claims, work := run(t, f, cost, 2000)

	r := ratio(work[1], work[2])
	t.Logf("claims slow=%d fast=%d, worker-time slow=%v fast=%v (ratio %.2f)", claims[1], claims[2], work[1], work[2], r)
	if r < 0.8 || r > 1.25 {
		t.Fatalf("worker-time ratio %.2f, want about 1.0", r)
	}
}

// Weight 2 vs weight 1 with the same cost: about 2x the worker-time.
func TestDRRHonoursWeights(t *testing.T) {
	f := &fakeStore{tenants: []domain.DueTenant{{ID: 1, Weight: 2}, {ID: 2, Weight: 1}}, cap: 100}
	cost := map[int64]time.Duration{1: 100 * time.Millisecond, 2: 100 * time.Millisecond}

	claims, work := run(t, f, cost, 2000)

	r := ratio(work[1], work[2])
	t.Logf("claims heavy=%d light=%d (ratio %.2f)", claims[1], claims[2], r)
	if r < 1.7 || r > 2.3 {
		t.Fatalf("weighted ratio %.2f, want about 2.0", r)
	}
}

// One slow tenant alone must still be served every round (no idle workers
// just because it is "in debt").
func TestDRRIsWorkConserving(t *testing.T) {
	f := &fakeStore{tenants: []domain.DueTenant{{ID: 1, Weight: 1}}, cap: 1}
	cost := map[int64]time.Duration{1: 5 * time.Second}

	claims, _ := run(t, f, cost, 100)

	if claims[1] != 100 {
		t.Fatalf("lone tenant got %d claims in 100 rounds, want 100", claims[1])
	}
}
