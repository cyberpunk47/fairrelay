package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

// DRR is Deficit Round Robin, measured in worker-time instead of packets.
//
// Plain round robin gives every tenant one turn per round. That is fair in
// turns, not in work: a tenant whose endpoint takes 2 s per delivery holds a
// worker 200x longer per turn than one that answers in 10 ms.
//
// DRR fixes that with a credit counter (the "deficit") per tenant:
//  1. Each round, every tenant with due events earns quantum x weight seconds of credit.
//  2. A tenant may claim events while its credit is above zero.
//  3. Each claim spends the tenant's estimated delivery time; when the delivery
//     finishes, Observe corrects the charge to the real time taken.
//
// Over time each tenant gets worker-time in proportion to its weight,
// however fast or slow its endpoint is. The per-tenant cap (max_inflight)
// still applies on top: DRR decides the order, the cap stays the hard limit.
//
// State lives in this process. With several dispatchers each one is fair on its
// own share of work; the cap is enforced globally by the database.
type DRR struct {
	store store.Store
	lease time.Duration

	mu       sync.Mutex
	deficit  map[int64]float64    // seconds of worker-time the tenant may still spend (negative = debt)
	estimate map[int64]float64    // moving average of the tenant's delivery time, in seconds
	charged  map[claimKey]float64 // what each in-flight claim was charged, for Observe to correct
	start    int                  // rotates which tenant picks first each round
}

type claimKey struct {
	eventID int64
	token   int
}

const (
	drrQuantum     = 0.1  // seconds of worker-time earned per round, per unit of weight
	drrDefaultCost = 0.1  // assumed delivery time (s) before we have measured a tenant
	drrAlpha       = 0.2  // moving-average weight given to each new measurement
	drrMaxDebt     = 60.0 // seconds; caps how far a tenant can fall behind, so it is never starved forever
)

func NewDRR(s store.Store, lease time.Duration) *DRR {
	return &DRR{
		store:    s,
		lease:    lease,
		deficit:  map[int64]float64{},
		estimate: map[int64]float64{},
		charged:  map[claimKey]float64{},
	}
}

func (d *DRR) Name() string { return "drr" }

// Next runs one DRR round and returns the events it claimed.
func (d *DRR) Next(ctx context.Context) ([]domain.ClaimedEvent, error) {
	due, err := d.store.DueTenantsWeighted(ctx)
	if err != nil {
		return nil, err
	}

	order := d.startRound(due)

	var out []domain.ClaimedEvent
	for _, t := range order {
		for d.credit(t.ID) > 0 {
			ev, found, err := d.store.ClaimNext(ctx, t.ID, d.lease)
			if err != nil {
				log.Printf("drr: claim tenant %d: %v", t.ID, err)
				break
			}
			if !found {
				break // cap full, breaker open, or nothing due
			}
			d.charge(ev)
			out = append(out, ev)
		}
	}
	return out, nil
}

// startRound hands out this round's credit and returns the tenants in the order
// they pick (rotated each round, so nobody always goes first).
func (d *DRR) startRound(due []domain.DueTenant) []domain.DueTenant {
	d.mu.Lock()
	defer d.mu.Unlock()

	// A tenant with nothing due keeps no spare credit (classic DRR), so it can't
	// save up credit while idle and then flood. Debt is kept: it was earned.
	isDue := map[int64]bool{}
	for _, t := range due {
		isDue[t.ID] = true
	}
	for id, v := range d.deficit {
		if !isDue[id] && v > 0 {
			delete(d.deficit, id)
		}
	}

	// 1. Everyone due earns quantum x weight. Spare credit is capped at one
	//    round's worth, again so it can't pile up.
	for _, t := range due {
		d.addCredit(t, 1)
	}

	// 2. Work-conserving rule: if every due tenant is still in debt, nobody could
	//    be served and workers would sit idle. Skip ahead the smallest number of
	//    rounds that puts at least one tenant back above zero. Everyone gets the
	//    same number of rounds, so the proportions stay fair.
	if len(due) > 0 && d.allInDebt(due) {
		skip := -1
		for _, t := range due {
			perRound := drrQuantum * float64(weightOf(t))
			rounds := int(-d.deficit[t.ID]/perRound) + 1
			if skip == -1 || rounds < skip {
				skip = rounds
			}
		}
		for _, t := range due {
			d.addCredit(t, skip)
		}
	}

	// 3. Rotate the starting tenant
	if len(due) == 0 {
		return nil
	}
	first := d.start % len(due)
	d.start++
	order := make([]domain.DueTenant, 0, len(due))
	order = append(order, due[first:]...)
	order = append(order, due[:first]...)
	return order
}

func (d *DRR) addCredit(t domain.DueTenant, rounds int) {
	perRound := drrQuantum * float64(weightOf(t))
	v := d.deficit[t.ID] + perRound*float64(rounds)
	if v > perRound {
		v = perRound
	}
	d.deficit[t.ID] = v
}

func (d *DRR) allInDebt(due []domain.DueTenant) bool {
	for _, t := range due {
		if d.deficit[t.ID] > 0 {
			return false
		}
	}
	return true
}

func (d *DRR) credit(tenantID int64) float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deficit[tenantID]
}

// charge spends the tenant's estimated delivery time for one claimed event.
func (d *DRR) charge(ev domain.ClaimedEvent) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cost := d.estimateFor(ev.TenantID)
	d.deficit[ev.TenantID] -= cost
	d.charged[claimKey{ev.ID, ev.LeaseToken}] = cost
}

// Observe is called by a worker when a delivery finishes. It replaces the
// estimate charged at claim time with the real time taken, and updates the
// tenant's moving average.
func (d *DRR) Observe(ev domain.ClaimedEvent, took time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()

	actual := took.Seconds()
	key := claimKey{ev.ID, ev.LeaseToken}
	if cost, ok := d.charged[key]; ok {
		delete(d.charged, key)
		d.deficit[ev.TenantID] -= actual - cost
		if d.deficit[ev.TenantID] < -drrMaxDebt {
			d.deficit[ev.TenantID] = -drrMaxDebt
		}
	}

	est := d.estimateFor(ev.TenantID)
	d.estimate[ev.TenantID] = est + drrAlpha*(actual-est)
}

// estimateFor must be called with d.mu held.
func (d *DRR) estimateFor(tenantID int64) float64 {
	est, ok := d.estimate[tenantID]
	if !ok {
		return drrDefaultCost
	}
	return est
}

func weightOf(t domain.DueTenant) int {
	if t.Weight < 1 {
		return 1
	}
	return t.Weight
}
