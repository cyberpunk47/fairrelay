package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/config"
	"github.com/cyberpunk47/fairrelay/internal/dispatcher"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The scenario (scaled-down version of the report's maths)
const (
	hangEvents    = 100             // shopA: endpoint hangs
	healthyEvents = 20              // shopB: healthy endpoint
	runTimeout    = 6 * time.Minute // give up waiting after this
)

// receiverURL is where the fake receiver is reachable from the bench.
// Local (go run): http://localhost:9900. Inside Docker: http://fakereceiver:9900 (set by docker-compose).
var receiverURL = "http://localhost:9900"

// One benchmark run: a scheduling policy, with or without the circuit breaker.
type run struct {
	Name    string
	Policy  string
	Breaker bool
}

// The breaker is off except in "fifo+breaker", so the other rows measure scheduling alone.
var runs = []run{
	{Name: "fifo", Policy: "fifo"},
	{Name: "fifo+breaker", Policy: "fifo", Breaker: true},
	{Name: "roundrobin", Policy: "roundrobin"},
	{Name: "drr", Policy: "drr"},
}

type result struct {
	Policy    string
	Delivered int
	AvgWait   float64 // seconds
	MaxWait   float64 // seconds
}

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Println("config error:", err)
		os.Exit(1)
	}
	if v := os.Getenv("BENCH_RECEIVER_URL"); v != "" {
		receiverURL = v
	}
	cfg.Workers = 5 // few workers, so the effect shows on a laptop

	log.SetOutput(io.Discard) // silence the dispatcher's per-event log lines; the bench prints with fmt

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		fmt.Println("database error:", err)
		os.Exit(1)
	}
	defer pool.Close()

	var results []result
	for _, rn := range runs {
		fmt.Printf("running %-12s ... ", rn.Name)
		start := time.Now()
		r, err := runOnce(ctx, pool, cfg, rn)
		if err != nil {
			fmt.Println("error:", err)
			os.Exit(1)
		}
		fmt.Printf("done in %v\n", time.Since(start).Round(time.Second))
		results = append(results, r)
	}

	table := formatTable(results)
	fmt.Println()
	fmt.Print(table)

	if err := saveResults(table); err != nil {
		fmt.Println("could not save results:", err)
	}
}

// runOnce runs one policy on fresh data and measures shopB's wait.
func runOnce(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, rn run) (result, error) {
	r := result{Policy: rn.Name}

	// 1. Fresh data
	if err := resetData(ctx, pool); err != nil {
		return r, fmt.Errorf("reset data: %w", err)
	}

	// 2. Find the two tenants, point their endpoints at the receiver,
	//    then make shopA's endpoint hang and keep shopB's healthy
	shopA, err := tenantID(ctx, pool, "shopA")
	if err != nil {
		return r, err
	}
	shopB, err := tenantID(ctx, pool, "shopB")
	if err != nil {
		return r, err
	}
	if err := pointEndpointsAtReceiver(ctx, pool); err != nil {
		return r, fmt.Errorf("set endpoint urls: %w", err)
	}
	if err := setMode(shopA, "hang"); err != nil {
		return r, err
	}
	if err := setMode(shopB, "ok"); err != nil {
		return r, err
	}

	// 3. Start the clock using the DATABASE's clock (same clock as delivered_at)
	var t0 time.Time
	if err := pool.QueryRow(ctx, "SELECT now()").Scan(&t0); err != nil {
		return r, fmt.Errorf("read db clock: %w", err)
	}

	// 4. Start the dispatcher in the background
	cfg.Policy = rn.Policy
	if !rn.Breaker {
		cfg.BreakerFailures = 1 << 30 // never reached: breaker effectively off
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- dispatcher.Run(runCtx, cfg)
	}()

	// 5. Wait until all of shopB is delivered (or we give up)
	deadline := time.Now().Add(runTimeout)
	for time.Now().Before(deadline) {
		n, err := countDelivered(ctx, pool)
		if err != nil {
			cancel()
			<-done
			return r, err
		}
		if n >= healthyEvents {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// 6. Stop the dispatcher and wait for it to finish cleanly
	cancel()
	if err := <-done; err != nil {
		return r, fmt.Errorf("dispatcher: %w", err)
	}

	// 7. Measure shopB's wait from t0
	const measureSQL = `
SELECT count(*),
       coalesce(avg(extract(epoch FROM e.delivered_at - $1::timestamptz)), 0)::float8,
       coalesce(max(extract(epoch FROM e.delivered_at - $1::timestamptz)), 0)::float8
FROM events e JOIN tenants t ON t.id = e.tenant_id
WHERE t.name = 'shopB' AND e.status = 'DELIVERED'`
	err = pool.QueryRow(ctx, measureSQL, t0).Scan(&r.Delivered, &r.AvgWait, &r.MaxWait)
	if err != nil {
		return r, fmt.Errorf("measure: %w", err)
	}
	return r, nil
}

// resetData empties events and inserts the scenario.
func resetData(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `DELETE FROM delivery_attempts`); err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `DELETE FROM events`); err != nil {
		return err
	}
	// Every run starts with closed breakers
	if _, err := pool.Exec(ctx, `UPDATE endpoints SET breaker_state = 'CLOSED', consecutive_failures = 0, next_probe_at = NULL`); err != nil {
		return err
	}

	// shopA: made 1 minute older, so FIFO serves them first
	const insertA = `
INSERT INTO events (tenant_id, endpoint_id, external_id, event_type, payload, next_attempt_at)
SELECT t.id, e.id, t.name || '_' || g, 'bench', '{}', now() - interval '1 minute'
FROM tenants t JOIN endpoints e ON e.tenant_id = t.id, generate_series(1, $1::int) g
WHERE t.name = 'shopA'`
	if _, err := pool.Exec(ctx, insertA, hangEvents); err != nil {
		return err
	}

	const insertB = `
INSERT INTO events (tenant_id, endpoint_id, external_id, event_type, payload)
SELECT t.id, e.id, t.name || '_' || g, 'bench', '{}'
FROM tenants t JOIN endpoints e ON e.tenant_id = t.id, generate_series(1, $1::int) g
WHERE t.name = 'shopB'`
	_, err := pool.Exec(ctx, insertB, healthyEvents)
	return err
}

// tenantID looks up a tenant's id by name (returned as text, for the receiver's URL).
func tenantID(ctx context.Context, pool *pgxpool.Pool, name string) (string, error) {
	var id int64
	err := pool.QueryRow(ctx, `SELECT id FROM tenants WHERE name = $1`, name).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("tenant %s not found (run scripts/seed.sql first): %w", name, err)
	}
	return strconv.FormatInt(id, 10), nil
}

// pointEndpointsAtReceiver makes shopA/shopB's endpoints call the receiver
// at the address that works from wherever this bench is running.
func pointEndpointsAtReceiver(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
UPDATE endpoints e SET url = $1 || '/t/' || e.tenant_id
FROM tenants t
WHERE t.id = e.tenant_id AND t.name IN ('shopA', 'shopB')`, receiverURL)
	return err
}

func countDelivered(ctx context.Context, pool *pgxpool.Pool) (int, error) {
	var n int
	err := pool.QueryRow(ctx, `
SELECT count(*) FROM events e JOIN tenants t ON t.id = e.tenant_id
WHERE t.name = 'shopB' AND e.status = 'DELIVERED'`).Scan(&n)
	return n, err
}

// setMode tells the fake receiver how a tenant's endpoint should behave.
func setMode(tenant, mode string) error {
	req, err := http.NewRequest(http.MethodPut, receiverURL+"/mode/"+tenant+"/"+mode, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fake receiver not reachable (is it running?): %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("set mode %s/%s: status %d", tenant, mode, resp.StatusCode)
	}
	return nil
}

func formatTable(results []result) string {
	s := fmt.Sprintf("Scenario: 5 workers, cap 2, shopA %d events (hanging), shopB %d events (healthy)\n\n", hangEvents, healthyEvents)
	s += "| Policy       | shopB delivered | avg wait (s) | max wait (s) |\n"
	s += "|--------------|-----------------|--------------|--------------|\n"
	for _, r := range results {
		delivered := fmt.Sprintf("%d / %d", r.Delivered, healthyEvents)
		s += fmt.Sprintf("| %-12s | %-15s | %12.2f | %12.2f |\n",
			r.Policy, delivered, r.AvgWait, r.MaxWait)
	}
	return s
}

// saveResults writes the table to bench/results/bench-YYYYMMDD-HHMMSS.md
func saveResults(table string) error {
	dir := filepath.Join("bench", "results")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	name := filepath.Join(dir, "bench-"+time.Now().Format("20060102-150405")+".md")
	if err := os.WriteFile(name, []byte(table), 0o644); err != nil {
		return err
	}
	fmt.Println("\nsaved to", name)
	return nil
}
