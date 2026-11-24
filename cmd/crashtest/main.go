// crashtest kills every dispatcher in the middle of deliveries (SIGKILL, no
// shutdown code runs), starts them again, and checks that no accepted event was lost.
//
// Run from the repo root while `docker compose up` is running:
//
//	$env:DATABASE_URL = "postgres://fairrelay:<password>@localhost:5433/fairrelay?sslmode=disable"
//	go run ./cmd/crashtest
//
// Exit code 0 = nothing lost, 1 = events lost or the test could not run.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	events     = 40
	recoverMax = 3 * time.Minute // lease expiry (30 s) + delivering the rest at ~1/s
)

// Overridable so the test can drive something other than Docker Compose.
var (
	receiverURL = envOr("CRASH_RECEIVER_URL", "http://localhost:9900")
	killCmd     = envOr("CRASH_KILL_CMD", "docker compose kill dispatcher")
	startCmd    = envOr("CRASH_START_CMD", "docker compose start dispatcher")
)

type counts struct{ Delivered, InFlight, Pending, Dead int }

func main() {
	if err := run(); err != nil {
		fmt.Println("FAILED:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return fmt.Errorf("DATABASE_URL is required (FairRelay's Postgres, localhost:5433 with Docker)")
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()

	// 1. Setup: shopB's endpoint answers slowly (2 s), so deliveries are in progress when we kill
	var tenantID, endpointID int64
	err = pool.QueryRow(ctx, `
SELECT t.id, e.id FROM tenants t JOIN endpoints e ON e.tenant_id = t.id
WHERE t.name = 'shopB' ORDER BY e.id LIMIT 1`).Scan(&tenantID, &endpointID)
	if err != nil {
		return fmt.Errorf("tenant shopB not found (run the seed first): %w", err)
	}
	if err := resetData(ctx, pool, tenantID, endpointID); err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	if err := setMode(tenantID, "slow"); err != nil {
		return err
	}
	defer setMode(tenantID, "ok") //nolint:errcheck // best effort cleanup

	// 2. Load: 40 events
	_, err = pool.Exec(ctx, `
INSERT INTO events (tenant_id, endpoint_id, external_id, event_type, payload)
SELECT $1, $2, 'crash_' || g, 'crash.test', '{}' FROM generate_series(1, $3::int) g`,
		tenantID, endpointID, events)
	if err != nil {
		return fmt.Errorf("insert events: %w", err)
	}
	fmt.Printf("inserted %d events for shopB (endpoint in slow mode)\n", events)

	// 3. Wait until deliveries are actually in progress
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := count(ctx, pool)
		if err != nil {
			return err
		}
		if c.Delivered >= 2 && c.InFlight > 0 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("deliveries never started (are the dispatchers running?) %+v", c)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// 4. Crash
	fmt.Printf("killing dispatchers: %s\n", killCmd)
	if err := shell(killCmd); err != nil {
		return err
	}
	atKill, err := count(ctx, pool)
	if err != nil {
		return err
	}
	fmt.Printf("right after the kill: %+v\n", atKill)

	// 5. Recover
	fmt.Printf("starting dispatchers: %s\n", startCmd)
	if err := shell(startCmd); err != nil {
		return err
	}
	start := time.Now()
	var final counts
	for {
		final, err = count(ctx, pool)
		if err != nil {
			return err
		}
		if final.Delivered+final.Dead == events {
			break
		}
		if time.Since(start) > recoverMax {
			break
		}
		time.Sleep(2 * time.Second)
	}
	recovered := time.Since(start).Round(time.Second)

	// 6. Verdict
	var retried int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM events WHERE external_id LIKE 'crash\_%' AND attempts > 1`).Scan(&retried); err != nil {
		return err
	}
	lost := events - final.Delivered - final.Dead

	report := fmt.Sprintf(`Crash test: %d events, dispatchers killed with SIGKILL mid-delivery

| Moment | DELIVERED | IN_FLIGHT | PENDING | DEAD |
|---|---|---|---|---|
| Right after the kill | %d | %d | %d | %d |
| After recovery (%v) | %d | %d | %d | %d |

Orphaned deliveries recovered by lease expiry (attempts > 1): %d
Lost events: %d
`, events,
		atKill.Delivered, atKill.InFlight, atKill.Pending, atKill.Dead,
		recovered, final.Delivered, final.InFlight, final.Pending, final.Dead,
		retried, lost)
	fmt.Println()
	fmt.Print(report)
	save(report)

	if lost != 0 {
		return fmt.Errorf("%d events lost or stuck", lost)
	}
	if atKill.InFlight == 0 {
		fmt.Println("note: nothing was in flight at the kill, so lease recovery was not exercised")
	}
	fmt.Println("PASS: no accepted event was lost")
	return nil
}

func resetData(ctx context.Context, pool *pgxpool.Pool, tenantID, endpointID int64) error {
	stmts := []string{
		`DELETE FROM delivery_attempts WHERE event_ref IN (SELECT id FROM events WHERE external_id LIKE 'crash\_%')`,
		`DELETE FROM events WHERE external_id LIKE 'crash\_%'`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			return err
		}
	}
	_, err := pool.Exec(ctx, `
UPDATE endpoints SET breaker_state = 'CLOSED', consecutive_failures = 0, next_probe_at = NULL
WHERE id = $1`, endpointID)
	return err
}

func count(ctx context.Context, pool *pgxpool.Pool) (counts, error) {
	var c counts
	err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE status = 'DELIVERED'),
       count(*) FILTER (WHERE status = 'IN_FLIGHT'),
       count(*) FILTER (WHERE status = 'PENDING'),
       count(*) FILTER (WHERE status = 'DEAD')
FROM events WHERE external_id LIKE 'crash\_%'`).Scan(&c.Delivered, &c.InFlight, &c.Pending, &c.Dead)
	return c, err
}

func setMode(tenantID int64, mode string) error {
	url := receiverURL + "/mode/" + strconv.FormatInt(tenantID, 10) + "/" + mode
	req, err := http.NewRequest(http.MethodPut, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("fake receiver not reachable at %s: %w", receiverURL, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("set mode %s: status %d", mode, resp.StatusCode)
	}
	return nil
}

// shell runs a command line like "docker compose kill dispatcher".
func shell(cmdline string) error {
	parts := strings.Fields(cmdline)
	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%q: %w", cmdline, err)
	}
	return nil
}

func save(report string) {
	dir := filepath.Join("bench", "results")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	name := filepath.Join(dir, "crash-"+time.Now().Format("20060102-150405")+".md")
	if err := os.WriteFile(name, []byte(report), 0o644); err == nil {
		fmt.Println("saved to", name)
	}
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
