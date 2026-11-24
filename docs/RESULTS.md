# FairRelay — Results

## Experiment 1: noisy neighbour, FIFO vs round robin (2026-11-24)

**Question:** when one tenant's webhook endpoint hangs, how long do *healthy* tenants wait?

### Setup

| Item | Value |
|---|---|
| Machine | Windows laptop; PostgreSQL 17 in Docker; dispatcher, fake receiver run with `go run` |
| Workers | 5 (`WORKERS_FAST=4`, `WORKERS_SLOW=1`) |
| Request timeout | 10 s |
| Per-tenant cap (`max_inflight`) | 2 |
| Tenant A (shopA) | 100 events, endpoint in `hang` mode (never replies), events made 1 minute older so FIFO serves them first |
| Tenant B (shopB) | 20 events, healthy endpoint (replies 200 in ~2 ms) |
| Only change between runs | `POLICY=fifo` vs `POLICY=roundrobin` |

### Results

| Policy | shopB avg wait | shopB max wait | What shopA did |
|---|---|---|---|
| FIFO (baseline) | **~200 s** | ~200 s | took all 5 workers, 10 s per batch |
| Round robin + cap | **0.29 s** | **0.38 s** | held to 2 workers; 3 stayed free |

**Healthy tenant's wait: ~200 s → 0.29 s (about 700× shorter), same hardware, same load.**

Prediction for FIFO: 100 events ÷ 5 workers × 10 s = 200 s. Matches.

### How "wait" was measured

`delivered_at − time of the dispatcher's first delivery attempt` (from `delivery_attempts.started_at`),
so the gap between inserting test rows and starting the dispatcher is not counted.

```sql
SELECT t.name, e.status, count(*),
       round(avg(extract(epoch FROM e.delivered_at - s.t0))::numeric, 2) AS avg_wait_s,
       round(max(extract(epoch FROM e.delivered_at - s.t0))::numeric, 2) AS max_wait_s
FROM events e
JOIN tenants t ON t.id = e.tenant_id
CROSS JOIN (SELECT min(started_at) AS t0 FROM delivery_attempts) s
GROUP BY t.name, e.status ORDER BY 1, 2;
```

### Caveats (be honest about these)

- **FIFO figure comes from the logs**, not the query above: dispatcher start ~12:22:42, shopB delivered 12:26:02.
  The original query for that run (`delivered_at − created_at`) gave 215.9 s, which included ~15 s between inserting rows and starting the dispatcher.
  To be re-measured with the automated benchmark.
- Single run each, one laptop, simulated endpoints. These are relative results, not production numbers.
- During a separate long FIFO run the laptop went to sleep; results from that window were discarded.
  Benchmarks must run with sleep disabled, and the database (`delivered_at`) is the source of truth.

### Other behaviour verified in this session

- Hanging endpoint: each try stops at the 10 s timeout; receiver and sender agree (`sender gave up`).
- Cap: a tenant never has more than `max_inflight` deliveries running.
- Retries use exponential backoff with jitter, honour `Retry-After`, and stop at `MAX_ATTEMPTS` (event becomes `DEAD`).
- Lease recovery: events held by a stopped worker return to the queue; stale results are rejected by the lease token.
- Ctrl+C: dispatcher stops claiming, lets in-flight deliveries finish or time out, records them, exits cleanly.

### Reproduce (manual, until the benchmark script exists)

1. `docker compose up -d postgres` and run the migration.
2. Start the fake receiver: `go run ./cmd/fakereceiver` (do not restart it mid-test; modes reset on restart).
3. In psql: delete `delivery_attempts` and `events`, insert 100 shopA events (`next_attempt_at = now() - 1 minute`) and 20 shopB events.
4. `curl.exe -X PUT http://localhost:9000/mode/1/hang`
5. `$env:POLICY="fifo"` (or `"roundrobin"`), `$env:WORKERS_FAST="4"`, `$env:WORKERS_SLOW="1"`, then `go run ./cmd/dispatcher`.
6. After shopB is delivered, Ctrl+C and run the query above.
