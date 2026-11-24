# FairRelay — Results

| # | Experiment | Headline |
|---|---|---|
| 1 | Noisy neighbour: FIFO vs round robin (laptop, Docker) | healthy tenant waits **200 s → 0.30 s** |
| 2 | Crash test: SIGKILL every dispatcher mid-delivery | **0 of 40 lost** |
| 3 | All four policies after breaker + DRR (sandbox) | breaker alone **200 s → 10 s**; RR/DRR **0.04–0.05 s** |
| 4 | Circuit breaker on a failing endpoint (sandbox) | 10 events cost **3 tries** instead of retrying blindly |
| 5 | Throughput: ingest (k6) and delivery (sandbox) | **500 req/s at p95 5.3 ms, 0 errors**; delivery bug fix **11 → 619/s** |

"Laptop" = Windows laptop, everything in Docker Compose. "Sandbox" = Linux container with 2 vCPUs,
PostgreSQL 16 and the Go binaries run directly; k6, the server, two dispatchers, the receiver and
Postgres all share those 2 vCPUs. Absolute numbers differ between the two; compare within a table.

## Experiment 1: noisy neighbour, FIFO vs round robin (2026-11-24)

**Question:** when one tenant's webhook endpoint hangs, how long do *healthy* tenants wait?

### Setup

| Item | Value |
|---|---|
| Machine | Windows laptop; PostgreSQL 17 in Docker; dispatcher, fake receiver run with `go run` |
| Workers | 5 |
| Request timeout | 10 s |
| Per-tenant cap (`max_inflight`) | 2 |
| Tenant A (shopA) | 100 events, endpoint in `hang` mode (never replies), events made 1 minute older so FIFO serves them first |
| Tenant B (shopB) | 20 events, healthy endpoint (replies 200 in ~2 ms) |
| Only change between runs | `POLICY=fifo` vs `POLICY=roundrobin` |

### Results (automated benchmark, `go run ./cmd/bench`, 2026-11-24 13:47)

| Policy | shopB delivered | shopB avg wait | shopB max wait | What shopA did |
|---|---|---|---|---|
| FIFO (baseline) | 20 / 20 | **200.28 s** | 200.31 s | took all 5 workers, 10 s per batch |
| Round robin + cap | 20 / 20 | **0.30 s** | **0.39 s** | held to 2 workers; 3 stayed free |

**Healthy tenant's wait: 200.28 s → 0.30 s (about 670× shorter), same hardware, same load.**

Prediction for FIFO: 100 events ÷ 5 workers × 10 s timeout = 200 s. Measured 200.28 s.

Raw output: `bench/results/bench-20261124-134720.md`.
An earlier manual run (FIFO timed from the logs, round robin by query) gave ~200 s vs 0.29 s, consistent with this.

### How "wait" is measured

The benchmark reads the database clock (`SELECT now()`) just before starting the dispatcher and uses it as t0.
Wait = `delivered_at − t0` for each shopB event. Both timestamps come from the same clock (PostgreSQL),
so Docker/Windows clock drift and the time spent inserting test rows are not counted.

```sql
SELECT count(*),
       avg(extract(epoch FROM e.delivered_at - $1::timestamptz)),
       max(extract(epoch FROM e.delivered_at - $1::timestamptz))
FROM events e JOIN tenants t ON t.id = e.tenant_id
WHERE t.name = 'shopB' AND e.status = 'DELIVERED';   -- $1 = t0
```

### Caveats (be honest about these)

- Single run per policy, one laptop, simulated endpoints. These are relative results, not production numbers.
- The dispatcher runs inside the benchmark process (same code path as `cmd/dispatcher`, via `dispatcher.Run`).
- During an earlier long manual FIFO run the laptop went to sleep; results from that window were discarded.
  Benchmarks must run with sleep disabled, and the database (`delivered_at`) is the source of truth.

### Other behaviour verified in this session

- Hanging endpoint: each try stops at the 10 s timeout; receiver and sender agree (`sender gave up`).
- Cap: a tenant never has more than `max_inflight` deliveries running.
- Retries use exponential backoff with jitter, honour `Retry-After`, and stop at `MAX_ATTEMPTS` (event becomes `DEAD`).
- Lease recovery: events held by a stopped worker return to the queue; stale results are rejected by the lease token.
- Ctrl+C: dispatcher stops claiming, lets in-flight deliveries finish or time out, records them, exits cleanly.

### Reproduce

**Option A: everything in Docker**

```powershell
docker compose up --build -d          # postgres, migrate, seed, server, 2 dispatchers, fake receiver
docker compose stop dispatcher        # the bench runs its own dispatcher; the others would steal its events
docker compose run --rm bench         # ~4.5 minutes (FIFO is the slow one); table saved to bench/results/
docker compose start dispatcher
```

**Option B: Go on the host, Postgres in Docker**

1. `docker compose up -d postgres migrate seed`
2. Terminal 1: `go run ./cmd/fakereceiver`
3. Terminal 2 (with `DATABASE_URL` pointing at `localhost:5433`, plus `PRODUCER_API_KEY`): `go run ./cmd/bench`

The bench looks up shopA/shopB by name and points their endpoints at the receiver address for wherever it runs
(`BENCH_RECEIVER_URL`, default `http://localhost:9900`). Running `docker compose up` again re-runs the seed and
points them back at `http://fakereceiver:9900`.

---

## Experiment 2: crash test, sudden death of every dispatcher (2026-11-24)

**Question:** if the dispatchers die without warning in the middle of deliveries, is any accepted event lost?

### Setup

| Item | Value |
|---|---|
| Deployment | Full Docker Compose: 2 dispatcher containers (200 workers each), policy `roundrobin`, per-tenant cap 2 |
| Endpoint | shopB in `slow` mode (2 s per request), so deliveries are in progress at the moment of the kill |
| Load | 40 events for shopB, inserted directly in the database |
| Failure | `docker compose kill dispatcher` (SIGKILL: no shutdown code runs, like a power cut or OOM kill) |
| Recovery | `docker compose start dispatcher`; lease TTL 30 s; reclaim loop every 5 s |

### Results

| Moment | DELIVERED | IN_FLIGHT | PENDING |
|---|---|---|---|
| Right after the kill | 6 | **2** (orphaned, worker dead) | 32 |
| ~90 s after restart | **40** | 0 | 0 |

- **0 of 40 events lost.**
- The 2 orphans (`crash_7`, `crash_8`) were returned to the queue by lease recovery
  (`reclaimed 2 expired leases` in dispatcher-1's log) and delivered on **attempt 2**.
- Only 2 events were in flight at the kill, even with two separate dispatcher processes:
  the per-tenant cap (2) is enforced in the database, not in process memory.

### Duplicates (expected: at-least-once delivery)

The receiver got `crash_7` and `crash_8` one extra time each. The killed workers had already sent those
requests; the receiver processed them, but the reply was never recorded, so after recovery they were sent again.
No other event was duplicated. This is the at-least-once guarantee: every event arrives, a crash can cause a
repeat, and receivers drop repeats using the `X-Webhook-ID` header.

(The raw receiver log showed every `crash_*` ID at least twice only because the same IDs were reused
from an earlier run within the 5-minute log window; that run accounts for exactly one copy of each.)

### Commands used

```powershell
curl.exe -X PUT http://localhost:9900/mode/2/slow
# psql: insert 40 crash_* events for shopB
docker compose kill dispatcher
# psql: SELECT status, count(*) FROM events WHERE external_id LIKE 'crash_%' GROUP BY status;
docker compose start dispatcher
# ~90 s later, psql: same query, plus
#   SELECT external_id, attempts FROM events WHERE external_id LIKE 'crash_%' AND attempts > 1;
docker compose logs dispatcher --since 5m | Select-String "reclaimed"
```


---

## Experiment 3: all four policies (sandbox, 2026-11-24)

Same scenario as Experiment 1 (5 workers, cap 2, shopA 100 hanging events, shopB 20 healthy events),
run with `go run ./cmd/bench` after the circuit breaker, DRR and the scheduler wake-up fix were added.
The breaker is off except in `fifo+breaker`, so the other rows measure scheduling alone.

| Policy | shopB delivered | avg wait (s) | max wait (s) |
|---|---|---|---|
| fifo | 20 / 20 | 200.12 | 200.13 |
| fifo+breaker | 20 / 20 | 10.03 | 10.04 |
| roundrobin | 20 / 20 | 0.04 | 0.05 |
| drr | 20 / 20 | 0.05 | 0.07 |

- FIFO reproduces the laptop result (200.28 s there, 200.12 s here).
- The breaker alone cuts FIFO's damage 20x: after 5 timeouts shopA's endpoint is OPEN, so the
  remaining workers go to shopB. It doesn't fix the first 10 s, because those 5 timeouts have to happen first.
- Round robin and DRR are the real fix: shopA never holds more than its cap of 2 workers.
- DRR ~= round robin here because shopB's deliveries are cheap. DRR's advantage (sharing worker-time,
  not turns, between tenants with slow-but-working endpoints, and honouring weights) is shown by the
  unit tests in `internal/scheduler/drr_test.go`:

| DRR unit test | Result |
|---|---|
| slow tenant (1 s/delivery) vs fast (0.1 s), equal weight | 200 vs 2000 deliveries, **worker-time 3m20s vs 3m20s** |
| weight 2 vs weight 1, same cost | worker-time ratio **2.00** |
| lone slow tenant | served every round (work-conserving: no idle workers) |

## Experiment 4: circuit breaker (sandbox, 2026-11-24)

Settings: `BREAKER_FAILURES=3`, `BREAKER_COOLDOWN=8s`, 2 dispatchers. shopB's endpoint set to return 500,
then 10 events sent, then the endpoint fixed.

| Time | What happened |
|---|---|
| +0 s | 3 deliveries fail (500) → breaker **OPEN**; the other 7 events wait, no tries wasted |
| +4 s | 10 PENDING, 3 attempts in total |
| +10 s | cooldown over → **HALF_OPEN**, exactly one test delivery → still 500 → **OPEN** again |
| endpoint fixed | next probe succeeds → **CLOSED** → all **10 DELIVERED** |

The breaker lives in the `endpoints` table, so every dispatcher sees the same state, and the
HALF_OPEN rule ("send only if nothing is in flight to this endpoint") is checked inside the same
transaction that holds the tenant lock, so only one probe goes out even with several dispatchers.
Covered by `TestBreakerOpensProbesAndCloses` in `internal/store/postgres_test.go`.

## Experiment 5: throughput (sandbox, 2026-11-24)

### Ingest (k6, `bench/k6/ingest.js`)

Constant arrival rate, new `event_id` per request (every 10th a deliberate duplicate), dispatchers
delivering in the background at the same time.

| Target rate | Achieved | p50 | p95 | p99 | Errors |
|---|---|---|---|---|---|
| 500 req/s, 20 s | 500 req/s | 0.95 ms | **5.3 ms** | 9.1 ms | **0 %** |
| 1500 req/s, 15 s | 1222 req/s | 182 ms | 551 ms | 578 ms | 0 % |
| 3000 req/s, 15 s | 2076 req/s | 80 ms | 558 ms | 796 ms | 0 % |

Above ~1,200 req/s the 2 shared vCPUs saturate: latency rises and k6 drops requests it can't start,
but nothing fails. Rerun on real hardware for absolute numbers.

### Delivery: a scheduler bug found and fixed

Measuring delivery speed for one tenant exposed a bug: when every tenant was at its cap, the scheduler
slept its full 200 ms idle wait, even though the deliveries holding the cap finished in about 1 ms.
With cap 2 that meant at most 2 deliveries per 200 ms. Fix: workers signal the scheduler when a
delivery finishes, and the scheduler wakes up on that signal.

| One tenant, fast endpoint | Before | After |
|---|---|---|
| `max_inflight = 2` | 11 deliveries/s | **619 deliveries/s** |
| `max_inflight = 20` | 285 deliveries/s | **952 deliveries/s** |

### Another bug fixed: retries ignored their backoff ceiling

`worker.go` capped every retry wait at `BACKOFF_BASE` (1 s) instead of `BACKOFF_MAX` (10 min), so
exponential backoff and long `Retry-After` values were silently cut to about a second. Fixed; waits now
grow 1 s, 2 s, 4 s... up to `BACKOFF_MAX`.
