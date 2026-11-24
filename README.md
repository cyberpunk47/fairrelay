# FairRelay

A multi-tenant webhook delivery engine in Go and PostgreSQL that keeps one tenant's broken
endpoint from delaying everyone else's webhooks.

| Result | Number | Details |
|---|---|---|
| Healthy tenant's wait while a neighbour's endpoint hangs | **200 s → 0.30 s** (FIFO → round robin) | [Experiment 1](docs/RESULTS.md#experiment-1-noisy-neighbour-fifo-vs-round-robin-2026-11-24) |
| Every dispatcher killed with SIGKILL mid-delivery | **0 of 40 events lost** | [Experiment 2](docs/RESULTS.md#experiment-2-crash-test-sudden-death-of-every-dispatcher-2026-11-24) |
| Circuit breaker alone, under FIFO | **200 s → 10 s** | [Experiment 3](docs/RESULTS.md#experiment-3-all-four-policies-sandbox-2026-11-24) |
| Ingest API (k6, 2 vCPU, everything co-located) | **500 req/s at p95 5.3 ms, 0 errors** | [Experiment 5](docs/RESULTS.md#experiment-5-throughput-sandbox-2026-11-24) |

## The problem

A webhook platform sends HTTP callbacks for many customers (tenants) from one shared pool of workers.
When one customer's endpoint hangs, a first-come-first-served queue lets that customer's backlog take every
worker: each one waits out a 10-second timeout, and healthy customers queue behind them.

Measured: 5 workers, shopA's endpoint hangs (100 events), shopB's is healthy (20 events).
With FIFO, shopB's webhooks waited **200 s** (100 events ÷ 5 workers × 10 s). With per-tenant
caps and fair scheduling, **0.30 s**, on the same hardware.

## How it works

```text
 producer ──POST /events──► API server ──INSERT (idempotent)──► PostgreSQL
                            GET /events/{id}                    events, endpoints, tenants,
                                                                delivery_attempts
                                                                     ▲   │ claim (lease)
                                 ┌───────────────────────────────────┘   ▼
                                 │        dispatcher (one or many processes)
                                 │   scheduler: policy picks whose turn it is
                                 │     fifo | roundrobin | drr
                                 │          │ unbuffered channel
                                 │   worker pool ──signed POST──► tenant endpoint
                                 └── record result (fenced by lease token)
                                     maintenance every 5 s: reclaim expired leases,
                                     move cooled-down breakers to HALF_OPEN
```

1. **Ingest.** `POST /events` checks the API key, validates, and inserts with
   `ON CONFLICT (tenant_id, external_id) DO NOTHING`. A retried request returns `200 duplicate`
   instead of creating a second delivery. `202` means the event is committed and won't be lost.
2. **Claim.** The scheduler asks its policy for work. A claim is one transaction:
   lock the tenant row → skip if all its endpoints' breakers are OPEN → count deliveries in flight →
   stop at the tenant's `max_inflight` → `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED)`.
   The claim sets a lease (`lease_until`) and increments a fencing token (`lease_token`).
3. **Deliver.** A worker POSTs the exact payload with `X-Webhook-ID`, `X-Webhook-Type` and an
   HMAC-SHA256 signature, with a 2 s connect timeout and a 10 s total timeout.
4. **Record.** Success → `DELIVERED`. Failure → `PENDING` again with exponential backoff and jitter
   (or the endpoint's `Retry-After`), up to `MAX_ATTEMPTS`, then `DEAD`. Every try is written to
   `delivery_attempts`. All result writes include `WHERE lease_token = <mine>`: a worker that stalled
   past its lease can't overwrite the worker that took over.
5. **Recover.** Every 5 s, events whose lease expired (their worker died) go back to `PENDING`.

## Design decisions

| Decision | Why |
|---|---|
| **PostgreSQL is the queue** (`FOR UPDATE SKIP LOCKED`), no Kafka/Redis | One source of truth. Accepting an event, claiming it and recording the result are all transactions in the same database, so there's no second system to keep consistent. |
| **Per-tenant cap enforced in the database** | The cap holds across any number of dispatcher processes (verified with 2). A cap kept in memory would only hold per process. |
| **Leases + fencing tokens** | A dead worker's event comes back after the lease; a slow worker's late result is rejected instead of overwriting the newer one. |
| **Three policies behind one interface** (Strategy pattern) | `fifo` is the baseline that shows the problem; `roundrobin` gives every tenant a turn; `drr` shares *worker-time*, not turns. |
| **DRR measured in worker-time** | Round robin is fair in turns, but a tenant whose endpoint takes 2 s per call holds a worker 200× longer per turn than one taking 10 ms. DRR charges each tenant for the time its deliveries actually took and honours `tenants.weight`. |
| **Circuit breaker per endpoint, stored in the database** | After `BREAKER_FAILURES` failures in a row (timeouts, connection errors, 5xx) the endpoint gets nothing for `BREAKER_COOLDOWN`; then exactly one test delivery decides whether it closes. State in Postgres means every dispatcher sees it. 4xx doesn't count: the endpoint is up. |
| **Scheduler wakes when a delivery finishes** | Without it, a tenant at its cap waited out a 200 ms idle sleep: 11 deliveries/s at cap 2. With it: 619/s. |
| **Unbuffered hand-off channel** | The scheduler only claims what a free worker can start, so leases aren't ticking on events sitting in a buffer. |
| **Graceful shutdown on SIGINT/SIGTERM** | Stop claiming, let in-flight deliveries finish, record them, exit. Covers both Ctrl+C and `docker stop`. |

## Delivery guarantee: at-least-once

Every accepted event is delivered or ends `DEAD` after `MAX_ATTEMPTS`; none disappear (see the crash test).
After a crash an event can arrive **twice**: the receiver got the request, but the dead worker never recorded
the reply. Receivers should skip repeats using `X-Webhook-ID`.

**Verifying a webhook (receiver side).** Header `X-Signature-256: t=<unix seconds>,v1=<hex>`, where
`v1 = HMAC-SHA256(endpoint secret, "<t>.<raw body>")`. Recompute it over the raw body, compare in
constant time, and reject old `t` values to stop replays.

## Run it

Requires Docker. Copy `.env.example` to `.env` and fill in the Postgres credentials, `DATABASE_URL` and `PRODUCER_API_KEY`.

```powershell
docker compose up --build -d     # postgres, migrate, seed, API server, 2 dispatchers, fake receiver
docker compose ps                # migrate and seed: Exited (0); everything else: Up / healthy
```

| Service | Address |
|---|---|
| API + Swagger UI | http://localhost:8081/docs |
| Health | http://localhost:8081/healthz |
| Fake receiver (`PUT /mode/{tenant}/{ok,slow,hang,error,ratelimit}`) | http://localhost:9900 |
| PostgreSQL | localhost:5433 |

```powershell
$key = (Select-String -Path .env -Pattern '^PRODUCER_API_KEY=').Line.Split('=', 2)[1].Trim()
curl.exe -X POST http://localhost:8081/events -H "X-API-Key: $key" -H "Content-Type: application/json" --data-binary "@body.json"
curl.exe http://localhost:8081/events/<id from the reply> -H "X-API-Key: $key"   # status + every delivery try
docker compose logs -f dispatcher fakereceiver
```

## Tests and experiments

| What | Command | Needs |
|---|---|---|
| Unit tests (backoff, DRR fairness) | `go test ./...` | nothing |
| Database tests (idempotency, cap, fencing, breaker, lookup) | `$env:TEST_DATABASE_URL = "<DATABASE_URL with localhost:5433>"` then `go test ./internal/store/ -v` | Postgres from Compose |
| Noisy-neighbour benchmark (4 policies, ~4.5 min) | `docker compose stop dispatcher; docker compose run --rm bench` | Compose |
| Crash test (SIGKILL, then count) | `go run ./cmd/crashtest` with `DATABASE_URL` set | Compose running |
| Ingest load test | `k6 run -e API_KEY=$key bench/k6/ingest.js` | k6 |

Results and methods: [docs/RESULTS.md](docs/RESULTS.md).

## Configuration

Environment variables (see `.env.example`). Per-tenant settings live in the `tenants` table:
`max_inflight` (cap, default 2) and `weight` (DRR share, default 1).

| Variable | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | required | PostgreSQL connection string |
| `PRODUCER_API_KEY` | required | value of the `X-API-Key` header |
| `HTTP_ADDR` | `:8080` | API listen address |
| `POLICY` | `drr` | `fifo`, `roundrobin` or `drr` |
| `WORKERS` | `200` | delivery goroutines per dispatcher process |
| `REQUEST_TIMEOUT` / `CONNECT_TIMEOUT` | `10s` / `2s` | per delivery try |
| `LEASE_TTL` | `30s` | must be longer than `REQUEST_TIMEOUT` (checked at startup) |
| `MAX_ATTEMPTS` | `8` | tries before `DEAD` |
| `BACKOFF_BASE` / `BACKOFF_MAX` | `1s` / `10m` | exponential backoff with jitter |
| `BREAKER_FAILURES` / `BREAKER_COOLDOWN` | `5` / `30s` | circuit breaker |

## Project layout

```text
cmd/
  server/        ingest API: POST /events, GET /events/{id}, /healthz, /docs
  dispatcher/    delivery engine (runs internal/dispatcher)
  fakereceiver/  stand-in customer endpoints with switchable failure modes
  bench/         noisy-neighbour benchmark
  crashtest/     kill-and-recover test
internal/
  config/        env loading and validation
  domain/        shared types
  store/         Store interface + PostgreSQL implementation (all SQL lives here)
  ingest/        HTTP handlers
  scheduler/     Policy interface, fifo, roundrobin, drr, scheduler loop
  delivery/      HTTP sender + worker (result recording, breaker updates)
  dispatcher/    wires workers, scheduler and maintenance together
  retry/         backoff
  security/      HMAC signing
  apidocs/       OpenAPI spec + Swagger UI
migrations/      schema (golang-migrate)
scripts/seed.sql demo tenants and endpoints
bench/k6/        load test
docs/            RESULTS.md (experiments), INTEGRATION.md (using it from other apps)
```

## Known limits

- **One shared producer key.** Any producer can send events for any tenant. A real system issues a key
  per tenant and checks it matches `tenant_id`.
- **DRR state is per dispatcher process.** With several dispatchers, each is fair over its own share;
  the cap and the breaker are global because they live in the database.
- **PostgreSQL as the queue** handled about 1,000 deliveries/s and 1,200+ ingest requests/s on 2 shared vCPUs.
  Far beyond that, the `events` table would need partitioning by tenant, or claiming would move to a log-based queue.
- **Metrics** are logs and SQL queries today; there is no Prometheus endpoint yet.
