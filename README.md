# FairRelay

Reliable, fair and resilient webhook delivery between independent software systems.

## Problem Statement

FairRelay is a delivery intermediary that sits between two software systems. It accepts delivery requests from producers and attempts to deliver them to receivers while providing retry handling, failure recovery, tenant fairness and controlled concurrency.

Instead of requiring every producer to independently manage delivery failures, retries, scheduling and recovery, FairRelay centralizes these responsibilities.

## Core Objectives

- Reliable delivery with retry and recovery mechanisms.
- Fair resource allocation across tenants.
- Controlled concurrency to prevent tenant monopolization.
- Failure isolation using circuit breakers.
- Recoverable delivery execution using leases.
- Persistent delivery state using PostgreSQL.

## Architecture

```text
Producer
   |
   v
FairRelay API
   |
   v
PostgreSQL
   |
   v
Scheduler (DRR)
   |
   v
Worker Pools
   |
   v
Receiver
```

## Design Decisions

### Deficit Round Robin (DRR)

DRR is used instead of basic Round Robin scheduling to support fair resource allocation across tenants.

Basic Round Robin distributes turns cyclically but does not inherently account for varying job costs or provide weighted fairness. DRR maintains deficit counters and scheduling credits, allowing queues to receive fair service over time.

### Worker Pools

Separate fast and slow worker pools allow delivery execution to be isolated by workload class, preventing slower workloads from unnecessarily consuming the entire fast pool.

### Tenant Concurrency Limits

A per-tenant in-flight limit prevents any single tenant from consuming an excessive number of concurrent delivery slots.

### Leases

Deliveries are claimed using temporary leases. If a worker fails or abandons a job, the lease can expire and the delivery can become eligible for recovery.

### Retry and Backoff

Failed deliveries are retried with exponential backoff and a configurable maximum delay to avoid continuously overwhelming unhealthy receivers.

### Circuit Breaker

A circuit breaker temporarily stops or defers requests to unhealthy receivers, reducing repeated failures and allowing recovery.

## Configuration

Configuration is provided through environment variables. See `.env.example` for available settings, worker limits, timeout values, retry policies and database configuration.

## Project Structure

- `cmd/` — Service entry points.
- `internal/` — Application logic and internal packages.
- `migrations/` — Database migrations.
- `tests/` — Integration and system tests.

## Delivery Semantics

FairRelay aims for reliable delivery with recovery and retries. Delivery is not inherently exactly-once: retries, timeouts and lease expiration may result in duplicate delivery. Receivers should support idempotency.

## Status

Under development.