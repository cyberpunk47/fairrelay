# Running SwayPay + HMS through FairRelay

Two real apps send their events through FairRelay, which delivers them as signed webhooks.

```
SwayPay (Spring Boot, on the laptop :8033)
  payment commits ──► outbox_events row (same DB transaction)
                         │  FairRelayPublisher, every 1 s
                         ▼
                   POST http://localhost:8081/events ─────────────┐
                                                                  │
HMS (Docker)                                                      ▼
  appointment-ms ──Kafka──► NotificationMS                 FairRelay (Docker)
                              FairRelayForwarder ──► POST   server :8081 → Postgres
                              (own consumer group)          2 dispatchers (fair, capped)
                              http://host.docker.internal:8081      │
                                                                    ▼
                                                    fakereceiver :9900  (/t/<tenant_id>)
```

| Tenant in FairRelay | Producer | Event types | event_id (dedupe key) |
|---|---|---|---|
| `swaypay` | SwayPay outbox publisher | `payment.succeeded`, `payment.failed` | transaction `txnId` |
| `hms` | HMS NotificationMS forwarder | `appointment.created/.cancelled/.completed/.rescheduled/.expired` | Kafka `HmsEvent.eventId` |

Ports: FairRelay API **8081**, fake receiver **9900** (HMS's gateway owns 9000), FairRelay Postgres **5433**,
SwayPay **8033**, HMS gateway **9000**.

## 1. Start FairRelay

```powershell
cd C:\Users\snipe\Documents\fairrelay
docker compose up -d --build
docker compose logs seed
```

Note the `tenant_id` and `endpoint_id` printed for **swaypay** and **hms** (on a fresh database: 3 and 4).
The apps default to 3 (SwayPay) and 4 (HMS); set the variables below only if yours differ.

## 2. Start HMS with forwarding on

Add to `HMS\.env`:

```
FAIRRELAY_ENABLED=true
FAIRRELAY_API_KEY=<same value as PRODUCER_API_KEY in fairrelay\.env>
FAIRRELAY_TENANT_ID=4
FAIRRELAY_ENDPOINT_ID=4
```

```powershell
cd C:\Users\snipe\Documents\HMS
docker compose up -d --build notification-ms
docker logs hms-notification-ms 2>&1 | Select-String "FairRelay"
```

Expected: `FairRelay forwarder on: url=http://host.docker.internal:8081 tenant=4 endpoint=4`.

## 3. Start SwayPay with publishing on

SwayPay still needs its own Postgres (5432) and Redis (6379) as before.

```powershell
cd C:\Users\snipe\Documents\SwayPay\backend
$env:FAIRRELAY_ENABLED = "true"
$env:FAIRRELAY_API_KEY = (Select-String -Path ..\..\fairrelay\.env -Pattern '^PRODUCER_API_KEY=').Line.Split('=', 2)[1].Trim()
$env:FAIRRELAY_TENANT_ID = "3"
$env:FAIRRELAY_ENDPOINT_ID = "3"
.\mvnw spring-boot:run
```

Expected log line: `FairRelay publisher on: url=http://localhost:8081 tenant=3 endpoint=3`.
The `outbox_events` table is created automatically (`ddl-auto=update`).

## 4. Demos

Watch the receiver in a separate window: `docker compose logs -f fakereceiver` (in the fairrelay folder).

**A. A payment becomes a webhook.** Make a payment in the SwayPay UI.
Receiver shows `received tenant=3 event=<txnId>`. In SwayPay's database:
`SELECT event_id, event_type, sent_at, attempts FROM outbox_events ORDER BY id DESC LIMIT 5;`

**B. An appointment becomes a webhook.** Book an appointment in the HMS UI.
Receiver shows `received tenant=4 event=<uuid>`.

**C. Noisy neighbour, real apps.** Make the clinic's endpoint hang, then use both apps:

```powershell
curl.exe -X PUT http://localhost:9900/mode/4/hang
```

Book several appointments and make payments. Payments still arrive at once; HMS's webhooks wait
in FairRelay and retry with backoff, never using more than its cap of workers. Undo with `/mode/4/ok`.

**D. FairRelay down, nothing lost (the outbox).**

```powershell
docker compose stop server      # in the fairrelay folder
```

Make 3 payments: they succeed, and `outbox_events` shows `sent_at` NULL with `last_error` set.
`docker compose start server`, and within a second they are sent and delivered.
HMS behaves the same way: Kafka keeps the events until FairRelay accepts them.

## Design notes

- **Why an outbox in SwayPay:** "commit payment, then call FairRelay" loses the webhook if the app dies in between.
  The outbox row commits in the same transaction as the payment, so both exist or neither does.
- **Why a separate consumer group in HMS:** the forwarder has its own Kafka offset, so a FairRelay outage
  delays only webhooks, never HMS's own notifications.
- **At-least-once, end to end:** both producers may send an event twice after a crash; FairRelay accepts the repeat
  as `200 duplicate` because the `event_id` is the same.
- **Thin HMS events:** only ids, times and status leave HMS; patient names, contacts and reasons do not.
- **Simplifications:** one shared producer API key for all tenants (a real system issues one key per tenant and checks
  that it matches `tenant_id`), and one endpoint per app (a real system has one per merchant or clinic).

## Troubleshooting

| Symptom | Cause |
|---|---|
| `last_error = FairRelay replied 401` | `FAIRRELAY_API_KEY` doesn't match FairRelay's `PRODUCER_API_KEY` |
| `replied 422` | tenant/endpoint ids don't match the seed output |
| HMS log: `ConnectException` to `host.docker.internal` | FairRelay server not running, or its port isn't 8081 |
| `port is already allocated` for 9000 | an old FairRelay receiver container; run `docker compose up -d` in fairrelay to recreate it on 9900 |
