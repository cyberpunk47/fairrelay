-- Table creation 
-- The tenants table is the main table that will be used to store tenent information.
-- name is Unique for each tenent, max_inflight is the maximum number of events that can be in flight for a tenent, weight is the weight share of the tenant which it will get using drr.
CREATE TABLE tenants(
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    name TEXT NOT NULL UNIQUE,
    max_inflight INT NOT NULL DEFAULT 2 CHECK (max_inflight > 0),
    weight INT NOT NULL DEFAULT 1 CHECK (weight > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- The Endpoint details Table Stores the Endpoint Details
-- Id basically the Endpoints Number , tenant_id is which customer(tenant) owns it , url is the address we are posting to , secret is the secret key for the endpoint, breaker_state is the state of the circuit breaker for the endpoint, consecutive_failures is the number of consecutive failures for the endpoint, next_probe_at is when we will next probe the endpoint, created_at is when the endpoint was created.
CREATE TABLE endpoints(
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    url TEXT NOT NULL,
    secret TEXT NOT NULL,
    breaker_state TEXT NOT NULL DEFAULT 'CLOSED' CHECK (breaker_state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
    consecutive_failures INT NOT NULL DEFAULT 0 check (consecutive_failures >= 0),
    next_probe_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    unique (id, tenant_id)
);

-- The Events Table Stores the Event Details
-- Id is the Event Number, tenant_id is which customer(tenant) owns it, endpoint_id is which endpoint we are posting to, event_id is the unique id of the event, event_type is the type of the event, payload is the data we are posting, status is the status of the event, attempts is the number of attempts we have made to deliver the event, next_attempt_at is when we will next attempt to deliver the event, lease_until is when we will release the lease on the event, lease_token is a token that we use to identify the lease, last_error is the last error we received when trying to deliver the event, delivered_at is when we successfully delivered the event, created_at is when the event was created.
create table events(
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    endpoint_id BIGINT NOT NULL REFERENCES endpoints(id) ON DELETE RESTRICT,
    external_id TEXT not null,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'IN_FLIGHT', 'DELIVERED', 'DEAD')),
    attempts INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_until TIMESTAMPTZ CHECK(status = 'IN_FLIGHT' AND lease_until IS NOT NULL OR status != 'IN_FLIGHT' AND lease_until IS NULL),
    lease_token INT NOT NULL DEFAULT 0,
    last_error TEXT,
    delivered_at TIMESTAMPTZ CHECK(status = 'DELIVERED' AND delivered_at IS NOT NULL OR status != 'DELIVERED' AND delivered_at IS NULL),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id , external_id),
    FOREIGN KEY (endpoint_id, tenant_id) REFERENCES endpoints (id, tenant_id)
);
-- The Delivery Attempts Table Stores the Delivery Attempt Details
-- Id is the Delivery Attempt Number, event_ref is which event we are attempting to deliver, attempt_number is the number of attempts we have made to deliver the event, lease_token is a token that we use to identify the lease, started_at is when we started the delivery attempt, finished_at is when we finished the delivery attempt, http_status is the http status code we received when trying to deliver the event, error is the error we received when trying to deliver the event, duration_ms is how long it took to deliver the event in milliseconds.
create table delivery_attempts(
    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    event_ref BIGINT NOT NULL REFERENCES events(id) ON DELETE RESTRICT,
    attempt_number INT NOT NULL CHECK (attempt_number > 0),
    lease_token INT NOT NULL,
    started_at TIMESTAMPTZ not null,
    finished_at TIMESTAMPTZ not null,
    http_status INT,
    error TEXT,
    duration_ms INT not null check (duration_ms >= 0),
    unique(event_ref, lease_token)
);