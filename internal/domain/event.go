package domain

import "time"

type NewEvent struct {
	TenantID   int64
	EndpointID int64
	ExternalID string
	EventType  string
	Payload    string
}
type ClaimedEvent struct {
	ID         int64
	TenantID   int64
	EndpointID int64
	ExternalID string
	EventType  string
	Payload    string
	Attempts   int
	LeaseToken int
}
type AttemptResult struct {
	EventID       int64
	LeaseToken    int
	AttemptNumber int
	StartedAt     time.Time
	FinishedAt    time.Time
	DurationMs    int
	HTTPStatus    int    // 0 means no reply came back; otherwise the HTTP status
	Error         string // empty "" means no error
}

type Endpoint struct {
	ID       int64
	TenantID int64
	URL      string
	Secret   string
}

const (
	StatusPending   = "PENDING"
	StatusInFlight  = "IN_FLIGHT"
	StatusDelivered = "DELIVERED"
	StatusDead      = "DEAD"
)

// Circuit breaker states of an endpoint (endpoints.breaker_state).
const (
	BreakerClosed   = "CLOSED"    // normal: deliveries flow
	BreakerOpen     = "OPEN"      // too many failures: nothing is sent until next_probe_at
	BreakerHalfOpen = "HALF_OPEN" // cooldown over: one test delivery decides CLOSED or OPEN again
)

// DueTenant is a tenant that has events ready to send, with its DRR weight.
type DueTenant struct {
	ID     int64
	Weight int
}

// EventView is what GET /events/{id} returns: the event plus every delivery try.
type EventView struct {
	ID            int64         `json:"id"`
	TenantID      int64         `json:"tenant_id"`
	EndpointID    int64         `json:"endpoint_id"`
	EventID       string        `json:"event_id"`
	EventType     string        `json:"event_type"`
	Status        string        `json:"status"`
	Attempts      int           `json:"attempts"`
	NextAttemptAt time.Time     `json:"next_attempt_at"`
	LastError     *string       `json:"last_error"`   // null when there is no error
	DeliveredAt   *time.Time    `json:"delivered_at"` // null until delivered
	CreatedAt     time.Time     `json:"created_at"`
	History       []AttemptView `json:"history"`
}

// AttemptView is one row of delivery_attempts.
type AttemptView struct {
	AttemptNumber int       `json:"attempt_number"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
	DurationMs    int       `json:"duration_ms"`
	HTTPStatus    *int      `json:"http_status"` // null when no reply came back
	Error         *string   `json:"error"`
}
