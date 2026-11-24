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
	HTTPStatus    int    // 0 means No Response, 200-599 means HTTP response, 600 means network error
	Error         string // Emppt "" means no error
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
