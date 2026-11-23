package domain

type NewEvent struct {
	TenantID   int64
	EndpointID int64
	ExternalID string
	EventType  string
	Payload    string
}
type ClaimedEvent struct{
	ID 	   int64
	TenantID int64
	EndpointID int64
	ExternalID string
	EventType  string
	Payload    string
	Attempts   int
	LeaseToken int
}
const (
	StatusPending   = "PENDING"
	StatusInFlight  = "IN_FLIGHT"
	StatusDelivered = "DELIVERED"
	StatusDead      = "DEAD"
)
