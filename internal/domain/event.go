package domain

type NewEvent struct{
	TenantID int64 ;
	EndpointID int64 ;
	ExternalID string ;
	EventType string ;
	Payload string ;
}
const (
	StatusPending = "PENDING"
	StatusInFlight = "IN_FLIGHT"
	StatusDelivered = "DELIVERED"
	StatusDead = "DEAD"

)