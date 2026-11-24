package ingest

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/store"
)

const maxBodyBytes = 1 << 20 // 1 MB

// request is the JSON a producer sends.
type request struct {
	TenantID   int64           `json:"tenant_id"`
	EndpointID int64           `json:"endpoint_id"`
	EventID    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	Payload    json.RawMessage `json:"payload"` // raw bytes, kept exactly as sent
}

type Handler struct {
	store  store.Store // the interface, not *Postgres
	apiKey string
}

func NewHandler(s store.Store, apiKey string) *Handler {
	return &Handler{store: s, apiKey: apiKey}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

// authorized checks the X-API-Key header.
// Constant-time compare, so the key can't be guessed by timing the replies.
func (h *Handler) authorized(r *http.Request) bool {
	key := r.Header.Get("X-API-Key")
	return subtle.ConstantTimeCompare([]byte(key), []byte(h.apiKey)) == 1
}

// CreateEvent handles POST /events.
func (h *Handler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	// 1. API key
	if !h.authorized(r) {
		writeError(w, http.StatusUnauthorized, "invalid api key")
		return
	}

	// 2. Body size limit
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	// 3. Decode JSON
	var req request
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "body larger than 1 MB")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	// 4. Validate
	if msg := validate(req); msg != "" {
		writeError(w, http.StatusBadRequest, msg)
		return
	}

	// 5. Save (every field filled in)
	id, created, err := h.store.InsertEvent(r.Context(), domain.NewEvent{
		TenantID:   req.TenantID,
		EndpointID: req.EndpointID,
		ExternalID: req.EventID,
		EventType:  req.EventType,
		Payload:    string(req.Payload),
	})

	// 6. Reply
	if errors.Is(err, store.ErrUnknownEndpoint) {
		writeError(w, http.StatusUnprocessableEntity, "unknown endpoint for this tenant")
		return
	}
	if err != nil {
		log.Printf("insert event failed: tenant=%d event=%s: %v", req.TenantID, req.EventID, err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if created {
		writeJSON(w, http.StatusAccepted, map[string]any{"id": id, "duplicate": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"duplicate": true})
}

// validate returns "" when the request is fine, otherwise a message for the client.
func validate(req request) string {
	if req.TenantID <= 0 {
		return "tenant_id must be greater than 0"
	}
	if req.EndpointID <= 0 {
		return "endpoint_id must be greater than 0"
	}
	if req.EventID == "" {
		return "event_id is required"
	}
	if req.EventType == "" {
		return "event_type is required"
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		return "payload is required"
	}
	if !json.Valid(req.Payload) {
		return "payload must be valid JSON"
	}
	return ""
}

// GetEvent handles GET /events/{id}: the event's status plus every delivery try.
func (h *Handler) GetEvent(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		writeError(w, http.StatusUnauthorized, "invalid api key")
		return
	}

	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "id must be a positive number")
		return
	}

	ev, err := h.store.GetEvent(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "event not found")
		return
	}
	if err != nil {
		log.Printf("get event %d failed: %v", id, err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, ev)
}

// Health handles GET /healthz: 200 when the database answers, 503 when it doesn't.
// No API key, so Docker and load balancers can call it.
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	if err := h.store.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "database unreachable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
