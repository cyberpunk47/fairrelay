package main

import (
	"log"
	"net/http"
	"os"
	"sync"
	"time"
)

// receiver pretends to be many customers' servers.
// Each tenant has a mode that controls how its "server" behaves.
type receiver struct {
	mu    sync.Mutex
	modes map[string]string // tenant ID → mode
}

func (rc *receiver) getMode(tenant string) string {
	rc.mu.Lock()
	m := rc.modes[tenant]
	rc.mu.Unlock()
	if m == "" {
		return "ok"
	}
	return m
}

func (rc *receiver) setMode(tenant string, mode string) {
	rc.mu.Lock()
	rc.modes[tenant] = mode
	rc.mu.Unlock()
}

// handleWebhook: POST /t/{tenant}, where webhooks are delivered.
func (rc *receiver) handleWebhook(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")
	eventID := r.Header.Get("X-Webhook-ID")
	mode := rc.getMode(tenant)

	// slow: wait 2 s, then carry on to the normal 200 below
	if mode == "slow" {
		time.Sleep(2 * time.Second)
	}

	// hang: wait up to 60 s, but stop as soon as the sender gives up
	if mode == "hang" {
		select {
		case <-r.Context().Done(): // sender disconnected (its timeout fired)
			log.Printf("sender gave up tenant=%s event=%s", tenant, eventID)
			return // not counted as received
		case <-time.After(60 * time.Second):
		}
	}

	// error: reply 500
	if mode == "error" {
		log.Printf("replied 500 tenant=%s event=%s", tenant, eventID)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("error"))
		return
	}

	// ratelimit: reply 429 and ask the sender to wait 5 s
	if mode == "ratelimit" {
		log.Printf("replied 429 tenant=%s event=%s", tenant, eventID)
		w.Header().Set("Retry-After", "5") // headers must be set BEFORE WriteHeader
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte("rate limit exceeded"))
		return
	}

	// ok (and slow after its wait): success
	log.Printf("received tenant=%s event=%s mode=%s", tenant, eventID, mode)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// handleSetMode: PUT /mode/{tenant}/{mode}, which changes a tenant's behaviour.
func (rc *receiver) handleSetMode(w http.ResponseWriter, r *http.Request) {
	tenant := r.PathValue("tenant")
	mode := r.PathValue("mode")

	if mode != "ok" && mode != "slow" && mode != "hang" && mode != "error" && mode != "ratelimit" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid mode: use ok, slow, hang, error or ratelimit"))
		return
	}

	rc.setMode(tenant, mode)
	log.Printf("set tenant=%s mode=%s", tenant, mode)
	w.WriteHeader(http.StatusNoContent) // 204: done, nothing to return
}

func main() {
	addr := os.Getenv("RECEIVER_ADDR")
	if addr == "" {
		addr = ":9000"
	}

	rc := &receiver{
		modes: make(map[string]string),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /t/{tenant}", rc.handleWebhook)
	mux.HandleFunc("PUT /mode/{tenant}/{mode}", rc.handleSetMode)

	log.Printf("fake receiver listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}