package delivery

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/cyberpunk47/fairrelay/internal/domain"
	"github.com/cyberpunk47/fairrelay/internal/security"
)

// Result is what happened during one HTTP try. The sender only reports;
// deciding delivered / retry / dead is a separate job.
type Result struct {
	StatusCode int   // 0 = no reply came back
	Err        error // timeout, connection refused, ...; nil if we got a reply
	StartedAt  time.Time
	FinishedAt time.Time
	RetryAfter time.Duration // from the Retry-After header; 0 if none
}

type Sender struct {
	client *http.Client
}

// NewSender builds one HTTP client that all workers share.
// Sharing it lets connections be reused instead of reopened for every webhook.
func NewSender(connectTimeout, requestTimeout time.Duration) *Sender {
	transport := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: connectTimeout}).DialContext, // give up connecting after 2 s
		MaxIdleConnsPerHost: 10,                                                 // keep up to 10 open connections per customer
		IdleConnTimeout:     90 * time.Second,                                   // close connections unused for 90 s
	}
	return &Sender{
		client: &http.Client{
			Transport: transport,
			Timeout:   requestTimeout, // the whole try (connect + send + reply) must finish in 10 s
		},
	}
}

// Send makes one POST to the endpoint and reports what happened.
func (s *Sender) Send(ctx context.Context, ep domain.Endpoint, ev domain.ClaimedEvent) Result {
	res := Result{StartedAt: time.Now()}
	body := []byte(ev.Payload) // the exact bytes the producer sent

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.URL, bytes.NewReader(body))
	if err != nil { // e.g. the URL in the database is malformed
		res.Err = err
		res.FinishedAt = time.Now()
		return res
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "FairRelay/1.0")
	req.Header.Set("X-Webhook-ID", ev.ExternalID) // lets the customer ignore duplicates
	req.Header.Set("X-Webhook-Type", ev.EventType)
	req.Header.Set("X-Signature-256", security.Sign(ep.Secret, res.StartedAt.Unix(), body))

	resp, err := s.client.Do(req) // actually send it and wait for the reply
	res.FinishedAt = time.Now()
	if err != nil { // no reply at all: timeout, refused, DNS failure...
		res.Err = err
		return res
	}
	defer resp.Body.Close()

	// Read and throw away the reply body (up to 64 KB).
	// Go can only reuse the connection once the body has been fully read.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	res.StatusCode = resp.StatusCode
	secs, err := strconv.Atoi(resp.Header.Get("Retry-After"))
	if err == nil && secs > 0 {
		res.RetryAfter = time.Duration(secs) * time.Second
	}
	return res
}
