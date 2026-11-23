package retry

import (
	"testing"
	"time"
)

func TestNextStaysInRange(t *testing.T) {
	base := 1 * time.Second
	max := 10 * time.Minute
	for attempt := 1; attempt <= 100; attempt++ {
		got := Next(attempt, base, max)
		t.Logf("attempt %d → %v", attempt, got)

		if got < 0 || got > max {
			t.Errorf("attempt %d: %v out of range", attempt, got)
		}
	}
}