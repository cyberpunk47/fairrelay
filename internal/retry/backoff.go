package retry

import (
	"math/rand/v2"
	"time"
)

func Next(attempt int, base , max time.Duration) time.Duration{
	// Variable d is the duration to wait before the next attempt. It starts at the base duration and doubles with each attempt, up to a maximum of max.
	// base is the initial duration to wait before the first retry attempt. 
	// max is the maximum duration to wait between attempts. 
	// attempt is the current attempt number (starting from 1).
	d := base 
	// Ensure that the attempt number is at least 1 to avoid negative or zero durations.
	if attempt < 1 {
		attempt = 1
	}
	// Loop to double the duration for each attempt, but not exceeding the maximum duration.
	for i := 1; i < attempt && d < max; i++ {
		d *= 2
	}
	// If the calculated duration exceeds the maximum, set it to the maximum.
	if d > max{
		d = max
	}
	// Jitter addition
	half := d / 2
	// Add a random jitter to the duration to avoid thundering herd problems. The jitter is a random value between -half and +half of the calculated duration.
	return 	half + time.Duration(rand.Int64N(int64(half) + 1))

}