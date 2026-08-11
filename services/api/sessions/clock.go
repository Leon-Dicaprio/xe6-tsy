package sessions

import "time"

// SystemClock is the production wall-clock adapter. Session timestamps are
// stored and compared in UTC.
type SystemClock struct{}

// Now returns a UTC timestamp so callers never persist host-local timezone
// interpretations in lifecycle records.
func (SystemClock) Now() time.Time {
	return time.Now().UTC()
}
