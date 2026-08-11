package sessions

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

// ULIDGenerator creates prefixed session and start-operation identities.
type ULIDGenerator struct {
	entropy *ulid.LockedMonotonicReader
}

// NewULIDGenerator returns a concurrency-safe production ID generator.
func NewULIDGenerator() *ULIDGenerator {
	return &ULIDGenerator{
		entropy: &ulid.LockedMonotonicReader{
			MonotonicReader: ulid.Monotonic(rand.Reader, 0),
		},
	}
}

// NewVoiceSessionID returns the durable identity for one complete voice
// conversation. The prefix keeps session IDs distinguishable in logs and data.
func (g *ULIDGenerator) NewVoiceSessionID() string {
	return "vs_" + g.newID()
}

// NewStartOperationID returns the identity shared by the durable StartOperation
// and the realtime runtime it owns.
func (g *ULIDGenerator) NewStartOperationID() string {
	return "op_" + g.newID()
}

// newID prefers monotonic, concurrency-safe ULIDs. The fallback paths preserve
// availability if a partially initialized generator is used or entropy fails;
// callers still validate that the returned identifier is non-empty.
func (g *ULIDGenerator) newID() string {
	if g == nil || g.entropy == nil {
		return ulid.Make().String()
	}
	id, err := ulid.New(ulid.Timestamp(time.Now().UTC()), g.entropy)
	if err != nil {
		// Appending wall-clock nanoseconds makes an entropy failure less likely to
		// collapse two fallback IDs onto the same value.
		return fmt.Sprintf("%s%x", ulid.Make().String(), time.Now().UnixNano())
	}
	return id.String()
}
