package sessions

import (
	"encoding/base64"
	"encoding/json"
	"time"
)

// sessionListCursorVersion identifies the serialized keyset cursor contract.
const sessionListCursorVersion = 1

// sessionListCursor stores the final row's stable sort tuple. Versioning makes
// an incompatible future encoding fail closed instead of silently changing the
// meaning of an existing client cursor.
type sessionListCursor struct {
	Version   int       `json:"v"`
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

// encodeSessionListCursor creates an opaque URL-safe cursor for descending
// keyset pagination by (created_at, id). UTC normalization keeps equivalent
// instants byte-stable across hosts.
func encodeSessionListCursor(createdAt time.Time, id string) (string, error) {
	if createdAt.IsZero() || id == "" {
		return "", ErrInvalidRequest
	}
	payload, err := json.Marshal(sessionListCursor{
		Version:   sessionListCursorVersion,
		CreatedAt: createdAt.UTC(),
		ID:        id,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

// decodeSessionListCursor treats malformed, unsupported, or incomplete cursor
// payloads as invalid client input. It never falls back to offset pagination.
func decodeSessionListCursor(encoded string) (sessionListCursor, error) {
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return sessionListCursor{}, ErrInvalidRequest
	}
	var cursor sessionListCursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return sessionListCursor{}, ErrInvalidRequest
	}
	if cursor.Version != sessionListCursorVersion ||
		cursor.CreatedAt.IsZero() ||
		cursor.ID == "" {
		return sessionListCursor{}, ErrInvalidRequest
	}
	cursor.CreatedAt = cursor.CreatedAt.UTC()
	return cursor, nil
}
