package sessions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
)

const defaultCompensationTimeout = 5 * time.Second

// Dependencies contains the required control-plane and media-plane boundaries.
// Logger is optional; a discard logger is used when it is absent.
type Dependencies struct {
	Repository          Repository
	LanguageConfigs     LanguageConfigReader
	WebRTCConnections   WebRTCConnectionReader
	Realtime            RealtimeLifecycle
	IDs                 IDGenerator
	Clock               Clock
	Logger              *slog.Logger
	CompensationTimeout time.Duration
}

// Service owns authenticated voice-session use cases without depending on HTTP
// or constructing infrastructure adapters.
type Service struct {
	deps  Dependencies
	locks keyedLocker
}

// NewService rejects a partially wired module before any use case can run.
func NewService(deps Dependencies) (*Service, error) {
	if deps.Repository == nil ||
		deps.LanguageConfigs == nil ||
		deps.WebRTCConnections == nil ||
		deps.Realtime == nil ||
		deps.IDs == nil ||
		deps.Clock == nil {
		return nil, errors.New("sessions: missing required dependency")
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if deps.CompensationTimeout <= 0 {
		deps.CompensationTimeout = defaultCompensationTimeout
	}
	return &Service{deps: deps, locks: newKeyedLocker()}, nil
}

// CreateInput carries authenticated ownership and canonical request identity.
type CreateInput struct {
	AccountID      string
	AudioConfig    *AudioConfig
	Capabilities   Capabilities
	IdempotencyKey string
	RequestHash    string
}

// StartInput carries authenticated ownership and audit metadata.
type StartInput struct {
	AccountID      string
	SessionID      string
	IdempotencyKey string
	RequestHash    string
	TraceID        string
	StartedBy      string
}

// EndInput carries authenticated ownership and a resumable request identity.
type EndInput struct {
	AccountID      string
	SessionID      string
	IdempotencyKey string
	RequestHash    string
	TraceID        string
	Reason         EndReason
}

// ResumeEndInput identifies an already-persisted end intent for background
// recovery. It never accepts a replacement idempotency key or reason.
type ResumeEndInput struct {
	AccountID string
	SessionID string
	TraceID   string
}

// DetailInput identifies an account-scoped session read.
type DetailInput struct {
	AccountID string
	SessionID string
}

// ListInput carries account-scoped persistent filters only.
type ListInput struct {
	AccountID string
	Status    *Status
	Cursor    string
	Limit     int
}

func validateIdentity(accountID string, sessionID string) error {
	if accountID == "" {
		return ErrUnauthorized
	}
	if sessionID == "" {
		return ErrInvalidRequest
	}
	return nil
}

func validateIdempotency(key string, requestHash string) error {
	if key == "" || requestHash == "" {
		return ErrInvalidRequest
	}
	return nil
}

func validateAudioConfig(config AudioConfig) error {
	if config.Codec != "opus" || config.SampleRateHz != 48000 || config.Channels != 1 {
		return ErrUnsupportedAudio
	}
	return nil
}

func validateCapabilities(capabilities Capabilities) error {
	if !capabilities.WebRTC ||
		!capabilities.DataChannel ||
		!capabilities.Microphone ||
		!capabilities.Speaker ||
		!capabilities.SpeakerDiarization {
		return ErrInvalidRequest
	}
	return nil
}

func decodeSessionReadiness(session VoiceSession) error {
	var audio AudioConfig
	if err := json.Unmarshal(session.AudioConfig, &audio); err != nil {
		return fmt.Errorf("%w: decode persisted audio config: %v", ErrUnsupportedAudio, err)
	}
	if err := validateAudioConfig(audio); err != nil {
		return err
	}

	var capabilities Capabilities
	if err := json.Unmarshal(session.Capabilities, &capabilities); err != nil {
		return fmt.Errorf("%w: decode persisted capabilities: %v", ErrInvalidRequest, err)
	}
	return validateCapabilities(capabilities)
}

func validateRuntimeSnapshot(snapshot RuntimeSnapshot, sessionID string) error {
	if snapshot.SessionID != sessionID || !snapshot.RuntimeState.Valid() || snapshot.UpdatedAt.IsZero() {
		return ErrRuntimeUnavailable
	}
	return nil
}

func validateStoppedRuntime(runtime RuntimeSnapshot, sessionID string) error {
	if err := validateRuntimeSnapshot(runtime, sessionID); err != nil {
		return fmt.Errorf("%w: invalid stop snapshot", ErrRealtimeStopFailed)
	}
	if runtime.RuntimeState != RuntimeStopped {
		return ErrRealtimeStopFailed
	}
	return nil
}

func mapDependencyError(ctx context.Context, err error, boundary error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, ErrNotImplemented) {
		return ErrNotImplemented
	}
	return fmt.Errorf("%w: %v", boundary, err)
}

func (s *Service) compensationContext(parent context.Context) (context.Context, context.CancelFunc) {
	// Compensation retains trace values but cannot be cancelled by a disconnected
	// client; its own timeout prevents an unbounded cross-service cleanup call.
	return context.WithTimeout(context.WithoutCancel(parent), s.deps.CompensationTimeout)
}
