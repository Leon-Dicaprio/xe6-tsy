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

// Default budgets keep cleanup and reconciliation bounded while allowing
// production composition to override them for deterministic tests or policy.
const (
	defaultCompensationTimeout        = 5 * time.Second
	defaultStartReconciliationTimeout = 5 * time.Second
	defaultEndAttemptTimeout          = 5 * time.Second
	defaultEndRecoveryLeaseDuration   = 10 * time.Second
)

// Dependencies contains the boundaries and time budgets required by session
// lifecycle requests and recovery. Logger is optional and discards by default.
type Dependencies struct {
	Repository                 Repository
	LanguageConfigs            LanguageConfigReader
	WebRTCConnections          WebRTCConnectionReader
	Realtime                   RealtimeLifecycle
	IDs                        IDGenerator
	Clock                      Clock
	Logger                     *slog.Logger
	CompensationTimeout        time.Duration
	StartReconciliationTimeout time.Duration
	EndAttemptTimeout          time.Duration
	EndRecoveryLeaseDuration   time.Duration
}

// Service owns voice-session use cases without depending on HTTP or
// constructing infrastructure adapters.
type Service struct {
	deps  Dependencies
	locks keyedLocker
}

// NewService rejects a partially wired session service.
func NewService(deps Dependencies) (*Service, error) {
	if deps.Repository == nil {
		return nil, fmt.Errorf("%w: repository is required", ErrInvalidDependency)
	}
	if deps.LanguageConfigs == nil {
		return nil, fmt.Errorf("%w: language config reader is required", ErrInvalidDependency)
	}
	if deps.WebRTCConnections == nil {
		return nil, fmt.Errorf("%w: WebRTC connection reader is required", ErrInvalidDependency)
	}
	if deps.Realtime == nil {
		return nil, fmt.Errorf("%w: realtime lifecycle is required", ErrInvalidDependency)
	}
	if deps.IDs == nil {
		return nil, fmt.Errorf("%w: ID generator is required", ErrInvalidDependency)
	}
	if deps.Clock == nil {
		return nil, fmt.Errorf("%w: clock is required", ErrInvalidDependency)
	}
	if deps.Logger == nil {
		deps.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if deps.CompensationTimeout <= 0 {
		deps.CompensationTimeout = defaultCompensationTimeout
	}
	if deps.StartReconciliationTimeout <= 0 {
		deps.StartReconciliationTimeout = defaultStartReconciliationTimeout
	}
	if deps.EndAttemptTimeout <= 0 {
		deps.EndAttemptTimeout = defaultEndAttemptTimeout
	}
	if deps.EndRecoveryLeaseDuration <= 0 {
		deps.EndRecoveryLeaseDuration = max(
			defaultEndRecoveryLeaseDuration,
			2*deps.EndAttemptTimeout,
		)
	}
	if deps.EndAttemptTimeout >= deps.EndRecoveryLeaseDuration {
		return nil, fmt.Errorf(
			"%w: end attempt timeout must be shorter than recovery lease",
			ErrInvalidDependency,
		)
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

// StartInput carries authenticated ownership, idempotency, and audit metadata.
type StartInput struct {
	AccountID      string
	SessionID      string
	IdempotencyKey string
	RequestHash    string
	TraceID        string
	StartedBy      string
}

// EndInput carries authenticated ownership and a durable request identity.
type EndInput struct {
	AccountID      string
	SessionID      string
	IdempotencyKey string
	RequestHash    string
	TraceID        string
	Reason         EndReason
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

// validateIdentity enforces the authorization boundary before any repository or
// realtime call. An absent account is an authentication failure, while an
// absent SessionID is malformed use-case input.
func validateIdentity(accountID string, sessionID string) error {
	if accountID == "" {
		return ErrUnauthorized
	}
	if sessionID == "" {
		return ErrInvalidRequest
	}
	return nil
}

// validateIdempotency requires both the caller-visible key and the canonical
// request fingerprint. Persisting only the key would make different requests
// indistinguishable during replay.
func validateIdempotency(key string, requestHash string) error {
	if key == "" || requestHash == "" {
		return ErrInvalidRequest
	}
	return nil
}

// validateRuntimeSnapshot verifies the minimum cross-service envelope shared by
// query and Stop paths. State-specific rules, such as requiring stopped during
// End, are intentionally applied by the calling workflow.
func validateRuntimeSnapshot(snapshot RuntimeSnapshot, sessionID string) error {
	if snapshot.SessionID != sessionID ||
		!snapshot.RuntimeState.Valid() ||
		snapshot.UpdatedAt.IsZero() {
		return ErrRuntimeUnavailable
	}
	return nil
}

// mapDependencyError preserves cancellation, deadlines, and the explicit
// not-implemented signal while collapsing provider-specific failures behind a
// stable sessions boundary. This keeps HTTP retry semantics independent from
// provider error strings.
func mapDependencyError(ctx context.Context, err error, boundary error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, ErrNotImplemented) {
		return ErrNotImplemented
	}
	return fmt.Errorf("%w: %v", boundary, err)
}

// validateAudioConfig enforces the currently supported media contract before a
// Session is persisted or started. Provider negotiation does not occur here.
func validateAudioConfig(config AudioConfig) error {
	if config.Codec != "opus" || config.SampleRateHz != 48000 || config.Channels != 1 {
		return ErrUnsupportedAudio
	}
	return nil
}

// validateCapabilities requires the complete P0 terminal feature set. These
// flags describe client capability; WebRTC readiness is checked separately
// from the live connection snapshot during Start.
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

// decodeSessionReadiness revalidates persisted JSON before crossing the
// realtime boundary. Treating stored data as trusted here could start a runtime
// with legacy or corrupted capabilities that the current contract rejects.
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

// compensationContext creates one bounded, cancellation-independent cleanup
// step while retaining trace values from the original request.
func (s *Service) compensationContext(parent context.Context) (context.Context, context.CancelFunc) {
	// Compensation retains trace values but ignores client cancellation. Its
	// independent timeout prevents a disconnected request from leaking cleanup.
	return context.WithTimeout(context.WithoutCancel(parent), s.deps.CompensationTimeout)
}

// startReconciliationContext gives ambiguous Realtime.Start outcomes a fresh
// bounded read-and-commit budget after the client request may have ended.
func (s *Service) startReconciliationContext(
	parent context.Context,
) (context.Context, context.CancelFunc) {
	// Reconciliation must outlive an uncertain Start request long enough to
	// determine whether that operation owns a running media pipeline.
	return context.WithTimeout(
		context.WithoutCancel(parent),
		s.deps.StartReconciliationTimeout,
	)
}

// endAttemptContext caps request or worker cleanup by both configured attempt
// timeout and remaining lease duration.
func (s *Service) endAttemptContext(
	parent context.Context,
	leaseRemaining time.Duration,
) (context.Context, context.CancelFunc) {
	// The attempt must finish before its durable lease can be reclaimed by
	// another request or worker. The shorter budget wins if little lease time
	// remains after repository work.
	return context.WithTimeout(parent, min(s.deps.EndAttemptTimeout, leaseRemaining))
}

// endPersistenceContext lets failure bookkeeping release a durable lease even
// after the public request context has been canceled.
func (s *Service) endPersistenceContext(parent context.Context) (context.Context, context.CancelFunc) {
	// A canceled request must still release its durable lease so recovery can
	// resume immediately instead of waiting for expiration.
	return context.WithTimeout(context.WithoutCancel(parent), s.deps.EndAttemptTimeout)
}
