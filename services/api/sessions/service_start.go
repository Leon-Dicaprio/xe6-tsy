package sessions

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Start validates control-plane prerequisites before starting realtime. The
// business session remains created until the media plane reports success.
func (s *Service) Start(ctx context.Context, input StartInput) (VoiceSession, error) {
	if err := ctx.Err(); err != nil {
		return VoiceSession{}, err
	}
	if err := validateIdentity(input.AccountID, input.SessionID); err != nil {
		return VoiceSession{}, err
	}
	if err := validateIdempotency(input.IdempotencyKey, input.RequestHash); err != nil {
		return VoiceSession{}, err
	}
	if input.TraceID == "" {
		return VoiceSession{}, ErrInvalidRequest
	}
	if input.StartedBy == "" {
		input.StartedBy = input.AccountID
	}

	unlock := s.locks.lock(input.SessionID)
	defer unlock()

	session, err := s.deps.Repository.GetOwned(ctx, input.AccountID, input.SessionID)
	if err != nil {
		return VoiceSession{}, fmt.Errorf("read voice session for start: %w", err)
	}
	switch session.Status {
	case StatusActive:
		return s.replayStart(ctx, input, session)
	case StatusCreated:
		// Continue with the only state allowed to create a realtime pipeline.
	case StatusEnded, StatusFailed:
		return VoiceSession{}, ErrSessionStateConflict
	default:
		return VoiceSession{}, ErrSessionStateConflict
	}
	if err := decodeSessionReadiness(session); err != nil {
		return VoiceSession{}, err
	}

	languageConfig, err := s.deps.LanguageConfigs.GetCurrentConfig(ctx, input.SessionID)
	if err != nil {
		return VoiceSession{}, mapDependencyError(ctx, err, ErrLanguageConfigNotReady)
	}
	if languageConfig.SessionID != input.SessionID || !languageConfig.Ready() {
		return VoiceSession{}, ErrLanguageConfigNotReady
	}

	connection, err := s.deps.WebRTCConnections.GetConnectionState(ctx, input.SessionID)
	if err != nil {
		return VoiceSession{}, mapDependencyError(ctx, err, ErrWebRTCUnavailable)
	}
	if connection.SessionID != input.SessionID || !connection.ConnectionState.Valid() {
		return VoiceSession{}, ErrWebRTCUnavailable
	}
	if !connection.ConnectionState.Ready() {
		return VoiceSession{}, ErrWebRTCNotReady
	}

	runtime, err := s.deps.Realtime.Start(ctx, StartRealtimeCommand{
		SessionID: input.SessionID,
		TraceID:   input.TraceID,
		StartedBy: input.StartedBy,
	})
	if err != nil {
		if errors.Is(err, ErrRealtimeAlreadyRunning) {
			return VoiceSession{}, ErrRealtimeAlreadyRunning
		}
		return VoiceSession{}, mapDependencyError(ctx, err, ErrRealtimeStartFailed)
	}
	if err := validateStartedRuntime(runtime, input.SessionID); err != nil {
		compensationErr := s.compensateStart(ctx, input, s.deps.Clock.Now().UTC(), err)
		return VoiceSession{}, errors.Join(err, compensationErr)
	}

	startedAt := s.deps.Clock.Now().UTC()
	active, _, transitionErr := s.deps.Repository.TransitionToActive(ctx, StartTransitionParams{
		SessionID:      input.SessionID,
		AccountID:      input.AccountID,
		Expected:       StatusCreated,
		StartedAt:      startedAt,
		IdempotencyKey: input.IdempotencyKey,
		RequestHash:    input.RequestHash,
	})
	if transitionErr == nil {
		return active, nil
	}

	compensationErr := s.compensateStart(ctx, input, startedAt, transitionErr)
	if compensationErr != nil {
		return VoiceSession{}, errors.Join(
			fmt.Errorf("transition voice session to active: %w", transitionErr),
			compensationErr,
		)
	}
	return VoiceSession{}, fmt.Errorf("transition voice session to active: %w", transitionErr)
}

func (s *Service) replayStart(ctx context.Context, input StartInput, current VoiceSession) (VoiceSession, error) {
	startedAt := s.deps.Clock.Now().UTC()
	if current.StartedAt != nil {
		startedAt = current.StartedAt.UTC()
	}
	session, replayed, err := s.deps.Repository.TransitionToActive(ctx, StartTransitionParams{
		SessionID:      input.SessionID,
		AccountID:      input.AccountID,
		Expected:       StatusCreated,
		StartedAt:      startedAt,
		IdempotencyKey: input.IdempotencyKey,
		RequestHash:    input.RequestHash,
	})
	if err != nil {
		return VoiceSession{}, fmt.Errorf("replay voice session start: %w", err)
	}
	if !replayed {
		return VoiceSession{}, ErrSessionStateConflict
	}
	return session, nil
}

func validateStartedRuntime(runtime RuntimeSnapshot, sessionID string) error {
	if err := validateRuntimeSnapshot(runtime, sessionID); err != nil {
		return fmt.Errorf("%w: invalid start snapshot", ErrRealtimeStartFailed)
	}
	switch runtime.RuntimeState {
	case RuntimeStarting,
		RuntimeListening,
		RuntimeASRProcessing,
		RuntimeTranslating,
		RuntimeTTSProcessing,
		RuntimePlaying:
		return nil
	default:
		return ErrRealtimeStartFailed
	}
}

func (s *Service) compensateStart(
	parent context.Context,
	input StartInput,
	startedAt time.Time,
	cause error,
) error {
	ctx, cancel := s.compensationContext(parent)
	defer cancel()

	runtime, stopErr := s.deps.Realtime.Stop(ctx, StopRealtimeCommand{
		SessionID: input.SessionID,
		TraceID:   input.TraceID,
		Reason:    EndReasonOperatorCancelled,
		EndedAt:   startedAt,
	})
	if stopErr == nil {
		stopErr = validateStoppedRuntime(runtime, input.SessionID)
	}
	if stopErr == nil {
		s.deps.Logger.WarnContext(ctx, "compensated realtime start after business transition failure",
			slog.String("request_id", input.TraceID),
			slog.String("session_id", input.SessionID),
			slog.Any("original_error", cause),
		)
		return nil
	}

	s.deps.Logger.ErrorContext(ctx, "failed to compensate realtime start",
		slog.String("request_id", input.TraceID),
		slog.String("session_id", input.SessionID),
		slog.Any("original_error", cause),
		slog.Any("compensation_error", stopErr),
	)
	return fmt.Errorf("compensate realtime start: %w", stopErr)
}
