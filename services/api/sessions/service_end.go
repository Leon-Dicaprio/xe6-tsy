package sessions

import (
	"context"
	"fmt"
)

// End persists request identity before attempting cleanup, so every failure
// after SaveEndIntent remains recoverable by replay or ResumeEnd.
func (s *Service) End(ctx context.Context, input EndInput) (VoiceSession, error) {
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
	if input.Reason == "" {
		input.Reason = EndReasonUserRequested
	}
	if !input.Reason.Valid() {
		return VoiceSession{}, ErrInvalidRequest
	}

	unlock := s.locks.lock(input.SessionID)
	defer unlock()

	session, err := s.deps.Repository.GetOwned(ctx, input.AccountID, input.SessionID)
	if err != nil {
		return VoiceSession{}, fmt.Errorf("read voice session for end: %w", err)
	}
	intent, _, err := s.deps.Repository.SaveEndIntent(ctx, EndIntent{
		SessionID:      input.SessionID,
		AccountID:      input.AccountID,
		Reason:         input.Reason,
		IdempotencyKey: input.IdempotencyKey,
		RequestHash:    input.RequestHash,
		RequestedAt:    s.deps.Clock.Now().UTC(),
	})
	if err != nil {
		return VoiceSession{}, fmt.Errorf("save voice session end intent: %w", err)
	}
	if !intent.MatchesRequest(input.IdempotencyKey, input.RequestHash) {
		return VoiceSession{}, ErrIdempotencyKeyConflict
	}
	return s.finishEnd(ctx, session, intent, input.TraceID)
}

// ResumeEnd completes an existing intent without allowing a worker to replace
// its authenticated account, idempotency identity, or terminal reason.
func (s *Service) ResumeEnd(ctx context.Context, input ResumeEndInput) (VoiceSession, error) {
	if err := ctx.Err(); err != nil {
		return VoiceSession{}, err
	}
	if err := validateIdentity(input.AccountID, input.SessionID); err != nil {
		return VoiceSession{}, err
	}
	if input.TraceID == "" {
		return VoiceSession{}, ErrInvalidRequest
	}

	unlock := s.locks.lock(input.SessionID)
	defer unlock()

	session, err := s.deps.Repository.GetOwned(ctx, input.AccountID, input.SessionID)
	if err != nil {
		return VoiceSession{}, fmt.Errorf("read voice session for end recovery: %w", err)
	}
	intent, err := s.deps.Repository.GetEndIntent(ctx, input.AccountID, input.SessionID)
	if err != nil {
		return VoiceSession{}, fmt.Errorf("read voice session end intent: %w", err)
	}
	return s.finishEnd(ctx, session, intent, input.TraceID)
}

func (s *Service) finishEnd(
	ctx context.Context,
	session VoiceSession,
	intent EndIntent,
	traceID string,
) (VoiceSession, error) {
	if intent.SessionID != session.ID || intent.AccountID != session.AccountID || !intent.Reason.Valid() {
		return VoiceSession{}, ErrInvalidRequest
	}
	if intent.Completed() {
		if session.Status == StatusEnded || session.Status == StatusFailed {
			return session, nil
		}
		return VoiceSession{}, ErrSessionStateConflict
	}

	switch session.Status {
	case StatusEnded, StatusFailed:
		return session, s.completeEndIntent(ctx, session)
	case StatusCreated:
		ended, err := s.deps.Repository.TransitionToEnded(ctx, EndTransitionParams{
			SessionID: session.ID,
			AccountID: session.AccountID,
			Expected:  StatusCreated,
			EndedAt:   s.deps.Clock.Now().UTC(),
			EndReason: intent.Reason,
		})
		if err != nil {
			return VoiceSession{}, fmt.Errorf("end created voice session: %w", err)
		}
		return ended, s.completeEndIntent(ctx, ended)
	case StatusActive:
		return s.stopAndEndActive(ctx, session, intent, traceID)
	default:
		return VoiceSession{}, ErrSessionStateConflict
	}
}

func (s *Service) stopAndEndActive(
	ctx context.Context,
	session VoiceSession,
	intent EndIntent,
	traceID string,
) (VoiceSession, error) {
	endedAt := s.deps.Clock.Now().UTC()
	runtime, err := s.deps.Realtime.Stop(ctx, StopRealtimeCommand{
		SessionID: session.ID,
		TraceID:   traceID,
		Reason:    intent.Reason,
		EndedAt:   endedAt,
	})
	if err != nil {
		return VoiceSession{}, mapDependencyError(ctx, err, ErrRealtimeStopFailed)
	}
	if err := validateStoppedRuntime(runtime, session.ID); err != nil {
		return VoiceSession{}, err
	}

	ended, err := s.deps.Repository.TransitionToEnded(ctx, EndTransitionParams{
		SessionID: session.ID,
		AccountID: session.AccountID,
		Expected:  StatusActive,
		EndedAt:   endedAt,
		EndReason: intent.Reason,
	})
	if err != nil {
		// The incomplete intent deliberately remains persisted. A retry invokes
		// the idempotent Stop again before retrying this conditional transition.
		return VoiceSession{}, fmt.Errorf("transition active voice session to ended: %w", err)
	}
	return ended, s.completeEndIntent(ctx, ended)
}

func (s *Service) completeEndIntent(
	ctx context.Context,
	session VoiceSession,
) error {
	if err := s.deps.Repository.CompleteEndIntent(
		ctx,
		session.AccountID,
		session.ID,
		s.deps.Clock.Now().UTC(),
	); err != nil {
		return fmt.Errorf("complete voice session end intent: %w", err)
	}
	return nil
}
