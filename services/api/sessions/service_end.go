package sessions

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// End persists request identity before attempting cleanup. Every failure after
// SaveEndIntent therefore remains recoverable by an idempotent replay.
func (s *Service) End(ctx context.Context, input EndInput) (VoiceSession, error) {
	if err := ctx.Err(); err != nil {
		return VoiceSession{}, err
	}
	if err := validateEndInput(input); err != nil {
		return VoiceSession{}, err
	}

	unlock, err := s.locks.lock(ctx, input.SessionID)
	if err != nil {
		return VoiceSession{}, err
	}
	defer unlock()

	session, err := s.deps.Repository.GetOwned(ctx, input.AccountID, input.SessionID)
	if err != nil {
		return VoiceSession{}, fmt.Errorf("read voice session for end: %w", err)
	}
	requestedAt, err := s.nowUTC("end intent")
	if err != nil {
		return VoiceSession{}, err
	}
	intent, _, err := s.deps.Repository.SaveEndIntent(ctx, EndIntent{
		SessionID:      input.SessionID,
		AccountID:      input.AccountID,
		Reason:         input.Reason,
		IdempotencyKey: input.IdempotencyKey,
		RequestHash:    input.RequestHash,
		RequestedAt:    requestedAt,
	})
	if err != nil {
		return VoiceSession{}, fmt.Errorf("save voice session end intent: %w", err)
	}
	if !intent.MatchesRequest(input.IdempotencyKey, input.RequestHash) {
		return VoiceSession{}, ErrIdempotencyKeyConflict
	}
	if err := validateEndIntent(intent, session, input.Reason); err != nil {
		return VoiceSession{}, err
	}
	return s.finishEnd(ctx, session, intent, input.TraceID)
}

// ResumeEnd completes an existing durable intent without allowing a recovery
// worker to replace its account, idempotency identity, or terminal reason.
func (s *Service) ResumeEnd(
	ctx context.Context,
	input ResumeEndInput,
) (VoiceSession, error) {
	if err := ctx.Err(); err != nil {
		return VoiceSession{}, err
	}
	if err := validateIdentity(input.AccountID, input.SessionID); err != nil {
		return VoiceSession{}, err
	}
	if input.TraceID == "" {
		return VoiceSession{}, ErrInvalidRequest
	}

	unlock, err := s.locks.lock(ctx, input.SessionID)
	if err != nil {
		return VoiceSession{}, err
	}
	defer unlock()

	session, err := s.deps.Repository.GetOwned(ctx, input.AccountID, input.SessionID)
	if err != nil {
		return VoiceSession{}, fmt.Errorf("read voice session for end recovery: %w", err)
	}
	intent, err := s.deps.Repository.GetEndIntent(ctx, input.AccountID, input.SessionID)
	if err != nil {
		return VoiceSession{}, fmt.Errorf("read voice session end intent: %w", err)
	}
	if err := validateEndIntent(intent, session, intent.Reason); err != nil {
		return VoiceSession{}, err
	}
	return s.finishEnd(ctx, session, intent, input.TraceID)
}

func validateEndInput(input EndInput) error {
	if err := validateIdentity(input.AccountID, input.SessionID); err != nil {
		return err
	}
	if err := validateIdempotency(input.IdempotencyKey, input.RequestHash); err != nil {
		return err
	}
	if input.TraceID == "" || !input.Reason.Valid() {
		return ErrInvalidRequest
	}
	return nil
}

func validateEndIntent(intent EndIntent, session VoiceSession, reason EndReason) error {
	if intent.SessionID != session.ID ||
		intent.AccountID != session.AccountID ||
		intent.IdempotencyKey == "" ||
		intent.RequestHash == "" ||
		intent.RequestedAt.IsZero() ||
		!intent.Reason.Valid() ||
		intent.Reason != reason ||
		(intent.CompletedAt != nil && intent.CompletedAt.IsZero()) {
		return fmt.Errorf("%w: invalid persisted end intent", ErrInvalidDependency)
	}
	return nil
}

func (s *Service) finishEnd(
	ctx context.Context,
	session VoiceSession,
	intent EndIntent,
	traceID string,
) (VoiceSession, error) {
	if intent.Completed() {
		if session.Status == StatusEnded || session.Status == StatusFailed {
			return session, nil
		}
		return s.readTerminalSession(ctx, session, "replay completed end intent")
	}

	switch session.Status {
	case StatusEnded, StatusFailed:
		return session, s.completeEndIntent(ctx, session)
	case StatusCreated:
		return s.endCreated(ctx, session, intent)
	case StatusActive:
		return s.stopAndEndActive(ctx, session, intent, traceID)
	default:
		return VoiceSession{}, ErrSessionStateConflict
	}
}

func (s *Service) endCreated(
	ctx context.Context,
	session VoiceSession,
	intent EndIntent,
) (VoiceSession, error) {
	endedAt, err := s.nowUTC("created session end")
	if err != nil {
		return VoiceSession{}, err
	}
	ended, err := s.transitionToEnded(ctx, session, intent, endedAt)
	if err != nil {
		return VoiceSession{}, err
	}
	return ended, s.completeEndIntent(ctx, ended)
}

func (s *Service) stopAndEndActive(
	ctx context.Context,
	session VoiceSession,
	intent EndIntent,
	traceID string,
) (VoiceSession, error) {
	endedAt, err := s.nowUTC("active session end")
	if err != nil {
		return VoiceSession{}, err
	}
	runtime, err := s.deps.Realtime.Stop(ctx, StopRealtimeCommand{
		SessionID: session.ID,
		TraceID:   traceID,
		Reason:    intent.Reason,
		EndedAt:   endedAt,
	})
	if err != nil {
		return VoiceSession{}, mapEndStopError(ctx, err)
	}
	if err := validateStoppedRuntime(runtime, session.ID); err != nil {
		return VoiceSession{}, err
	}

	ended, err := s.transitionToEnded(ctx, session, intent, endedAt)
	if err != nil {
		return VoiceSession{}, err
	}
	return ended, s.completeEndIntent(ctx, ended)
}

func validateStoppedRuntime(runtime RuntimeSnapshot, sessionID string) error {
	if err := validateRuntimeSnapshot(runtime, sessionID); err != nil {
		return fmt.Errorf("%w: invalid stop snapshot", ErrRealtimeStopFailed)
	}
	if runtime.RuntimeState != RuntimeStopped {
		return fmt.Errorf(
			"%w: cleanup is not confirmed in runtime state %q",
			ErrRealtimeStopFailed,
			runtime.RuntimeState,
		)
	}
	return nil
}

func mapEndStopError(ctx context.Context, err error) error {
	if errors.Is(err, ErrNotImplemented) {
		return ErrNotImplemented
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%w: %w", ErrRealtimeStopFailed, ctxErr)
	}
	return fmt.Errorf("%w: %w", ErrRealtimeStopFailed, err)
}

func (s *Service) transitionToEnded(
	parent context.Context,
	session VoiceSession,
	intent EndIntent,
	endedAt time.Time,
) (VoiceSession, error) {
	ctx, cancel := s.compensationContext(parent)
	defer cancel()

	ended, err := s.deps.Repository.TransitionToEnded(ctx, EndTransitionParams{
		SessionID: session.ID,
		AccountID: session.AccountID,
		Expected:  session.Status,
		EndedAt:   endedAt,
		EndReason: intent.Reason,
	})
	if err == nil {
		return ended, nil
	}
	if !errors.Is(err, ErrConcurrentTransition) {
		return VoiceSession{}, fmt.Errorf("transition voice session to ended: %w", err)
	}

	// Another instance may have committed the same terminal transition. Read
	// with a new budget and accept only an immutable terminal state.
	return s.readTerminalSession(parent, session, "reconcile voice session end")
}

func (s *Service) readTerminalSession(
	parent context.Context,
	session VoiceSession,
	stage string,
) (VoiceSession, error) {
	reconcileCtx, reconcileCancel := s.compensationContext(parent)
	defer reconcileCancel()
	current, readErr := s.deps.Repository.GetOwned(
		reconcileCtx,
		session.AccountID,
		session.ID,
	)
	if readErr != nil {
		return VoiceSession{}, fmt.Errorf("%s: %w", stage, readErr)
	}
	if current.Status != StatusEnded && current.Status != StatusFailed {
		return VoiceSession{}, fmt.Errorf("%s: %w", stage, ErrConcurrentTransition)
	}
	return current, nil
}

func (s *Service) completeEndIntent(
	parent context.Context,
	session VoiceSession,
) error {
	completedAt, err := s.nowUTC("end intent completion")
	if err != nil {
		return err
	}
	ctx, cancel := s.compensationContext(parent)
	defer cancel()
	if err := s.deps.Repository.CompleteEndIntent(
		ctx,
		session.AccountID,
		session.ID,
		completedAt,
	); err != nil {
		return fmt.Errorf("complete voice session end intent: %w", err)
	}
	return nil
}
