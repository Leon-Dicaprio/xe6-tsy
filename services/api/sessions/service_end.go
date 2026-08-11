package sessions

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// End persists request identity before attempting cleanup. Every failure after
// SaveEndIntent therefore remains recoverable by an idempotent replay.
func (s *Service) End(
	ctx context.Context,
	input EndInput,
) (result VoiceSession, resultErr error) {
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
	requestOwner := "request:" + input.TraceID
	leaseExpiresAt := requestedAt.Add(s.deps.EndRecoveryLeaseDuration)
	leaseStartedAt := time.Now()
	// Persist intent and acquire the request path's lease before any Stop call.
	// From this point onward an interrupted request is recoverable from storage.
	intent, _, err := s.deps.Repository.SaveEndIntent(ctx, EndIntent{
		SessionID:      input.SessionID,
		AccountID:      input.AccountID,
		Reason:         input.Reason,
		IdempotencyKey: input.IdempotencyKey,
		RequestHash:    input.RequestHash,
		TraceID:        input.TraceID,
		RequestedAt:    requestedAt,
		RecoveryOwner:  &requestOwner,
		LeaseExpiresAt: &leaseExpiresAt,
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
	if intent.Completed() {
		return session, nil
	}
	if intent.RecoveryOwner == nil || *intent.RecoveryOwner != requestOwner ||
		intent.LeaseExpiresAt == nil {
		return VoiceSession{}, ErrConcurrentTransition
	}
	leaseRemaining := s.deps.EndRecoveryLeaseDuration - time.Since(leaseStartedAt)
	if leaseRemaining <= 0 {
		return VoiceSession{}, ErrConcurrentTransition
	}
	// Every post-intent failure releases the lease and records an immediately due
	// retry using a cancellation-independent persistence context.
	defer func() {
		if resultErr == nil {
			return
		}
		persistCtx, cancel := s.endPersistenceContext(ctx)
		defer cancel()
		err := s.deps.Repository.RetryClaimedEndIntent(
			persistCtx,
			RetryEndIntentParams{
				SessionID:  intent.SessionID,
				AccountID:  intent.AccountID,
				WorkerID:   requestOwner,
				LastError:  resultErr.Error(),
				RetryAfter: 0,
			},
		)
		if err != nil && !errors.Is(err, ErrConcurrentTransition) {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf("persist failed end request for recovery: %w", err),
			)
		}
	}()

	attemptCtx, cancel := s.endAttemptContext(ctx, leaseRemaining)
	defer cancel()

	switch session.Status {
	case StatusEnded, StatusFailed:
		return session, s.completeEndIntent(attemptCtx, session)
	case StatusCreated:
		return s.endCreated(attemptCtx, session, intent)
	case StatusActive:
		return s.stopAndEndActive(attemptCtx, session, intent, input.TraceID)
	default:
		return VoiceSession{}, ErrSessionStateConflict
	}
}

// validateEndInput rejects malformed request identities before an EndIntent can
// be persisted. Reason is part of the canonical request and cannot change on
// an idempotent replay.
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

// validateEndIntent treats repository output as an integration contract. A
// corrupt or mismatched durable intent must not authorize Stop or a terminal
// business transition.
func validateEndIntent(intent EndIntent, session VoiceSession, reason EndReason) error {
	if intent.SessionID != session.ID ||
		intent.AccountID != session.AccountID ||
		intent.IdempotencyKey == "" ||
		intent.RequestHash == "" ||
		intent.TraceID == "" ||
		intent.RequestedAt.IsZero() ||
		!intent.Reason.Valid() ||
		intent.Reason != reason ||
		(intent.CompletedAt != nil && intent.CompletedAt.IsZero()) {
		return fmt.Errorf("%w: invalid persisted end intent", ErrInvalidDependency)
	}
	return nil
}

// endCreated transitions directly to ended because a created Session has no
// active media resources. The repository Start/End interlock is what makes this
// shortcut safe under concurrent requests.
func (s *Service) endCreated(
	ctx context.Context,
	session VoiceSession,
	intent EndIntent,
) (VoiceSession, error) {
	endedAt, err := s.nowUTC("created session end")
	if err != nil {
		return VoiceSession{}, err
	}
	ended, err := s.transitionCreatedToEnded(ctx, session, intent, endedAt)
	if err != nil {
		return VoiceSession{}, err
	}
	return ended, s.completeEndIntent(ctx, ended)
}

// transitionCreatedToEnded performs the business transition but deliberately
// leaves EndIntent completion to the caller, allowing recovery from a process
// interruption between those two durable writes.
func (s *Service) transitionCreatedToEnded(
	ctx context.Context,
	session VoiceSession,
	intent EndIntent,
	endedAt time.Time,
) (VoiceSession, error) {
	ended, err := s.deps.Repository.TransitionToEnded(ctx, EndTransitionParams{
		SessionID: session.ID,
		AccountID: session.AccountID,
		Expected:  StatusCreated,
		EndedAt:   endedAt,
		EndReason: intent.Reason,
	})
	if err != nil {
		return VoiceSession{}, fmt.Errorf("end created voice session: %w", err)
	}
	return ended, nil
}

// stopAndEndActive runs cleanup, commits the terminal business state, and only
// then completes the intent. Each durable interruption point is replayable.
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
	ended, err := s.stopAndTransitionActive(ctx, session, intent, traceID, endedAt)
	if err != nil {
		return VoiceSession{}, err
	}
	return ended, s.completeEndIntent(ctx, ended)
}

// stopAndTransitionActive never writes ended until Realtime.Stop returns a
// valid stopped snapshot for this Session. RPC success without stopped cleanup
// confirmation is treated as a failed End attempt.
func (s *Service) stopAndTransitionActive(
	ctx context.Context,
	session VoiceSession,
	intent EndIntent,
	traceID string,
	endedAt time.Time,
) (VoiceSession, error) {
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

	ended, err := s.deps.Repository.TransitionToEnded(ctx, EndTransitionParams{
		SessionID: session.ID,
		AccountID: session.AccountID,
		Expected:  StatusActive,
		EndedAt:   endedAt,
		EndReason: intent.Reason,
	})
	if err != nil {
		return VoiceSession{}, fmt.Errorf("transition active voice session to ended: %w", err)
	}
	return ended, nil
}

// validateStoppedRuntime is the cleanup-confirmation gate shared by request and
// recovery paths. Transitional, failed, stale, or foreign snapshots keep the
// prior business status unchanged.
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

// mapEndStopError retains not-implemented for deliberately deferred adapters
// and otherwise wraps all Stop failures in the stable domain boundary.
func mapEndStopError(ctx context.Context, err error) error {
	if errors.Is(err, ErrNotImplemented) {
		return ErrNotImplemented
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%w: %w", ErrRealtimeStopFailed, ctxErr)
	}
	return fmt.Errorf("%w: %w", ErrRealtimeStopFailed, err)
}

// completeEndIntent records that cleanup and the terminal Session transition
// have both committed. It is intentionally separate from TransitionToEnded so
// the recovery worker can finish after a crash between writes.
func (s *Service) completeEndIntent(
	ctx context.Context,
	session VoiceSession,
) error {
	completedAt, err := s.nowUTC("end intent completion")
	if err != nil {
		return err
	}
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
