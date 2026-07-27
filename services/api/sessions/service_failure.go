package sessions

import (
	"context"
	"fmt"
)

// ConsumeRuntimeFailure records an unrecoverable terminal failure only after
// the authoritative realtime snapshot confirms cleanup reached stopped.
func (s *Service) ConsumeRuntimeFailure(ctx context.Context, failure RuntimeFailure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if failure.SessionID == "" ||
		failure.TraceID == "" ||
		failure.ErrorCode == "" ||
		failure.OccurredAt.IsZero() {
		return ErrInvalidRequest
	}

	unlock := s.locks.lock(failure.SessionID)
	defer unlock()

	session, err := s.deps.Repository.Get(ctx, failure.SessionID)
	if err != nil {
		return fmt.Errorf("read voice session for runtime failure: %w", err)
	}
	switch session.Status {
	case StatusEnded, StatusFailed:
		return nil
	case StatusActive:
		// Continue with cleanup confirmation.
	case StatusCreated:
		return ErrSessionStateConflict
	default:
		return ErrSessionStateConflict
	}

	runtime, err := s.deps.Realtime.GetRuntimeState(ctx, failure.SessionID)
	if err != nil {
		return mapDependencyError(ctx, err, ErrRuntimeUnavailable)
	}
	if err := validateRuntimeSnapshot(runtime, failure.SessionID); err != nil {
		return err
	}
	if runtime.RuntimeState != RuntimeStopped {
		return ErrRealtimeStopFailed
	}

	_, err = s.deps.Repository.TransitionToFailed(ctx, FailureTransitionParams{
		SessionID: failure.SessionID,
		AccountID: session.AccountID,
		Expected:  StatusActive,
		FailedAt:  failure.OccurredAt.UTC(),
		ErrorCode: failure.ErrorCode,
	})
	if err != nil {
		return fmt.Errorf("transition voice session to failed: %w", err)
	}
	return nil
}

var (
	_ RuntimeFailureConsumer = (*Service)(nil)
)
