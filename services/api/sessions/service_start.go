package sessions

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// Start coordinates one durable operation across the control-plane and
// realtime boundaries. The repository operation, not the in-process lock, is
// the authority for cross-instance activation and compensation ownership.
func (s *Service) Start(ctx context.Context, input StartInput) (VoiceSession, error) {
	if err := validateStartInput(ctx, &input); err != nil {
		return VoiceSession{}, err
	}

	unlock, err := s.locks.lock(ctx, input.SessionID)
	if err != nil {
		return VoiceSession{}, err
	}
	defer unlock()

	session, err := s.deps.Repository.GetOwned(ctx, input.AccountID, input.SessionID)
	if err != nil {
		return VoiceSession{}, fmt.Errorf("read voice session for start: %w", err)
	}
	switch session.Status {
	case StatusActive:
		return s.replayCompletedStart(ctx, input, session)
	case StatusCreated:
		return s.startCreatedSession(ctx, input, session)
	default:
		return VoiceSession{}, ErrSessionStateConflict
	}
}

// startCreatedSession either resumes durable work or validates prerequisites and
// creates a new pending operation before crossing into realtime.
func (s *Service) startCreatedSession(
	ctx context.Context,
	input StartInput,
	session VoiceSession,
) (VoiceSession, error) {
	if session.AccountID == "" {
		return VoiceSession{}, fmt.Errorf(
			"%w: repository returned session without owner",
			ErrInvalidDependency,
		)
	}
	// Recovery is checked before current readiness. A durable compensating
	// operation must finish cleanup even if Language or WebRTC has since become
	// unavailable; applying new-start prerequisites first could strand it.
	operation, found, err := s.findStartOperation(
		ctx, input, session.AccountID,
	)
	if err != nil {
		return VoiceSession{}, err
	}
	if found {
		return s.continueExistingStartOperation(ctx, input, session, operation)
	}
	if err := s.validateStartReadiness(ctx, input, session); err != nil {
		return VoiceSession{}, err
	}
	operation, err = s.beginStartOperation(ctx, input, session.AccountID)
	if err != nil {
		return VoiceSession{}, err
	}
	return s.continueStartOperation(ctx, input, operation)
}

// findStartOperation resolves the actor through Repository authorization but
// verifies the returned operation against the Session's immutable owner. It
// also distinguishes "no matching operation" from another unresolved Start
// that already owns this Session.
func (s *Service) findStartOperation(
	ctx context.Context,
	input StartInput,
	ownerAccountID string,
) (StartOperation, bool, error) {
	if ownerAccountID == "" {
		return StartOperation{}, false, fmt.Errorf(
			"%w: session owner is required",
			ErrInvalidDependency,
		)
	}
	operation, err := s.deps.Repository.GetStartOperation(
		ctx,
		input.AccountID,
		input.SessionID,
		input.IdempotencyKey,
	)
	if errors.Is(err, ErrStartOperationNotFound) {
		return StartOperation{}, false, nil
	}
	if err != nil {
		return StartOperation{}, false, fmt.Errorf("read voice session start operation: %w", err)
	}
	if operation.ID == "" ||
		operation.SessionID != input.SessionID ||
		operation.AccountID != ownerAccountID ||
		operation.IdempotencyKey != input.IdempotencyKey ||
		!operation.Status.Valid() {
		return StartOperation{}, false, fmt.Errorf(
			"%w: invalid start operation returned by repository",
			ErrConcurrentTransition,
		)
	}
	if operation.RequestHash != input.RequestHash {
		return StartOperation{}, false, ErrIdempotencyKeyConflict
	}
	return operation, true, nil
}

// continueExistingStartOperation resumes from durable state instead of
// restarting the workflow. Pending work still needs current readiness;
// compensating work must resume with its persisted claim regardless of it.
func (s *Service) continueExistingStartOperation(
	ctx context.Context,
	input StartInput,
	session VoiceSession,
	operation StartOperation,
) (VoiceSession, error) {
	switch operation.Status {
	case StartOperationPending:
		if err := s.validateStartReadiness(ctx, input, session); err != nil {
			return VoiceSession{}, err
		}
		return s.startPendingOperation(ctx, input, operation)
	case StartOperationCompensating:
		return s.resumeStartCompensation(ctx, input, operation)
	default:
		return s.continueStartOperation(ctx, input, operation)
	}
}

// validateStartInput checks stable request identity before locking or invoking
// dependencies. StartedBy defaults to the authenticated actor for auditability,
// but ownership remains determined by Repository.GetOwned.
func validateStartInput(ctx context.Context, input *StartInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateIdentity(input.AccountID, input.SessionID); err != nil {
		return err
	}
	if err := validateIdempotency(input.IdempotencyKey, input.RequestHash); err != nil {
		return err
	}
	if input.TraceID == "" {
		return ErrInvalidRequest
	}
	if input.StartedBy == "" {
		input.StartedBy = input.AccountID
	}
	return nil
}

// validateStartReadiness checks three independent prerequisites: persisted
// terminal capabilities, an active two-direction language config, and a live
// connected WebRTC transport. None of these checks mutates durable state.
func (s *Service) validateStartReadiness(
	ctx context.Context,
	input StartInput,
	session VoiceSession,
) error {
	if err := decodeSessionReadiness(session); err != nil {
		return err
	}
	languageConfig, err := s.deps.LanguageConfigs.GetCurrentConfig(ctx, input.SessionID)
	if err != nil {
		return mapDependencyError(ctx, err, ErrLanguageConfigNotReady)
	}
	if languageConfig.SessionID != input.SessionID || !languageConfig.Ready() {
		return ErrLanguageConfigNotReady
	}

	connection, err := s.deps.WebRTCConnections.GetConnectionState(ctx, input.SessionID)
	if err != nil {
		return mapDependencyError(ctx, err, ErrWebRTCUnavailable)
	}
	if connection.SessionID != input.SessionID || !connection.ConnectionState.Valid() {
		return ErrWebRTCUnavailable
	}
	if !connection.ConnectionState.Ready() {
		return ErrWebRTCNotReady
	}
	return nil
}

// beginStartOperation persists the OperationID before Realtime.Start. The
// repository may return an existing operation for an idempotent replay, so the
// result is revalidated against Session, owner, key, hash, and known status.
func (s *Service) beginStartOperation(
	ctx context.Context,
	input StartInput,
	ownerAccountID string,
) (StartOperation, error) {
	if ownerAccountID == "" {
		return StartOperation{}, fmt.Errorf(
			"%w: session owner is required",
			ErrInvalidDependency,
		)
	}
	operationID := s.deps.IDs.NewStartOperationID()
	if operationID == "" {
		return StartOperation{}, fmt.Errorf(
			"%w: ID generator returned an empty start operation ID",
			ErrInvalidDependency,
		)
	}
	now, err := s.nowUTC("begin start operation")
	if err != nil {
		return StartOperation{}, err
	}
	begin, err := s.deps.Repository.BeginStartOperation(ctx, BeginStartOperationParams{
		OperationID:    operationID,
		SessionID:      input.SessionID,
		AccountID:      input.AccountID,
		IdempotencyKey: input.IdempotencyKey,
		RequestHash:    input.RequestHash,
		CreatedAt:      now,
	})
	if err != nil {
		return StartOperation{}, fmt.Errorf("begin voice session start operation: %w", err)
	}
	operation := begin.Operation
	if operation.ID == "" ||
		operation.SessionID != input.SessionID ||
		operation.AccountID != ownerAccountID ||
		!operation.MatchesRequest(input.IdempotencyKey, input.RequestHash) ||
		!operation.Status.Valid() {
		return StartOperation{}, fmt.Errorf(
			"%w: invalid start operation returned by repository",
			ErrConcurrentTransition,
		)
	}
	return operation, nil
}

// replayCompletedStart proves that an already-active Session was activated by
// the same durable request. Merely seeing StatusActive is insufficient because
// a different idempotency key must not claim another request's result.
func (s *Service) replayCompletedStart(
	ctx context.Context,
	input StartInput,
	current VoiceSession,
) (VoiceSession, error) {
	operation, err := s.beginStartOperation(ctx, input, current.AccountID)
	if err != nil {
		return VoiceSession{}, err
	}
	if operation.Status != StartOperationCompleted {
		return VoiceSession{}, ErrConcurrentTransition
	}
	return current, nil
}

// continueStartOperation dispatches solely from the repository-owned operation
// state. Terminal compensation states intentionally do not call realtime.
func (s *Service) continueStartOperation(
	ctx context.Context,
	input StartInput,
	operation StartOperation,
) (VoiceSession, error) {
	switch operation.Status {
	case StartOperationPending:
		return s.startPendingOperation(ctx, input, operation)
	case StartOperationCompensating:
		return s.resumeStartCompensation(ctx, input, operation)
	case StartOperationCompleted:
		session, err := s.deps.Repository.GetOwned(ctx, input.AccountID, input.SessionID)
		if err != nil {
			return VoiceSession{}, fmt.Errorf("read completed voice session start: %w", err)
		}
		if session.Status != StatusActive {
			return VoiceSession{}, ErrConcurrentTransition
		}
		return session, nil
	case StartOperationCompensated:
		return VoiceSession{}, ErrIdempotencyKeyConflict
	case StartOperationCompensationFailed:
		return VoiceSession{}, ErrSessionStartInProgress
	default:
		return VoiceSession{}, ErrConcurrentTransition
	}
}

// resumeStartCompensation reuses the persisted ClaimID. Generating a new claim
// would violate the cleanup ownership fence after a process restart.
func (s *Service) resumeStartCompensation(
	ctx context.Context,
	input StartInput,
	operation StartOperation,
) (VoiceSession, error) {
	if operation.CompensationClaimID == nil || *operation.CompensationClaimID == "" {
		return VoiceSession{}, ErrConcurrentTransition
	}
	return s.compensateStartedOperation(
		ctx,
		input,
		operation,
		*operation.CompensationClaimID,
		ErrRealtimeStartFailed,
	)
}

// validateCompensatedRuntime requires positive cleanup confirmation, not merely
// a successful Stop RPC. A mismatched, stale, or non-stopped snapshot leaves
// compensation failed and recoverable.
func validateCompensatedRuntime(runtime RuntimeSnapshot, sessionID string) error {
	if err := validateRuntimeSnapshot(runtime, sessionID); err != nil {
		return fmt.Errorf("%w: invalid compensation snapshot", ErrRealtimeStopFailed)
	}
	if runtime.RuntimeState != RuntimeStopped {
		return ErrRealtimeStopFailed
	}
	return nil
}

// mapRealtimeStopError preserves both the stable session-domain boundary and
// the underlying cancellation, timeout, unsupported-operation, or provider
// cause. Callers can therefore classify every failed Stop consistently
// without losing the detail needed for retry and recovery decisions.
func mapRealtimeStopError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	mapped := mapDependencyError(ctx, err, ErrRealtimeStopFailed)
	if !errors.Is(mapped, ErrRealtimeStopFailed) {
		mapped = errors.Join(ErrRealtimeStopFailed, mapped)
	}
	if !errors.Is(mapped, err) {
		mapped = errors.Join(mapped, err)
	}
	return mapped
}

// compensationPersistenceContext gives the terminal repository write a fresh
// bounded lifetime after realtime Stop returns. It preserves request values
// but neither inherits client cancellation nor a deadline exhausted by Stop.
func (s *Service) compensationPersistenceContext(
	parent context.Context,
) (context.Context, context.CancelFunc) {
	return context.WithTimeout(
		context.WithoutCancel(parent),
		s.deps.CompensationTimeout,
	)
}

// compensateStartedOperation stops realtime only after the repository grants
// this operation and ClaimID exclusive cleanup authority. A denied or
// uncertain claim is a hard prohibition on Stop.
func (s *Service) compensateStartedOperation(
	parent context.Context,
	input StartInput,
	operation StartOperation,
	claimID string,
	originalErr error,
) (VoiceSession, error) {
	compensationAt, timeErr := s.nowUTC("claim start compensation")
	if timeErr != nil {
		return VoiceSession{}, errors.Join(originalErr, timeErr)
	}

	// Claim is the destructive-action authorization boundary. A successful
	// Realtime.Start or matching SessionID alone never grants permission to Stop.
	claimCtx, claimCancel := s.compensationContext(parent)
	claim, claimErr := s.deps.Repository.ClaimStartCompensation(
		claimCtx,
		ClaimStartCompensationParams{
			SessionID:   input.SessionID,
			AccountID:   input.AccountID,
			OperationID: operation.ID,
			ClaimID:     claimID,
			ClaimedAt:   compensationAt,
		},
	)
	claimCancel()
	if claimErr != nil {
		return VoiceSession{}, errors.Join(
			originalErr,
			fmt.Errorf("claim realtime start compensation: %w", claimErr),
		)
	}
	if !claim.Claimed {
		resolveCtx, resolveCancel := s.compensationContext(parent)
		defer resolveCancel()
		return s.resolveDeniedStartCompensation(resolveCtx, input, originalErr)
	}

	// Stop receives a fresh budget after Claim so a slow repository cannot spend
	// the media cleanup timeout before cleanup begins.
	stopCtx, stopCancel := s.compensationContext(parent)
	runtime, stopErr := s.deps.Realtime.Stop(stopCtx, StopRealtimeCommand{
		SessionID: input.SessionID,
		TraceID:   input.TraceID,
		Reason:    EndReasonOperatorCancelled,
		EndedAt:   compensationAt,
	})
	if stopErr != nil {
		stopErr = mapRealtimeStopError(stopCtx, stopErr)
	} else {
		stopErr = validateCompensatedRuntime(runtime, input.SessionID)
	}
	stopCancel()

	// Terminal persistence gets another independent budget. Even a timed-out Stop
	// must record compensation_failed when possible so recovery does not depend
	// on logs or the lifetime of this process.
	persistCtx, persistCancel := s.compensationPersistenceContext(parent)
	defer persistCancel()
	if stopErr == nil {
		return s.completeStartCompensation(
			persistCtx,
			input,
			operation,
			claimID,
			originalErr,
		)
	}
	return s.failStartCompensation(
		persistCtx,
		input,
		operation,
		claimID,
		originalErr,
		stopErr,
	)
}

// completeStartCompensation records confirmed cleanup as compensated while
// returning the original activation error to the caller. The failed Start does
// not become a successful request merely because rollback succeeded.
func (s *Service) completeStartCompensation(
	ctx context.Context,
	input StartInput,
	operation StartOperation,
	claimID string,
	originalErr error,
) (VoiceSession, error) {
	completedAt, timeErr := s.nowUTC("complete start compensation")
	if timeErr != nil {
		return VoiceSession{}, errors.Join(originalErr, timeErr)
	}
	err := s.deps.Repository.CompleteStartCompensation(
		ctx,
		CompleteStartCompensationParams{
			SessionID:   input.SessionID,
			AccountID:   input.AccountID,
			OperationID: operation.ID,
			ClaimID:     claimID,
			CompletedAt: completedAt,
		},
	)
	if err != nil {
		return VoiceSession{}, errors.Join(
			originalErr,
			fmt.Errorf("complete realtime start compensation: %w", err),
		)
	}
	s.deps.Logger.WarnContext(ctx, "compensated realtime start after activation failure",
		slog.String("request_id", input.TraceID),
		slog.String("session_id", input.SessionID),
		slog.String("operation_id", operation.ID),
		slog.Any("original_error", originalErr),
	)
	return VoiceSession{}, originalErr
}

// failStartCompensation persists uncertain cleanup and joins the activation,
// Stop, and persistence causes so callers can classify both the stable session
// error and the underlying failure.
func (s *Service) failStartCompensation(
	ctx context.Context,
	input StartInput,
	operation StartOperation,
	claimID string,
	originalErr error,
	stopErr error,
) (VoiceSession, error) {
	failedAt, timeErr := s.nowUTC("fail start compensation")
	if timeErr != nil {
		return VoiceSession{}, errors.Join(
			originalErr,
			fmt.Errorf("compensate realtime start: %w", stopErr),
			timeErr,
		)
	}
	persistErr := s.deps.Repository.FailStartCompensation(
		ctx,
		FailStartCompensationParams{
			SessionID:   input.SessionID,
			AccountID:   input.AccountID,
			OperationID: operation.ID,
			ClaimID:     claimID,
			FailedAt:    failedAt,
		},
	)
	s.deps.Logger.ErrorContext(ctx, "failed to compensate realtime start",
		slog.String("request_id", input.TraceID),
		slog.String("session_id", input.SessionID),
		slog.String("operation_id", operation.ID),
		slog.Any("original_error", originalErr),
		slog.Any("compensation_error", stopErr),
		slog.Any("persistence_error", persistErr),
	)
	if persistErr != nil {
		return VoiceSession{}, errors.Join(
			originalErr,
			fmt.Errorf("compensate realtime start: %w", stopErr),
			fmt.Errorf("persist failed realtime start compensation: %w", persistErr),
		)
	}
	return VoiceSession{}, errors.Join(
		originalErr,
		fmt.Errorf("compensate realtime start: %w", stopErr),
	)
}

// resolveDeniedStartCompensation handles a lost race without destructive work.
// If another instance committed activation, it replays that result; otherwise
// it reports in-progress and leaves the current owner responsible for cleanup.
func (s *Service) resolveDeniedStartCompensation(
	ctx context.Context,
	input StartInput,
	originalErr error,
) (VoiceSession, error) {
	session, err := s.deps.Repository.GetOwned(ctx, input.AccountID, input.SessionID)
	if err != nil {
		return VoiceSession{}, errors.Join(
			originalErr,
			fmt.Errorf("read voice session after denied compensation claim: %w", err),
		)
	}
	if session.Status == StatusActive {
		replayed, replayErr := s.replayCompletedStart(ctx, input, session)
		if replayErr != nil {
			return VoiceSession{}, errors.Join(originalErr, replayErr)
		}
		return replayed, nil
	}
	return VoiceSession{}, errors.Join(originalErr, ErrSessionStartInProgress)
}
