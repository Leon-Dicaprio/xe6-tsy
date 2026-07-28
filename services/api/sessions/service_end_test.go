package sessions

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type endTraceKey struct{}

type endRepository struct {
	*startRepository

	endMu sync.Mutex

	intent            *EndIntent
	saveErr           error
	getIntentErr      error
	completeErr       error
	transitionErrors  []error
	saveCalls         int
	completeCalls     int
	transitionCalls   int
	transitionHook    func(context.Context)
	completeHook      func(context.Context)
	transitionCtxErr  error
	completeCtxErr    error
	transitionCtxData any
	completeCtxData   any
}

func (r *endRepository) SaveEndIntent(
	_ context.Context,
	intent EndIntent,
) (EndIntent, bool, error) {
	r.endMu.Lock()
	defer r.endMu.Unlock()
	r.saveCalls++
	if r.saveErr != nil {
		return EndIntent{}, false, r.saveErr
	}
	r.mu.Lock()
	var operation StartOperation
	hasOperation := r.operation != nil
	if hasOperation {
		operation = *r.operation
	}
	r.mu.Unlock()
	if hasOperation {
		switch operation.Status {
		case StartOperationPending,
			StartOperationCompensating,
			StartOperationCompensationFailed:
			return EndIntent{}, false, ErrSessionStartInProgress
		}
	}
	if r.intent != nil {
		if !r.intent.MatchesRequest(intent.IdempotencyKey, intent.RequestHash) {
			return EndIntent{}, false, ErrIdempotencyKeyConflict
		}
		return *r.intent, true, nil
	}
	saved := intent
	r.intent = &saved
	return saved, false, nil
}

func (r *endRepository) GetEndIntent(
	_ context.Context,
	accountID string,
	sessionID string,
) (EndIntent, error) {
	r.endMu.Lock()
	defer r.endMu.Unlock()
	if r.getIntentErr != nil {
		return EndIntent{}, r.getIntentErr
	}
	if r.intent == nil ||
		r.intent.AccountID != accountID ||
		r.intent.SessionID != sessionID {
		return EndIntent{}, ErrEndIntentNotFound
	}
	return *r.intent, nil
}

func (r *endRepository) CompleteEndIntent(
	ctx context.Context,
	accountID string,
	sessionID string,
	completedAt time.Time,
) error {
	r.endMu.Lock()
	r.completeCalls++
	hook := r.completeHook
	r.completeCtxErr = ctx.Err()
	r.completeCtxData = ctx.Value(endTraceKey{})
	r.endMu.Unlock()
	if hook != nil {
		hook(ctx)
	}

	r.endMu.Lock()
	defer r.endMu.Unlock()
	if r.completeErr != nil {
		return r.completeErr
	}
	if r.intent == nil ||
		r.intent.AccountID != accountID ||
		r.intent.SessionID != sessionID {
		return ErrEndIntentNotFound
	}
	if r.intent.CompletedAt == nil {
		r.intent.CompletedAt = &completedAt
	}
	return nil
}

func (r *endRepository) TransitionToEnded(
	ctx context.Context,
	params EndTransitionParams,
) (VoiceSession, error) {
	r.endMu.Lock()
	index := r.transitionCalls
	r.transitionCalls++
	hook := r.transitionHook
	r.transitionCtxErr = ctx.Err()
	r.transitionCtxData = ctx.Value(endTraceKey{})
	var transitionErr error
	if len(r.transitionErrors) > 0 {
		transitionErr = r.transitionErrors[min(index, len(r.transitionErrors)-1)]
	}
	r.endMu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	if transitionErr != nil {
		return VoiceSession{}, transitionErr
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.session.ID != params.SessionID || r.session.AccountID != params.AccountID {
		return VoiceSession{}, ErrVoiceSessionNotFound
	}
	if r.session.Status != params.Expected {
		return VoiceSession{}, ErrConcurrentTransition
	}
	r.session.Status = StatusEnded
	r.session.EndedAt = &params.EndedAt
	return r.session, nil
}

type endFixture struct {
	service    *Service
	repository *endRepository
	realtime   *startRealtime
	clock      *fakeClock
}

func newEndFixture(t *testing.T, status Status) *endFixture {
	t.Helper()
	now := time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC)
	session := VoiceSession{
		ID:        "vs_1",
		AccountID: "acct_1",
		Status:    status,
		CreatedAt: now.Add(-time.Hour),
	}
	if status == StatusActive || status == StatusEnded || status == StatusFailed {
		startedAt := now.Add(-30 * time.Minute)
		session.StartedAt = &startedAt
	}
	if status == StatusEnded || status == StatusFailed {
		endedAt := now.Add(-time.Minute)
		session.EndedAt = &endedAt
	}
	repository := &endRepository{startRepository: &startRepository{session: session}}
	realtime := &startRealtime{stopResult: RuntimeSnapshot{
		SessionID: "vs_1", RuntimeState: RuntimeStopped, UpdatedAt: now,
	}}
	clock := &fakeClock{now: now}
	service := newSharedStartService(
		t,
		repository,
		&fakeLanguageConfigReader{},
		&fakeWebRTCConnectionReader{},
		realtime,
		clock,
	)
	return &endFixture{
		service: service, repository: repository, realtime: realtime, clock: clock,
	}
}

func validEndInput() EndInput {
	return EndInput{
		AccountID:      "acct_1",
		SessionID:      "vs_1",
		IdempotencyKey: "end_1",
		RequestHash:    "hash_1",
		TraceID:        "req_1",
		Reason:         EndReasonUserRequested,
	}
}

func TestServiceEndCreatedSessionWithoutRealtime(t *testing.T) {
	fixture := newEndFixture(t, StatusCreated)

	got, err := fixture.service.End(context.Background(), validEndInput())
	if err != nil {
		t.Fatalf("End() error = %v", err)
	}
	if got.Status != StatusEnded || got.StartedAt != nil || got.EndedAt == nil {
		t.Fatalf("End() = %#v", got)
	}
	if fixture.realtime.stopCalls != 0 {
		t.Fatalf("Stop() calls = %d, want 0", fixture.realtime.stopCalls)
	}
	assertCompletedEnd(t, fixture)
}

func TestServiceEndCreatedRejectsUnresolvedStartOperation(t *testing.T) {
	fixture := newEndFixture(t, StatusCreated)
	fixture.repository.operation = &StartOperation{
		ID:             "op_1",
		SessionID:      "vs_1",
		AccountID:      "acct_1",
		IdempotencyKey: "start_1",
		RequestHash:    "start_hash",
		Status:         StartOperationPending,
		CreatedAt:      fixture.clock.now,
		UpdatedAt:      fixture.clock.now,
	}

	_, err := fixture.service.End(context.Background(), validEndInput())
	if !errors.Is(err, ErrSessionStartInProgress) {
		t.Fatalf("End() error = %v, want ErrSessionStartInProgress", err)
	}
	if fixture.realtime.stopCalls != 0 {
		t.Fatalf("Stop() calls = %d, want 0 without cleanup authority", fixture.realtime.stopCalls)
	}
	fixture.repository.mu.Lock()
	status := fixture.repository.session.Status
	fixture.repository.mu.Unlock()
	if status != StatusCreated {
		t.Fatalf("session status = %q, want created", status)
	}
}

func TestServiceEndPreservesExistingTerminalState(t *testing.T) {
	for _, status := range []Status{StatusEnded, StatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			fixture := newEndFixture(t, status)

			got, err := fixture.service.End(context.Background(), validEndInput())
			if err != nil {
				t.Fatalf("End() error = %v", err)
			}
			if got.Status != status {
				t.Fatalf("End() status = %q, want %q", got.Status, status)
			}
			if fixture.realtime.stopCalls != 0 {
				t.Fatalf("Stop() calls = %d, want 0", fixture.realtime.stopCalls)
			}
			assertCompletedEnd(t, fixture)
		})
	}
}

func TestServiceEndRejectsInvalidInputBeforeDependencies(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*EndInput)
		want   error
	}{
		{
			name: "missing account",
			mutate: func(input *EndInput) {
				input.AccountID = ""
			},
			want: ErrUnauthorized,
		},
		{
			name: "missing session",
			mutate: func(input *EndInput) {
				input.SessionID = ""
			},
			want: ErrInvalidRequest,
		},
		{
			name: "missing idempotency key",
			mutate: func(input *EndInput) {
				input.IdempotencyKey = ""
			},
			want: ErrInvalidRequest,
		},
		{
			name: "missing request hash",
			mutate: func(input *EndInput) {
				input.RequestHash = ""
			},
			want: ErrInvalidRequest,
		},
		{
			name: "missing trace",
			mutate: func(input *EndInput) {
				input.TraceID = ""
			},
			want: ErrInvalidRequest,
		},
		{
			name: "invalid reason",
			mutate: func(input *EndInput) {
				input.Reason = "unknown"
			},
			want: ErrInvalidRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newEndFixture(t, StatusActive)
			input := validEndInput()
			test.mutate(&input)

			_, err := fixture.service.End(context.Background(), input)
			if !errors.Is(err, test.want) {
				t.Fatalf("End() error = %v, want %v", err, test.want)
			}
			if fixture.repository.getCalls != 0 || fixture.realtime.stopCalls != 0 {
				t.Fatalf("dependency calls = get %d, Stop %d; want 0, 0",
					fixture.repository.getCalls, fixture.realtime.stopCalls)
			}
		})
	}
}

func TestServiceEndActiveStopsBeforeTerminalCommit(t *testing.T) {
	fixture := newEndFixture(t, StatusActive)
	stopReturned := false
	fixture.realtime.stopHook = func(context.Context) {
		stopReturned = true
	}
	fixture.repository.transitionHook = func(context.Context) {
		if !stopReturned {
			t.Fatal("TransitionToEnded() ran before Stop() returned")
		}
	}

	got, err := fixture.service.End(context.Background(), validEndInput())
	if err != nil {
		t.Fatalf("End() error = %v", err)
	}
	if got.Status != StatusEnded || fixture.realtime.stopCalls != 1 {
		t.Fatalf("End() = %#v, Stop() calls = %d", got, fixture.realtime.stopCalls)
	}
	assertCompletedEnd(t, fixture)
}

func TestServiceEndStopFailureLeavesActiveIntentRecoverable(t *testing.T) {
	fixture := newEndFixture(t, StatusActive)
	fixture.realtime.stopErr = errDependency

	_, err := fixture.service.End(context.Background(), validEndInput())
	if !errors.Is(err, ErrRealtimeStopFailed) || !errors.Is(err, errDependency) {
		t.Fatalf("End() error = %v, want stop and dependency errors", err)
	}
	assertIncompleteActiveEnd(t, fixture)
	assertNoEndTransition(t, fixture)
}

func TestServiceEndStopDeadlineLeavesActiveIntentRecoverable(t *testing.T) {
	fixture := newEndFixture(t, StatusActive)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	fixture.realtime.stopHook = func(ctx context.Context) {
		<-ctx.Done()
		fixture.realtime.mu.Lock()
		fixture.realtime.stopErr = ctx.Err()
		fixture.realtime.mu.Unlock()
	}

	_, err := fixture.service.End(ctx, validEndInput())
	if !errors.Is(err, ErrRealtimeStopFailed) ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("End() error = %v, want stop and deadline errors", err)
	}
	assertIncompleteActiveEnd(t, fixture)
	assertNoEndTransition(t, fixture)
}

func TestServiceEndRejectsUnconfirmedCleanupStates(t *testing.T) {
	tests := []struct {
		name     string
		snapshot RuntimeSnapshot
	}{
		{
			name: "stopping",
			snapshot: RuntimeSnapshot{
				SessionID: "vs_1", RuntimeState: RuntimeStopping,
				UpdatedAt: time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "failed",
			snapshot: RuntimeSnapshot{
				SessionID: "vs_1", RuntimeState: RuntimeFailed,
				UpdatedAt: time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "wrong session",
			snapshot: RuntimeSnapshot{
				SessionID: "vs_other", RuntimeState: RuntimeStopped,
				UpdatedAt: time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC),
			},
		},
		{
			name: "missing timestamp",
			snapshot: RuntimeSnapshot{
				SessionID: "vs_1", RuntimeState: RuntimeStopped,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newEndFixture(t, StatusActive)
			fixture.realtime.stopResult = test.snapshot

			_, err := fixture.service.End(context.Background(), validEndInput())
			if !errors.Is(err, ErrRealtimeStopFailed) {
				t.Fatalf("End() error = %v, want ErrRealtimeStopFailed", err)
			}
			assertIncompleteActiveEnd(t, fixture)
			assertNoEndTransition(t, fixture)
		})
	}
}

func TestServiceEndRetriesTransitionAfterIdempotentStop(t *testing.T) {
	fixture := newEndFixture(t, StatusActive)
	fixture.repository.transitionErrors = []error{errDependency, nil}

	_, err := fixture.service.End(context.Background(), validEndInput())
	if !errors.Is(err, errDependency) {
		t.Fatalf("first End() error = %v, want transition error", err)
	}
	assertIncompleteActiveEnd(t, fixture)

	got, err := fixture.service.End(context.Background(), validEndInput())
	if err != nil || got.Status != StatusEnded {
		t.Fatalf("retry End() = %#v, %v", got, err)
	}
	if fixture.realtime.stopCalls != 2 {
		t.Fatalf("Stop() calls = %d, want 2", fixture.realtime.stopCalls)
	}
	assertCompletedEnd(t, fixture)
}

func TestServiceEndReplaysTerminalResultAfterIntentCompletionFailure(t *testing.T) {
	fixture := newEndFixture(t, StatusActive)
	fixture.repository.completeErr = errDependency

	first, err := fixture.service.End(context.Background(), validEndInput())
	if !errors.Is(err, errDependency) || first.Status != StatusEnded {
		t.Fatalf("first End() = %#v, %v", first, err)
	}
	if fixture.repository.intent == nil || fixture.repository.intent.Completed() {
		t.Fatalf("intent = %#v, want incomplete", fixture.repository.intent)
	}
	fixture.repository.completeErr = nil

	replayed, err := fixture.service.End(context.Background(), validEndInput())
	if err != nil || replayed.Status != StatusEnded {
		t.Fatalf("replayed End() = %#v, %v", replayed, err)
	}
	if fixture.realtime.stopCalls != 1 {
		t.Fatalf("Stop() calls = %d, want 1", fixture.realtime.stopCalls)
	}
	assertCompletedEnd(t, fixture)
}

func TestServiceEndCompletedReplayRefreshesStaleSessionRead(t *testing.T) {
	fixture := newEndFixture(t, StatusActive)
	input := validEndInput()
	requestedAt := fixture.clock.now.Add(-time.Minute)
	fixture.repository.intent = &EndIntent{
		SessionID:      input.SessionID,
		AccountID:      input.AccountID,
		Reason:         input.Reason,
		IdempotencyKey: input.IdempotencyKey,
		RequestHash:    input.RequestHash,
		RequestedAt:    requestedAt,
	}
	var once sync.Once
	fixture.repository.getHook = func(context.Context) {
		once.Do(func() {
			fixture.repository.mu.Lock()
			endedAt := fixture.clock.now
			fixture.repository.session.Status = StatusEnded
			fixture.repository.session.EndedAt = &endedAt
			fixture.repository.mu.Unlock()
			fixture.repository.endMu.Lock()
			fixture.repository.intent.CompletedAt = &endedAt
			fixture.repository.endMu.Unlock()
		})
	}

	got, err := fixture.service.End(context.Background(), input)
	if err != nil || got.Status != StatusEnded {
		t.Fatalf("End() = %#v, %v", got, err)
	}
	if fixture.realtime.stopCalls != 0 {
		t.Fatalf("Stop() calls = %d, want 0 for completed replay", fixture.realtime.stopCalls)
	}
}

func TestServiceEndRejectsIdempotencyConflict(t *testing.T) {
	fixture := newEndFixture(t, StatusCreated)
	if _, err := fixture.service.End(context.Background(), validEndInput()); err != nil {
		t.Fatalf("first End() error = %v", err)
	}

	conflict := validEndInput()
	conflict.RequestHash = "different"
	_, err := fixture.service.End(context.Background(), conflict)
	if !errors.Is(err, ErrIdempotencyKeyConflict) {
		t.Fatalf("conflicting End() error = %v", err)
	}
}

func TestServiceResumeEndRequiresPersistedIntent(t *testing.T) {
	fixture := newEndFixture(t, StatusActive)

	_, err := fixture.service.ResumeEnd(context.Background(), ResumeEndInput{
		AccountID: "acct_1", SessionID: "vs_1", TraceID: "req_recovery",
	})
	if !errors.Is(err, ErrEndIntentNotFound) {
		t.Fatalf("ResumeEnd() error = %v, want ErrEndIntentNotFound", err)
	}
}

func TestServiceResumeEndUsesPersistedReason(t *testing.T) {
	fixture := newEndFixture(t, StatusActive)
	fixture.realtime.stopErr = errDependency
	input := validEndInput()
	input.Reason = EndReasonClientDisconnected
	if _, err := fixture.service.End(context.Background(), input); err == nil {
		t.Fatal("first End() error = nil, want Stop failure")
	}
	fixture.realtime.stopErr = nil

	got, err := fixture.service.ResumeEnd(context.Background(), ResumeEndInput{
		AccountID: "acct_1", SessionID: "vs_1", TraceID: "req_recovery",
	})
	if err != nil || got.Status != StatusEnded {
		t.Fatalf("ResumeEnd() = %#v, %v", got, err)
	}
	if fixture.realtime.stopCommand.Reason != EndReasonClientDisconnected {
		t.Fatalf("Stop() reason = %q", fixture.realtime.stopCommand.Reason)
	}
}

func TestServiceEndFinalizesWithFreshContextAfterRequestCancellation(t *testing.T) {
	fixture := newEndFixture(t, StatusActive)
	parent, cancel := context.WithCancel(
		context.WithValue(context.Background(), endTraceKey{}, "trace-value"),
	)
	fixture.realtime.stopHook = func(context.Context) {
		cancel()
	}

	got, err := fixture.service.End(parent, validEndInput())
	if err != nil || got.Status != StatusEnded {
		t.Fatalf("End() = %#v, %v", got, err)
	}
	if fixture.repository.transitionCtxErr != nil ||
		fixture.repository.completeCtxErr != nil {
		t.Fatalf("fresh context errors = transition %v, complete %v",
			fixture.repository.transitionCtxErr, fixture.repository.completeCtxErr)
	}
	if fixture.repository.transitionCtxData != "trace-value" ||
		fixture.repository.completeCtxData != "trace-value" {
		t.Fatalf("trace values = transition %#v, complete %#v",
			fixture.repository.transitionCtxData, fixture.repository.completeCtxData)
	}
}

func TestServiceEndDifferentSessionsProceedInParallel(t *testing.T) {
	now := time.Date(2026, 7, 28, 8, 0, 0, 0, time.UTC)
	repository := &multiEndRepository{
		sessions: map[string]VoiceSession{
			"vs_1": {ID: "vs_1", AccountID: "acct_1", Status: StatusActive, CreatedAt: now},
			"vs_2": {ID: "vs_2", AccountID: "acct_1", Status: StatusActive, CreatedAt: now},
		},
		intents: make(map[string]EndIntent),
	}
	realtime := &parallelEndRealtime{
		now:     now,
		entered: make(chan string, 2),
		release: make(chan struct{}),
	}
	service := newSharedStartService(
		t,
		repository,
		&fakeLanguageConfigReader{},
		&fakeWebRTCConnectionReader{},
		realtime,
		&fakeClock{now: now},
	)

	results := make(chan error, 2)
	for _, sessionID := range []string{"vs_1", "vs_2"} {
		go func(sessionID string) {
			input := validEndInput()
			input.SessionID = sessionID
			input.IdempotencyKey = "end_" + sessionID
			results <- func() error {
				_, err := service.End(context.Background(), input)
				return err
			}()
		}(sessionID)
	}
	first := <-realtime.entered
	second := <-realtime.entered
	if first == second {
		t.Fatalf("parallel Stop sessions = %q, %q", first, second)
	}
	close(realtime.release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("End() error = %v", err)
		}
	}
}

func TestServiceEndCrossInstanceReplayConverges(t *testing.T) {
	fixture := newEndFixture(t, StatusActive)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	fixture.realtime.stopHook = func(context.Context) {
		entered <- struct{}{}
		<-release
	}
	second := newSharedStartService(
		t,
		fixture.repository,
		&fakeLanguageConfigReader{},
		&fakeWebRTCConnectionReader{},
		fixture.realtime,
		fixture.clock,
	)

	results := make(chan error, 2)
	for _, service := range []*Service{fixture.service, second} {
		go func(service *Service) {
			_, err := service.End(context.Background(), validEndInput())
			results <- err
		}(service)
	}
	<-entered
	<-entered
	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("End() error = %v", err)
		}
	}
	if fixture.realtime.stopCalls != 2 {
		t.Fatalf("Stop() calls = %d, want 2 idempotent calls", fixture.realtime.stopCalls)
	}
	assertCompletedEnd(t, fixture)
}

func assertIncompleteActiveEnd(t *testing.T, fixture *endFixture) {
	t.Helper()
	fixture.repository.mu.Lock()
	status := fixture.repository.session.Status
	endedAt := fixture.repository.session.EndedAt
	fixture.repository.mu.Unlock()
	fixture.repository.endMu.Lock()
	intent := fixture.repository.intent
	fixture.repository.endMu.Unlock()
	if status != StatusActive || endedAt != nil || intent == nil || intent.Completed() {
		t.Fatalf("session status = %q, ended_at = %v, intent = %#v",
			status, endedAt, intent)
	}
}

func assertNoEndTransition(t *testing.T, fixture *endFixture) {
	t.Helper()
	fixture.repository.endMu.Lock()
	defer fixture.repository.endMu.Unlock()
	transitions := fixture.repository.transitionCalls
	if transitions != 0 {
		t.Fatalf("TransitionToEnded() calls = %d, want 0", transitions)
	}
}

func assertCompletedEnd(t *testing.T, fixture *endFixture) {
	t.Helper()
	fixture.repository.endMu.Lock()
	defer fixture.repository.endMu.Unlock()
	if fixture.repository.intent == nil || !fixture.repository.intent.Completed() {
		t.Fatalf("intent = %#v, want completed", fixture.repository.intent)
	}
}

type multiEndRepository struct {
	mu       sync.Mutex
	sessions map[string]VoiceSession
	intents  map[string]EndIntent
}

func (*multiEndRepository) Create(context.Context, CreateParams) (VoiceSession, bool, error) {
	return VoiceSession{}, false, ErrNotImplemented
}

func (r *multiEndRepository) GetOwned(
	_ context.Context,
	accountID string,
	sessionID string,
) (VoiceSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	session, ok := r.sessions[sessionID]
	if !ok || session.AccountID != accountID {
		return VoiceSession{}, ErrVoiceSessionNotFound
	}
	return session, nil
}

func (*multiEndRepository) List(context.Context, ListFilter) (ListPage, error) {
	return ListPage{}, ErrNotImplemented
}

func (*multiEndRepository) GetStartOperation(
	context.Context,
	string,
	string,
	string,
) (StartOperation, error) {
	return StartOperation{}, ErrStartOperationNotFound
}

func (*multiEndRepository) BeginStartOperation(
	context.Context,
	BeginStartOperationParams,
) (BeginStartOperationResult, error) {
	return BeginStartOperationResult{}, ErrNotImplemented
}

func (*multiEndRepository) ClaimStartCompensation(
	context.Context,
	ClaimStartCompensationParams,
) (ClaimStartCompensationResult, error) {
	return ClaimStartCompensationResult{}, ErrNotImplemented
}

func (*multiEndRepository) CompleteStartCompensation(
	context.Context,
	CompleteStartCompensationParams,
) error {
	return ErrNotImplemented
}

func (*multiEndRepository) FailStartCompensation(
	context.Context,
	FailStartCompensationParams,
) error {
	return ErrNotImplemented
}

func (r *multiEndRepository) SaveEndIntent(
	_ context.Context,
	intent EndIntent,
) (EndIntent, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if saved, ok := r.intents[intent.SessionID]; ok {
		if !saved.MatchesRequest(intent.IdempotencyKey, intent.RequestHash) {
			return EndIntent{}, false, ErrIdempotencyKeyConflict
		}
		return saved, true, nil
	}
	r.intents[intent.SessionID] = intent
	return intent, false, nil
}

func (r *multiEndRepository) GetEndIntent(
	_ context.Context,
	accountID string,
	sessionID string,
) (EndIntent, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	intent, ok := r.intents[sessionID]
	if !ok || intent.AccountID != accountID {
		return EndIntent{}, ErrEndIntentNotFound
	}
	return intent, nil
}

func (r *multiEndRepository) CompleteEndIntent(
	_ context.Context,
	accountID string,
	sessionID string,
	completedAt time.Time,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	intent, ok := r.intents[sessionID]
	if !ok || intent.AccountID != accountID {
		return ErrEndIntentNotFound
	}
	if intent.CompletedAt == nil {
		intent.CompletedAt = &completedAt
		r.intents[sessionID] = intent
	}
	return nil
}

func (*multiEndRepository) TransitionToActive(
	context.Context,
	StartTransitionParams,
) (VoiceSession, bool, error) {
	return VoiceSession{}, false, ErrNotImplemented
}

func (r *multiEndRepository) TransitionToEnded(
	_ context.Context,
	params EndTransitionParams,
) (VoiceSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	session, ok := r.sessions[params.SessionID]
	if !ok || session.AccountID != params.AccountID {
		return VoiceSession{}, ErrVoiceSessionNotFound
	}
	if session.Status != params.Expected {
		return VoiceSession{}, ErrConcurrentTransition
	}
	session.Status = StatusEnded
	session.EndedAt = &params.EndedAt
	r.sessions[params.SessionID] = session
	return session, nil
}

func (*multiEndRepository) TransitionToFailed(
	context.Context,
	FailureTransitionParams,
) (VoiceSession, error) {
	return VoiceSession{}, ErrNotImplemented
}

type parallelEndRealtime struct {
	mu      sync.Mutex
	now     time.Time
	entered chan string
	release chan struct{}
}

func (*parallelEndRealtime) Start(
	context.Context,
	StartRealtimeCommand,
) (RuntimeSnapshot, error) {
	return RuntimeSnapshot{}, ErrNotImplemented
}

func (r *parallelEndRealtime) Stop(
	_ context.Context,
	command StopRealtimeCommand,
) (RuntimeSnapshot, error) {
	r.entered <- command.SessionID
	<-r.release
	return RuntimeSnapshot{
		SessionID: command.SessionID, RuntimeState: RuntimeStopped, UpdatedAt: r.now,
	}, nil
}

func (*parallelEndRealtime) GetRuntimeState(
	context.Context,
	string,
) (RuntimeSnapshot, error) {
	return RuntimeSnapshot{}, ErrNotImplemented
}
