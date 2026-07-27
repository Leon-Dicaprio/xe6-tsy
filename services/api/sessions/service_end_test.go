package sessions

import (
	"context"
	"errors"
	"testing"
)

func TestServiceEndCreatedSessionWithoutRealtime(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusCreated)}
	service, realtime := newTestService(t, repository)

	got, err := service.End(context.Background(), validEndInput())
	if err != nil {
		t.Fatalf("End() error = %v", err)
	}
	if got.Status != StatusEnded || got.StartedAt != nil || got.EndedAt == nil {
		t.Fatalf("End() = %#v", got)
	}
	_, stop, _ := realtime.callCounts()
	if stop != 0 || len(repository.endTransitions) != 1 || repository.completeCalls != 1 {
		t.Fatalf("calls = stop %d, transition %d, complete %d; want 0, 1, 1",
			stop, len(repository.endTransitions), repository.completeCalls)
	}
}

func TestServiceEndActiveSessionStopsBeforeTransition(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusActive)}
	service, realtime := newTestService(t, repository)

	got, err := service.End(context.Background(), validEndInput())
	if err != nil {
		t.Fatalf("End() error = %v", err)
	}
	if got.Status != StatusEnded || repository.intent == nil || !repository.intent.Completed() {
		t.Fatalf("End() = %#v, intent = %#v", got, repository.intent)
	}
	_, stop, _ := realtime.callCounts()
	if stop != 1 || len(repository.endTransitions) != 1 {
		t.Fatalf("calls = stop %d, transition %d; want 1, 1", stop, len(repository.endTransitions))
	}
}

func TestServiceEndRetriesIncompleteIntentAfterStopFailure(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusActive)}
	service, realtime := newTestService(t, repository)
	realtime.stopErrors = []error{errDependency, nil}
	realtime.stopSnapshots = []RuntimeSnapshot{
		{},
		{SessionID: "vs_1", RuntimeState: RuntimeStopped, UpdatedAt: repository.session.CreatedAt},
	}

	_, err := service.End(context.Background(), validEndInput())
	if !errors.Is(err, ErrRealtimeStopFailed) {
		t.Fatalf("first End() error = %v, want ErrRealtimeStopFailed", err)
	}
	if repository.session.Status != StatusActive || repository.intent == nil || repository.intent.Completed() {
		t.Fatalf("after failed Stop: session = %#v, intent = %#v", repository.session, repository.intent)
	}

	got, err := service.End(context.Background(), validEndInput())
	if err != nil {
		t.Fatalf("retry End() error = %v", err)
	}
	if got.Status != StatusEnded || !repository.intent.Completed() {
		t.Fatalf("retry End() = %#v, intent = %#v", got, repository.intent)
	}
	_, stop, _ := realtime.callCounts()
	if stop != 2 {
		t.Fatalf("stop calls = %d, want 2", stop)
	}
}

func TestServiceEndRejectsUnconfirmedCleanup(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusActive)}
	service, realtime := newTestService(t, repository)
	realtime.stopSnapshots = []RuntimeSnapshot{{
		SessionID: "vs_1", RuntimeState: RuntimeStopping, UpdatedAt: repository.session.CreatedAt,
	}}

	_, err := service.End(context.Background(), validEndInput())
	if !errors.Is(err, ErrRealtimeStopFailed) {
		t.Fatalf("End() error = %v, want ErrRealtimeStopFailed", err)
	}
	if repository.session.Status != StatusActive || len(repository.endTransitions) != 0 {
		t.Fatalf("session = %#v, transitions = %d; want active, 0",
			repository.session, len(repository.endTransitions))
	}
}

func TestServiceEndPreservesNotImplementedDependency(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusActive)}
	service, realtime := newTestService(t, repository)
	realtime.stopErrors = []error{ErrNotImplemented}

	_, err := service.End(context.Background(), validEndInput())
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("End() error = %v, want ErrNotImplemented", err)
	}
	if repository.session.Status != StatusActive || repository.intent == nil || repository.intent.Completed() {
		t.Fatalf("session = %#v, intent = %#v; want resumable active end", repository.session, repository.intent)
	}
}

func TestServiceEndRetriesTransitionAfterIdempotentStop(t *testing.T) {
	repository := &fakeRepository{
		session:             newVoiceSession(t, StatusActive),
		transitionEndedErrs: []error{errDependency, nil},
	}
	service, realtime := newTestService(t, repository)

	_, err := service.End(context.Background(), validEndInput())
	if !errors.Is(err, errDependency) {
		t.Fatalf("first End() error = %v, want transition error", err)
	}
	if repository.session.Status != StatusActive || repository.intent.Completed() {
		t.Fatalf("after transition failure: session = %#v, intent = %#v", repository.session, repository.intent)
	}

	got, err := service.End(context.Background(), validEndInput())
	if err != nil || got.Status != StatusEnded {
		t.Fatalf("retry End() = %#v, %v", got, err)
	}
	_, stop, _ := realtime.callCounts()
	if stop != 2 {
		t.Fatalf("stop calls = %d, want 2", stop)
	}
}

func TestServiceEndRejectsIdempotencyConflict(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusCreated)}
	service, _ := newTestService(t, repository)
	if _, err := service.End(context.Background(), validEndInput()); err != nil {
		t.Fatalf("first End() error = %v", err)
	}

	conflict := validEndInput()
	conflict.RequestHash = "different"
	_, err := service.End(context.Background(), conflict)
	if !errors.Is(err, ErrIdempotencyKeyConflict) {
		t.Fatalf("conflicting End() error = %v, want ErrIdempotencyKeyConflict", err)
	}
}

func TestServiceResumeEndRequiresPersistedIntent(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusActive)}
	service, _ := newTestService(t, repository)

	_, err := service.ResumeEnd(context.Background(), ResumeEndInput{
		AccountID: "acct_1", SessionID: "vs_1", TraceID: "req_recovery",
	})
	if !errors.Is(err, ErrEndIntentNotFound) {
		t.Fatalf("ResumeEnd() error = %v, want ErrEndIntentNotFound", err)
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
