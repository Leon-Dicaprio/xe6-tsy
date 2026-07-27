package sessions

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestServiceConsumeRuntimeFailureAfterCleanup(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusActive)}
	service, realtime := newTestService(t, repository)
	realtime.getSnapshot.RuntimeState = RuntimeStopped

	occurredAt := time.Date(2026, 7, 27, 9, 30, 0, 0, time.UTC)
	err := service.ConsumeRuntimeFailure(context.Background(), RuntimeFailure{
		SessionID: "vs_1", TraceID: "req_failure", ErrorCode: "asr_unavailable", OccurredAt: occurredAt,
	})
	if err != nil {
		t.Fatalf("ConsumeRuntimeFailure() error = %v", err)
	}
	if repository.session.Status != StatusFailed || repository.session.EndedAt == nil {
		t.Fatalf("session = %#v, want failed terminal state", repository.session)
	}
	if len(repository.failureTransitions) != 1 ||
		repository.failureTransitions[0].ErrorCode != "asr_unavailable" {
		t.Fatalf("failure transitions = %#v", repository.failureTransitions)
	}
}

func TestServiceConsumeRuntimeFailureRequiresStoppedRuntime(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusActive)}
	service, realtime := newTestService(t, repository)
	realtime.getSnapshot.RuntimeState = RuntimeFailed

	err := service.ConsumeRuntimeFailure(context.Background(), RuntimeFailure{
		SessionID: "vs_1", TraceID: "req_failure", ErrorCode: "asr_unavailable",
		OccurredAt: time.Now(),
	})
	if !errors.Is(err, ErrRealtimeStopFailed) {
		t.Fatalf("ConsumeRuntimeFailure() error = %v, want ErrRealtimeStopFailed", err)
	}
	if len(repository.failureTransitions) != 0 {
		t.Fatalf("failure transitions = %d, want 0", len(repository.failureTransitions))
	}
}

func TestServiceConsumeRuntimeFailureIsIdempotentForTerminalSession(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusFailed)}
	service, realtime := newTestService(t, repository)

	err := service.ConsumeRuntimeFailure(context.Background(), RuntimeFailure{
		SessionID: "vs_1", TraceID: "req_failure", ErrorCode: "asr_unavailable",
		OccurredAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("ConsumeRuntimeFailure() error = %v", err)
	}
	_, _, get := realtime.callCounts()
	if get != 0 {
		t.Fatalf("runtime get calls = %d, want 0", get)
	}
}
