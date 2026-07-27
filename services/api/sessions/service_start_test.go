package sessions

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestServiceStartRunsPrerequisitesBeforeTransition(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusCreated)}
	service, realtime := newTestService(t, repository)
	languages := service.deps.LanguageConfigs.(*fakeLanguageConfigs)
	connections := service.deps.WebRTCConnections.(*fakeWebRTCConnections)

	got, err := service.Start(context.Background(), validStartInput())
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got.Status != StatusActive || got.StartedAt == nil {
		t.Fatalf("Start() = %#v", got)
	}
	if languages.calls != 1 || connections.calls != 1 {
		t.Fatalf("prerequisite calls = languages %d, connections %d; want 1, 1",
			languages.calls, connections.calls)
	}
	start, stop, _ := realtime.callCounts()
	if start != 1 || stop != 0 || len(repository.startTransitions) != 1 {
		t.Fatalf("calls = realtime start %d, stop %d, transition %d; want 1, 0, 1",
			start, stop, len(repository.startTransitions))
	}
}

func TestServiceStartRejectsUnmetPrerequisites(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Service, *fakeRealtime)
		want error
	}{
		{
			name: "language config not ready",
			edit: func(service *Service, realtime *fakeRealtime) {
				service.deps.LanguageConfigs.(*fakeLanguageConfigs).snapshot.LanguagePairCount = 1
			},
			want: ErrLanguageConfigNotReady,
		},
		{
			name: "language dependency error",
			edit: func(service *Service, realtime *fakeRealtime) {
				service.deps.LanguageConfigs.(*fakeLanguageConfigs).err = errDependency
			},
			want: ErrLanguageConfigNotReady,
		},
		{
			name: "language dependency not implemented",
			edit: func(service *Service, realtime *fakeRealtime) {
				service.deps.LanguageConfigs.(*fakeLanguageConfigs).err = ErrNotImplemented
			},
			want: ErrNotImplemented,
		},
		{
			name: "WebRTC not ready",
			edit: func(service *Service, realtime *fakeRealtime) {
				service.deps.WebRTCConnections.(*fakeWebRTCConnections).snapshot.ConnectionState = ConnectionConnecting
			},
			want: ErrWebRTCNotReady,
		},
		{
			name: "WebRTC dependency error",
			edit: func(service *Service, realtime *fakeRealtime) {
				service.deps.WebRTCConnections.(*fakeWebRTCConnections).err = errDependency
			},
			want: ErrWebRTCUnavailable,
		},
		{
			name: "realtime start error",
			edit: func(service *Service, realtime *fakeRealtime) {
				realtime.startErr = errDependency
			},
			want: ErrRealtimeStartFailed,
		},
		{
			name: "realtime already running",
			edit: func(service *Service, realtime *fakeRealtime) {
				realtime.startErr = ErrRealtimeAlreadyRunning
			},
			want: ErrRealtimeAlreadyRunning,
		},
		{
			name: "realtime reports failed",
			edit: func(service *Service, realtime *fakeRealtime) {
				realtime.startSnapshot.RuntimeState = RuntimeFailed
			},
			want: ErrRealtimeStartFailed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := &fakeRepository{session: newVoiceSession(t, StatusCreated)}
			service, realtime := newTestService(t, repository)
			test.edit(service, realtime)

			_, err := service.Start(context.Background(), validStartInput())
			if !errors.Is(err, test.want) {
				t.Fatalf("Start() error = %v, want %v", err, test.want)
			}
			if repository.session.Status != StatusCreated || len(repository.startTransitions) != 0 {
				t.Fatalf("session = %#v, transitions = %d; want unchanged", repository.session, len(repository.startTransitions))
			}
		})
	}
}

func TestServiceStartReplaysActiveSessionWithoutCallingDependencies(t *testing.T) {
	session := newVoiceSession(t, StatusActive)
	startedAt := session.CreatedAt.Add(time.Minute)
	session.StartedAt = &startedAt
	repository := &fakeRepository{
		session:   session,
		startKey:  "start_1",
		startHash: "hash_1",
	}
	service, realtime := newTestService(t, repository)

	got, err := service.Start(context.Background(), validStartInput())
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if got.Status != StatusActive {
		t.Fatalf("Start() = %#v", got)
	}
	if service.deps.LanguageConfigs.(*fakeLanguageConfigs).calls != 0 ||
		service.deps.WebRTCConnections.(*fakeWebRTCConnections).calls != 0 {
		t.Fatal("active replay called readiness dependencies")
	}
	start, _, _ := realtime.callCounts()
	if start != 0 {
		t.Fatalf("realtime start calls = %d, want 0", start)
	}
}

func TestServiceStartCompensatesTransitionFailureAfterCancellation(t *testing.T) {
	repository := &fakeRepository{
		session:              newVoiceSession(t, StatusCreated),
		transitionActiveErrs: []error{errDependency},
	}
	service, realtime := newTestService(t, repository)
	ctx, cancel := context.WithCancel(context.Background())
	repository.transitionActiveHook = cancel

	var compensationContextValid bool
	realtime.stopHook = func(ctx context.Context) {
		_, hasDeadline := ctx.Deadline()
		compensationContextValid = ctx.Err() == nil && hasDeadline
	}

	_, err := service.Start(ctx, validStartInput())
	if !errors.Is(err, errDependency) {
		t.Fatalf("Start() error = %v, want transition error", err)
	}
	if !compensationContextValid {
		t.Fatal("compensation inherited cancellation or had no deadline")
	}
	_, stop, _ := realtime.callCounts()
	if stop != 1 || repository.session.Status != StatusCreated {
		t.Fatalf("stop calls = %d, session status = %q; want 1, created", stop, repository.session.Status)
	}
}

func TestServiceStartSerializesConcurrentRequests(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusCreated)}
	service, realtime := newTestService(t, repository)
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	realtime.startHook = func(ctx context.Context) {
		close(startEntered)
		<-releaseStart
	}

	var wg sync.WaitGroup
	results := make(chan error, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := service.Start(context.Background(), validStartInput())
		results <- err
	}()
	<-startEntered

	secondStarted := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(secondStarted)
		_, err := service.Start(context.Background(), validStartInput())
		results <- err
	}()
	<-secondStarted
	close(releaseStart)
	wg.Wait()
	close(results)

	for err := range results {
		if err != nil {
			t.Fatalf("concurrent Start() error = %v", err)
		}
	}
	start, _, _ := realtime.callCounts()
	if start != 1 {
		t.Fatalf("realtime start calls = %d, want 1", start)
	}
}

func validStartInput() StartInput {
	return StartInput{
		AccountID:      "acct_1",
		SessionID:      "vs_1",
		IdempotencyKey: "start_1",
		RequestHash:    "hash_1",
		TraceID:        "req_1",
		StartedBy:      "acct_1",
	}
}
