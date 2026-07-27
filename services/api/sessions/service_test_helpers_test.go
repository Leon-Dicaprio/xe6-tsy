package sessions

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

var errDependency = errors.New("dependency failed")

type fakeIDGenerator struct {
	mu    sync.Mutex
	next  int
	calls int
}

func (f *fakeIDGenerator) NewVoiceSessionID() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.next++
	return "vs_test_" + string(rune('0'+f.next))
}

type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	calls int
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.now.Add(time.Duration(f.calls-1) * time.Second)
}

type fakeLanguageConfigs struct {
	snapshot LanguageConfigSnapshot
	err      error
	calls    int
}

func (f *fakeLanguageConfigs) GetCurrentConfig(
	ctx context.Context,
	sessionID string,
) (LanguageConfigSnapshot, error) {
	f.calls++
	return f.snapshot, f.err
}

type fakeWebRTCConnections struct {
	snapshot WebRTCConnectionSnapshot
	err      error
	calls    int
}

func (f *fakeWebRTCConnections) GetConnectionState(
	ctx context.Context,
	sessionID string,
) (WebRTCConnectionSnapshot, error) {
	f.calls++
	return f.snapshot, f.err
}

type fakeRealtime struct {
	mu sync.Mutex

	startSnapshot RuntimeSnapshot
	startErr      error
	startCalls    int
	startHook     func(context.Context)

	stopSnapshots []RuntimeSnapshot
	stopErrors    []error
	stopCalls     int
	stopHook      func(context.Context)

	getSnapshot RuntimeSnapshot
	getErr      error
	getCalls    int
}

func (f *fakeRealtime) Start(
	ctx context.Context,
	command StartRealtimeCommand,
) (RuntimeSnapshot, error) {
	f.mu.Lock()
	f.startCalls++
	hook := f.startHook
	snapshot := f.startSnapshot
	err := f.startErr
	f.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	return snapshot, err
}

func (f *fakeRealtime) Stop(
	ctx context.Context,
	command StopRealtimeCommand,
) (RuntimeSnapshot, error) {
	f.mu.Lock()
	index := f.stopCalls
	f.stopCalls++
	hook := f.stopHook
	var snapshot RuntimeSnapshot
	if len(f.stopSnapshots) > 0 {
		snapshot = f.stopSnapshots[min(index, len(f.stopSnapshots)-1)]
	}
	var err error
	if len(f.stopErrors) > 0 {
		err = f.stopErrors[min(index, len(f.stopErrors)-1)]
	}
	f.mu.Unlock()
	if hook != nil {
		hook(ctx)
	}
	return snapshot, err
}

func (f *fakeRealtime) GetRuntimeState(
	ctx context.Context,
	sessionID string,
) (RuntimeSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	return f.getSnapshot, f.getErr
}

func (f *fakeRealtime) callCounts() (start int, stop int, get int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.startCalls, f.stopCalls, f.getCalls
}

type fakeRepository struct {
	mu sync.Mutex

	session VoiceSession
	page    ListPage
	intent  *EndIntent

	createErr            error
	getErr               error
	getOwnedErr          error
	listErr              error
	saveEndIntentErr     error
	getEndIntentErr      error
	completeEndIntentErr error
	transitionActiveErrs []error
	transitionEndedErrs  []error
	transitionFailedErr  error
	transitionActiveHook func()

	createParams       []CreateParams
	listFilters        []ListFilter
	startTransitions   []StartTransitionParams
	endTransitions     []EndTransitionParams
	failureTransitions []FailureTransitionParams
	completeCalls      int

	startKey  string
	startHash string
}

func (f *fakeRepository) Create(
	ctx context.Context,
	params CreateParams,
) (VoiceSession, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createParams = append(f.createParams, params)
	if f.createErr != nil {
		return VoiceSession{}, false, f.createErr
	}
	if f.session.ID != "" {
		previous := f.createParams[0]
		if previous.IdempotencyKey != params.IdempotencyKey || previous.RequestHash != params.RequestHash {
			return VoiceSession{}, false, ErrIdempotencyKeyConflict
		}
		return f.session, true, nil
	}
	f.session = VoiceSession{
		ID:           params.ID,
		AccountID:    params.AccountID,
		Status:       StatusCreated,
		AudioConfig:  marshalTestJSON(params.AudioConfig),
		Capabilities: marshalTestJSON(params.Capabilities),
		CreatedAt:    params.CreatedAt,
	}
	return f.session, false, nil
}

func (f *fakeRepository) Get(ctx context.Context, sessionID string) (VoiceSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getErr != nil {
		return VoiceSession{}, f.getErr
	}
	if f.session.ID != sessionID {
		return VoiceSession{}, ErrVoiceSessionNotFound
	}
	return f.session, nil
}

func (f *fakeRepository) GetOwned(
	ctx context.Context,
	accountID string,
	sessionID string,
) (VoiceSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getOwnedErr != nil {
		return VoiceSession{}, f.getOwnedErr
	}
	if f.session.ID != sessionID || f.session.AccountID != accountID {
		return VoiceSession{}, ErrVoiceSessionNotFound
	}
	return f.session, nil
}

func (f *fakeRepository) List(ctx context.Context, filter ListFilter) (ListPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listFilters = append(f.listFilters, filter)
	return f.page, f.listErr
}

func (f *fakeRepository) SaveEndIntent(
	ctx context.Context,
	intent EndIntent,
) (EndIntent, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveEndIntentErr != nil {
		return EndIntent{}, false, f.saveEndIntentErr
	}
	if f.intent != nil {
		if !f.intent.MatchesRequest(intent.IdempotencyKey, intent.RequestHash) {
			return EndIntent{}, false, ErrIdempotencyKeyConflict
		}
		return *f.intent, true, nil
	}
	copy := intent
	f.intent = &copy
	return copy, false, nil
}

func (f *fakeRepository) GetEndIntent(
	ctx context.Context,
	accountID string,
	sessionID string,
) (EndIntent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getEndIntentErr != nil {
		return EndIntent{}, f.getEndIntentErr
	}
	if f.intent == nil || f.intent.AccountID != accountID || f.intent.SessionID != sessionID {
		return EndIntent{}, ErrEndIntentNotFound
	}
	return *f.intent, nil
}

func (f *fakeRepository) CompleteEndIntent(
	ctx context.Context,
	accountID string,
	sessionID string,
	completedAt time.Time,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completeCalls++
	if f.completeEndIntentErr != nil {
		return f.completeEndIntentErr
	}
	if f.intent == nil {
		return ErrEndIntentNotFound
	}
	f.intent.CompletedAt = &completedAt
	return nil
}

func (f *fakeRepository) TransitionToActive(
	ctx context.Context,
	params StartTransitionParams,
) (VoiceSession, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := len(f.startTransitions)
	f.startTransitions = append(f.startTransitions, params)
	if f.transitionActiveHook != nil {
		f.transitionActiveHook()
	}
	if len(f.transitionActiveErrs) > 0 {
		err := f.transitionActiveErrs[min(index, len(f.transitionActiveErrs)-1)]
		if err != nil {
			return VoiceSession{}, false, err
		}
	}
	if f.startKey != "" {
		if f.startKey != params.IdempotencyKey || f.startHash != params.RequestHash {
			return VoiceSession{}, false, ErrIdempotencyKeyConflict
		}
		return f.session, true, nil
	}
	if f.session.Status != params.Expected {
		return VoiceSession{}, false, ErrConcurrentTransition
	}
	f.startKey = params.IdempotencyKey
	f.startHash = params.RequestHash
	f.session.Status = StatusActive
	f.session.StartedAt = &params.StartedAt
	return f.session, false, nil
}

func (f *fakeRepository) TransitionToEnded(
	ctx context.Context,
	params EndTransitionParams,
) (VoiceSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := len(f.endTransitions)
	f.endTransitions = append(f.endTransitions, params)
	if len(f.transitionEndedErrs) > 0 {
		err := f.transitionEndedErrs[min(index, len(f.transitionEndedErrs)-1)]
		if err != nil {
			return VoiceSession{}, err
		}
	}
	if f.session.Status != params.Expected {
		return VoiceSession{}, ErrConcurrentTransition
	}
	f.session.Status = StatusEnded
	f.session.EndedAt = &params.EndedAt
	return f.session, nil
}

func (f *fakeRepository) TransitionToFailed(
	ctx context.Context,
	params FailureTransitionParams,
) (VoiceSession, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failureTransitions = append(f.failureTransitions, params)
	if f.transitionFailedErr != nil {
		return VoiceSession{}, f.transitionFailedErr
	}
	if f.session.Status != params.Expected {
		return VoiceSession{}, ErrConcurrentTransition
	}
	f.session.Status = StatusFailed
	f.session.EndedAt = &params.FailedAt
	return f.session, nil
}

func newTestService(t *testing.T, repository *fakeRepository) (*Service, *fakeRealtime) {
	t.Helper()
	now := time.Date(2026, 7, 27, 9, 0, 0, 0, time.UTC)
	realtime := &fakeRealtime{
		startSnapshot: RuntimeSnapshot{
			SessionID:    repository.session.ID,
			RuntimeState: RuntimeListening,
			UpdatedAt:    now,
		},
		stopSnapshots: []RuntimeSnapshot{{
			SessionID:    repository.session.ID,
			RuntimeState: RuntimeStopped,
			UpdatedAt:    now,
		}},
		getSnapshot: RuntimeSnapshot{
			SessionID:    repository.session.ID,
			RuntimeState: RuntimeListening,
			UpdatedAt:    now,
		},
	}
	service, err := NewService(Dependencies{
		Repository: repository,
		LanguageConfigs: &fakeLanguageConfigs{snapshot: LanguageConfigSnapshot{
			SessionID: repository.session.ID, Version: 1,
			LanguagePairCount: 2, Status: LanguageConfigActive,
		}},
		WebRTCConnections: &fakeWebRTCConnections{snapshot: WebRTCConnectionSnapshot{
			SessionID: repository.session.ID, ConnectionID: "pc_1",
			ConnectionState: ConnectionConnected, UpdatedAt: now,
		}},
		Realtime: realtime,
		IDs:      &fakeIDGenerator{},
		Clock:    &fakeClock{now: now},
	})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service, realtime
}

func newVoiceSession(t *testing.T, status Status) VoiceSession {
	t.Helper()
	return VoiceSession{
		ID:           "vs_1",
		AccountID:    "acct_1",
		Status:       status,
		AudioConfig:  marshalTestJSON(DefaultAudioConfig()),
		Capabilities: marshalTestJSON(validCapabilities()),
		CreatedAt:    time.Date(2026, 7, 27, 8, 0, 0, 0, time.UTC),
	}
}

func validCapabilities() Capabilities {
	return Capabilities{
		WebRTC:             true,
		DataChannel:        true,
		Microphone:         true,
		Speaker:            true,
		SpeakerDiarization: true,
	}
}

func marshalTestJSON(value any) json.RawMessage {
	body, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return body
}
