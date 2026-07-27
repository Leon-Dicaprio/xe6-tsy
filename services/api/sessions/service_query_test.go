package sessions

import (
	"context"
	"errors"
	"testing"
)

func TestServiceDetailAndStateCombineLiveRuntime(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusCreated)}
	service, realtime := newTestService(t, repository)
	realtime.getSnapshot.RuntimeState = RuntimeFailed

	detail, err := service.GetDetail(context.Background(), DetailInput{
		AccountID: "acct_1", SessionID: "vs_1",
	})
	if err != nil {
		t.Fatalf("GetDetail() error = %v", err)
	}
	if detail.RuntimeState != RuntimeFailed || !detail.Retryable {
		t.Fatalf("GetDetail() = %#v, want retryable failed runtime", detail)
	}

	state, err := service.GetState(context.Background(), DetailInput{
		AccountID: "acct_1", SessionID: "vs_1",
	})
	if err != nil {
		t.Fatalf("GetState() error = %v", err)
	}
	if state.SessionID != "vs_1" || state.RuntimeState != RuntimeFailed || !state.Retryable {
		t.Fatalf("GetState() = %#v", state)
	}
	_, _, getCalls := realtime.callCounts()
	if getCalls != 2 {
		t.Fatalf("runtime get calls = %d, want 2", getCalls)
	}
}

func TestServiceDetailMapsRuntimeDependencyFailure(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusActive)}
	service, realtime := newTestService(t, repository)
	realtime.getErr = errDependency

	_, err := service.GetDetail(context.Background(), DetailInput{
		AccountID: "acct_1", SessionID: "vs_1",
	})
	if !errors.Is(err, ErrRuntimeUnavailable) {
		t.Fatalf("GetDetail() error = %v, want ErrRuntimeUnavailable", err)
	}
}

func TestServiceDetailPreservesNotImplementedDependency(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusActive)}
	service, realtime := newTestService(t, repository)
	realtime.getErr = ErrNotImplemented

	_, err := service.GetDetail(context.Background(), DetailInput{
		AccountID: "acct_1", SessionID: "vs_1",
	})
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("GetDetail() error = %v, want ErrNotImplemented", err)
	}
}

func TestServiceListUsesPersistentRepositoryOnly(t *testing.T) {
	repository := &fakeRepository{
		session: newVoiceSession(t, StatusEnded),
		page: ListPage{Sessions: []VoiceSessionListItem{{
			ID: "vs_1", AccountID: "acct_1", Status: StatusEnded,
		}}},
	}
	service, realtime := newTestService(t, repository)

	page, err := service.List(context.Background(), ListInput{AccountID: "acct_1"})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(page.Sessions) != 1 || len(repository.listFilters) != 1 {
		t.Fatalf("List() = %#v, filters = %#v", page, repository.listFilters)
	}
	if repository.listFilters[0].Limit != defaultListLimit {
		t.Fatalf("list limit = %d, want %d", repository.listFilters[0].Limit, defaultListLimit)
	}
	_, _, getCalls := realtime.callCounts()
	if getCalls != 0 {
		t.Fatalf("runtime get calls = %d, want 0", getCalls)
	}
}

func TestServiceListValidatesFilter(t *testing.T) {
	invalidStatus := Status("unknown")
	tests := []ListInput{
		{},
		{AccountID: "acct_1", Status: &invalidStatus},
		{AccountID: "acct_1", Limit: -1},
		{AccountID: "acct_1", Limit: 101},
	}
	for _, input := range tests {
		repository := &fakeRepository{}
		service, _ := newTestService(t, repository)
		if _, err := service.List(context.Background(), input); err == nil {
			t.Fatalf("List(%#v) error = nil", input)
		}
		if len(repository.listFilters) != 0 {
			t.Fatalf("List(%#v) reached repository", input)
		}
	}
}

func TestServiceGetSessionUsesTrustedRead(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusActive)}
	service, _ := newTestService(t, repository)

	got, err := service.GetSession(context.Background(), "vs_1")
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if got.SessionID != "vs_1" || got.AccountID != "acct_1" || got.Status != StatusActive {
		t.Fatalf("GetSession() = %#v", got)
	}
}
