package sessions

import (
	"context"
	"errors"
	"testing"
)

func TestNewServiceRejectsMissingDependencies(t *testing.T) {
	repository := &fakeRepository{session: newVoiceSession(t, StatusCreated)}
	service, _ := newTestService(t, repository)
	valid := service.deps
	tests := []struct {
		name string
		edit func(*Dependencies)
	}{
		{name: "repository", edit: func(deps *Dependencies) { deps.Repository = nil }},
		{name: "language configs", edit: func(deps *Dependencies) { deps.LanguageConfigs = nil }},
		{name: "WebRTC connections", edit: func(deps *Dependencies) { deps.WebRTCConnections = nil }},
		{name: "realtime", edit: func(deps *Dependencies) { deps.Realtime = nil }},
		{name: "IDs", edit: func(deps *Dependencies) { deps.IDs = nil }},
		{name: "clock", edit: func(deps *Dependencies) { deps.Clock = nil }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deps := valid
			test.edit(&deps)
			if _, err := NewService(deps); err == nil {
				t.Fatal("NewService() error = nil, want missing dependency error")
			}
		})
	}
}

func TestServiceCreateUsesDefaultsWithoutCallingRealtime(t *testing.T) {
	repository := &fakeRepository{}
	service, realtime := newTestService(t, repository)

	got, err := service.Create(context.Background(), CreateInput{
		AccountID:      "acct_1",
		Capabilities:   validCapabilities(),
		IdempotencyKey: "create_1",
		RequestHash:    "hash_1",
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if got.Status != StatusCreated || got.AccountID != "acct_1" {
		t.Fatalf("Create() = %#v", got)
	}
	if len(repository.createParams) != 1 {
		t.Fatalf("Create repository calls = %d, want 1", len(repository.createParams))
	}
	params := repository.createParams[0]
	if params.AudioConfig != DefaultAudioConfig() {
		t.Fatalf("AudioConfig = %#v, want default", params.AudioConfig)
	}
	start, stop, get := realtime.callCounts()
	if start != 0 || stop != 0 || get != 0 {
		t.Fatalf("realtime calls = start %d, stop %d, get %d; want zero", start, stop, get)
	}
}

func TestServiceCreateValidatesInput(t *testing.T) {
	unsupported := DefaultAudioConfig()
	unsupported.Codec = "pcm"
	tests := []struct {
		name  string
		input CreateInput
		want  error
	}{
		{
			name: "missing account",
			input: CreateInput{
				Capabilities: validCapabilities(), IdempotencyKey: "key", RequestHash: "hash",
			},
			want: ErrUnauthorized,
		},
		{
			name: "missing idempotency key",
			input: CreateInput{
				AccountID: "acct_1", Capabilities: validCapabilities(), RequestHash: "hash",
			},
			want: ErrInvalidRequest,
		},
		{
			name: "missing request hash",
			input: CreateInput{
				AccountID: "acct_1", Capabilities: validCapabilities(), IdempotencyKey: "key",
			},
			want: ErrInvalidRequest,
		},
		{
			name: "missing required capability",
			input: CreateInput{
				AccountID: "acct_1", IdempotencyKey: "key", RequestHash: "hash",
			},
			want: ErrInvalidRequest,
		},
		{
			name: "unsupported audio",
			input: CreateInput{
				AccountID: "acct_1", AudioConfig: &unsupported,
				Capabilities: validCapabilities(), IdempotencyKey: "key", RequestHash: "hash",
			},
			want: ErrUnsupportedAudio,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := &fakeRepository{}
			service, _ := newTestService(t, repository)
			_, err := service.Create(context.Background(), test.input)
			if !errors.Is(err, test.want) {
				t.Fatalf("Create() error = %v, want %v", err, test.want)
			}
			if len(repository.createParams) != 0 {
				t.Fatalf("repository calls = %d, want 0", len(repository.createParams))
			}
		})
	}
}

func TestServiceCreatePropagatesRepositoryAndContextErrors(t *testing.T) {
	t.Run("repository", func(t *testing.T) {
		repository := &fakeRepository{createErr: errDependency}
		service, _ := newTestService(t, repository)
		_, err := service.Create(context.Background(), CreateInput{
			AccountID:      "acct_1",
			Capabilities:   validCapabilities(),
			IdempotencyKey: "key",
			RequestHash:    "hash",
		})
		if !errors.Is(err, errDependency) {
			t.Fatalf("Create() error = %v, want dependency error", err)
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		repository := &fakeRepository{}
		service, _ := newTestService(t, repository)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := service.Create(ctx, CreateInput{
			AccountID:      "acct_1",
			Capabilities:   validCapabilities(),
			IdempotencyKey: "key",
			RequestHash:    "hash",
		})
		if !errors.Is(err, context.Canceled) || len(repository.createParams) != 0 {
			t.Fatalf("Create() error = %v, repository calls = %d", err, len(repository.createParams))
		}
	})
}
