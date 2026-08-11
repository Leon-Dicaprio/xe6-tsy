package sessions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const maxHTTPBodyBytes = 1 << 20

// AccountIDFromRequest extracts the account ID written by trusted auth middleware.
type AccountIDFromRequest func(*http.Request) (string, bool)

// UseCases is the application boundary consumed by the HTTP handler.
type UseCases interface {
	Create(context.Context, CreateInput) (VoiceSession, error)
	Start(context.Context, StartInput) (VoiceSession, error)
	End(context.Context, EndInput) (VoiceSession, error)
	GetDetail(context.Context, DetailInput) (VoiceSessionDetail, error)
	GetState(context.Context, DetailInput) (StateSnapshot, error)
	List(context.Context, ListInput) (ListPage, error)
}

// RealtimeTicket is the browser-facing mint response for WebRTC signaling.
type RealtimeTicket struct {
	Ticket    string    `json:"ticket"`
	SessionID string    `json:"session_id"`
	ExpiresAt time.Time `json:"expires_at"`
}

// RealtimeTicketMinter issues short-lived realtime tickets for session owners.
type RealtimeTicketMinter interface {
	MintRealtimeTicket(ctx context.Context, accountID, sessionID string) (RealtimeTicket, error)
}

// Handler exposes Issue #86's public voice-session HTTP contract.
type Handler struct {
	service   UseCases
	accountID AccountIDFromRequest
	tickets   RealtimeTicketMinter
}

// NewHandler wires the HTTP boundary. The account extractor must read only
// middleware-injected identity; request bodies and headers are not authority.
func NewHandler(service UseCases, accountID AccountIDFromRequest) *Handler {
	if accountID == nil {
		accountID = func(*http.Request) (string, bool) { return "", false }
	}
	return &Handler{service: service, accountID: accountID}
}

// WithRealtimeTickets enables POST .../realtime-ticket for browser WebRTC join.
func (h *Handler) WithRealtimeTickets(tickets RealtimeTicketMinter) *Handler {
	if h == nil {
		return nil
	}
	h.tickets = tickets
	return h
}

// Register attaches voice-session lifecycle routes behind authentication.
func (h *Handler) Register(mux *http.ServeMux, authenticate func(http.Handler) http.Handler) {
	if authenticate == nil {
		panic("sessions authentication middleware is required")
	}
	mux.Handle("POST /api/v1/voice-sessions", authenticate(http.HandlerFunc(h.create)))
	mux.Handle("POST /api/v1/voice-sessions/{id}/start", authenticate(http.HandlerFunc(h.start)))
	mux.Handle("POST /api/v1/voice-sessions/{id}/end", authenticate(http.HandlerFunc(h.end)))
	mux.Handle("GET /api/v1/voice-sessions/{id}", authenticate(http.HandlerFunc(h.detail)))
	mux.Handle("GET /api/v1/voice-sessions", authenticate(http.HandlerFunc(h.list)))
	mux.Handle("GET /api/v1/voice-sessions/{id}/state", authenticate(http.HandlerFunc(h.state)))
	if h.tickets != nil {
		mux.Handle(
			"POST /api/v1/voice-sessions/{id}/realtime-ticket",
			authenticate(http.HandlerFunc(h.mintRealtimeTicket)),
		)
	}
}

// createRequest is the public JSON shape. AccountID and idempotency metadata are
// injected from authenticated request context and headers, not accepted here.
type createRequest struct {
	AudioConfig  *AudioConfig `json:"audio_config,omitempty"`
	Capabilities Capabilities `json:"capabilities"`
}

// endRequest contains the only optional End body field; an omitted reason is
// resolved before the request fingerprint is computed.
type endRequest struct {
	Reason EndReason `json:"reason,omitempty"`
}

// create converts the authenticated HTTP request into a canonical CreateInput.
// Account ownership comes exclusively from middleware, never from JSON fields.
func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	accountID, err := h.requireAccount(r)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	if h.service == nil {
		writeHTTPError(w, r, ErrNotImplemented)
		return
	}
	var body createRequest
	if err := decodeHTTPJSON(r, &body); err != nil {
		writeHTTPError(w, r, err)
		return
	}
	input := CreateInput{
		AccountID:      accountID,
		AudioConfig:    body.AudioConfig,
		Capabilities:   body.Capabilities,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		RequestHash:    canonicalHash("voice-sessions.create", body),
	}
	session, err := h.service.Create(r.Context(), input)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	writeHTTPJSON(w, http.StatusCreated, session)
}

// start rejects request bodies because the SessionID and authenticated actor are
// the complete public command. The stable hash still includes the SessionID so
// an idempotency key cannot be reused for a different Session.
func (h *Handler) start(w http.ResponseWriter, r *http.Request) {
	accountID, err := h.requireAccount(r)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	if h.service == nil {
		writeHTTPError(w, r, ErrNotImplemented)
		return
	}
	if err := rejectNonEmptyBody(r); err != nil {
		writeHTTPError(w, r, err)
		return
	}
	sessionID := r.PathValue("id")
	input := StartInput{
		AccountID:      accountID,
		SessionID:      sessionID,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		RequestHash: canonicalHash("voice-sessions.start", struct {
			SessionID string `json:"session_id"`
		}{SessionID: sessionID}),
		TraceID:   requestIDFromHTTP(r),
		StartedBy: accountID,
	}
	session, err := h.service.Start(r.Context(), input)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	writeHTTPJSON(w, http.StatusOK, session)
}

// end defaults the public reason while preserving that resolved value in the
// canonical request hash. Omitted and explicit default reasons therefore replay
// the same durable intent.
func (h *Handler) end(w http.ResponseWriter, r *http.Request) {
	accountID, err := h.requireAccount(r)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	if h.service == nil {
		writeHTTPError(w, r, ErrNotImplemented)
		return
	}
	body := endRequest{Reason: EndReasonUserRequested}
	if err := decodeOptionalHTTPJSON(r, &body); err != nil {
		writeHTTPError(w, r, err)
		return
	}
	if body.Reason == "" {
		body.Reason = EndReasonUserRequested
	}
	sessionID := r.PathValue("id")
	input := EndInput{
		AccountID:      accountID,
		SessionID:      sessionID,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
		RequestHash: canonicalHash("voice-sessions.end", struct {
			SessionID string    `json:"session_id"`
			Reason    EndReason `json:"reason"`
		}{SessionID: sessionID, Reason: body.Reason}),
		TraceID: requestIDFromHTTP(r),
		Reason:  body.Reason,
	}
	session, err := h.service.End(r.Context(), input)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	writeHTTPJSON(w, http.StatusOK, session)
}

// detail returns the persistent Session plus one validated realtime snapshot.
func (h *Handler) detail(w http.ResponseWriter, r *http.Request) {
	accountID, err := h.requireAccount(r)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	if h.service == nil {
		writeHTTPError(w, r, ErrNotImplemented)
		return
	}
	detail, err := h.service.GetDetail(r.Context(), DetailInput{
		AccountID: accountID,
		SessionID: r.PathValue("id"),
	})
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	writeHTTPJSON(w, http.StatusOK, detail)
}

// state exposes the compact projection intended for client polling.
func (h *Handler) state(w http.ResponseWriter, r *http.Request) {
	accountID, err := h.requireAccount(r)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	if h.service == nil {
		writeHTTPError(w, r, ErrNotImplemented)
		return
	}
	state, err := h.service.GetState(r.Context(), DetailInput{
		AccountID: accountID,
		SessionID: r.PathValue("id"),
	})
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	writeHTTPJSON(w, http.StatusOK, state)
}

// mintRealtimeTicket issues a short-lived credential only after the ticket
// boundary independently verifies Session ownership.
func (h *Handler) mintRealtimeTicket(w http.ResponseWriter, r *http.Request) {
	accountID, err := h.requireAccount(r)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	if h.tickets == nil {
		writeHTTPError(w, r, ErrNotImplemented)
		return
	}
	if err := rejectNonEmptyBody(r); err != nil {
		writeHTTPError(w, r, err)
		return
	}
	ticket, err := h.tickets.MintRealtimeTicket(r.Context(), accountID, r.PathValue("id"))
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	writeHTTPJSON(w, http.StatusOK, ticket)
}

// list accepts only persistent filters. Runtime and connection filters are
// deliberately absent so one page never expands into cross-service N+1 calls.
func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	accountID, err := h.requireAccount(r)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	if h.service == nil {
		writeHTTPError(w, r, ErrNotImplemented)
		return
	}
	input := ListInput{
		AccountID: accountID,
		Cursor:    r.URL.Query().Get("cursor"),
	}
	if raw := r.URL.Query().Get("status"); raw != "" {
		status := Status(raw)
		input.Status = &status
	}
	limit, err := parseListLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	input.Limit = limit
	page, err := h.service.List(r.Context(), input)
	if err != nil {
		writeHTTPError(w, r, err)
		return
	}
	writeHTTPJSON(w, http.StatusOK, page)
}

// requireAccount reads identity previously injected by trusted authentication
// middleware. Raw headers or request bodies are not authorization sources.
func (h *Handler) requireAccount(r *http.Request) (string, error) {
	accountID, ok := h.accountID(r)
	if !ok || accountID == "" {
		return "", ErrUnauthorized
	}
	return accountID, nil
}

// parseListLimit preserves zero as "use the service default" and rejects values
// outside the contract before invoking a use case.
func parseListLimit(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > maxListLimit {
		return 0, ErrInvalidRequest
	}
	return limit, nil
}

// decodeHTTPJSON accepts exactly one bounded JSON object and rejects unknown
// fields or trailing values. The body limit prevents unbounded allocation at
// the public boundary.
func decodeHTTPJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxHTTPBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrInvalidRequest
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrInvalidRequest
	}
	return nil
}

// decodeOptionalHTTPJSON applies the same strict decoding contract while
// allowing an empty body for commands with complete server-side defaults.
func decodeOptionalHTTPJSON(r *http.Request, target any) error {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxHTTPBodyBytes))
	if err != nil {
		return ErrInvalidRequest
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return ErrInvalidRequest
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrInvalidRequest
	}
	return nil
}

// rejectNonEmptyBody enforces bodyless command contracts without trusting
// Content-Length, which may be absent for streamed requests.
func rejectNonEmptyBody(r *http.Request) error {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, maxHTTPBodyBytes))
	if err != nil {
		return ErrInvalidRequest
	}
	if strings.TrimSpace(string(body)) != "" {
		return ErrInvalidRequest
	}
	return nil
}

// canonicalHash namespaces a deterministic JSON fingerprint by operation. The
// NUL separator prevents ambiguous concatenation, and the namespace prevents
// one Idempotency-Key payload from matching a different command type.
func canonicalHash(operation string, value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("sessions canonical request hash: %v", err))
	}
	sum := sha256.Sum256(append([]byte(operation+"\x00"), payload...))
	return hex.EncodeToString(sum[:])
}

// httpErrorEnvelope preserves one stable top-level error object for clients.
type httpErrorEnvelope struct {
	Error httpErrorDetail `json:"error"`
}

// httpErrorDetail exposes only stable, non-sensitive failure information.
type httpErrorDetail struct {
	Code      string         `json:"code"`
	Message   string         `json:"message"`
	RequestID string         `json:"request_id"`
	Retryable bool           `json:"retryable"`
	Details   map[string]any `json:"details"`
}

// writeHTTPJSON applies the module's JSON content type and status consistently.
func writeHTTPJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// writeHTTPError emits the stable public envelope and never exposes wrapped
// provider or persistence details to clients.
func writeHTTPError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, message := statusCodeForError(err)
	writeHTTPJSON(w, status, httpErrorEnvelope{
		Error: httpErrorDetail{
			Code:      string(code),
			Message:   message,
			RequestID: requestIDFromHTTP(r),
			Retryable: false,
			Details:   map[string]any{},
		},
	})
}

// statusCodeForError is the single public mapping from wrapped domain errors to
// HTTP semantics. errors.Is keeps the mapping stable through contextual wraps.
func statusCodeForError(err error) (int, ErrorCode, string) {
	switch {
	case errors.Is(err, ErrInvalidRequest):
		return http.StatusBadRequest, CodeInvalidRequest, "invalid request"
	case errors.Is(err, ErrUnauthorized):
		return http.StatusUnauthorized, CodeUnauthorized, "authentication required"
	case errors.Is(err, ErrVoiceSessionNotFound):
		return http.StatusNotFound, CodeVoiceSessionNotFound, "voice session not found"
	case errors.Is(err, ErrIdempotencyKeyConflict):
		return http.StatusConflict, CodeIdempotencyKeyConflict, "idempotency key was reused with a different request"
	case errors.Is(err, ErrSessionStartInProgress):
		return http.StatusConflict, CodeSessionStartInProgress, "voice session start is already in progress"
	case errors.Is(err, ErrSessionStateConflict):
		return http.StatusConflict, CodeSessionStateConflict, "voice session state does not allow this operation"
	case errors.Is(err, ErrLanguageConfigNotReady):
		return http.StatusConflict, CodeLanguageConfigNotReady, "language config is not ready"
	case errors.Is(err, ErrWebRTCNotReady):
		return http.StatusConflict, CodeWebRTCNotReady, "WebRTC connection is not ready"
	case errors.Is(err, ErrRealtimeAlreadyRunning):
		return http.StatusConflict, CodeRealtimeAlreadyRunning, "realtime pipeline is already running"
	case errors.Is(err, ErrUnsupportedAudio):
		return http.StatusUnprocessableEntity, CodeUnsupportedAudio, "audio config is not supported"
	case errors.Is(err, ErrRealtimeStartFailed):
		return http.StatusServiceUnavailable, CodeRealtimeStartFailed, "realtime pipeline failed to start"
	case errors.Is(err, ErrRealtimeStopFailed):
		return http.StatusServiceUnavailable, CodeRealtimeStopFailed, "realtime pipeline failed to stop"
	case errors.Is(err, ErrRuntimeUnavailable):
		return http.StatusServiceUnavailable, CodeRuntimeUnavailable, "runtime state is unavailable"
	case errors.Is(err, ErrWebRTCUnavailable):
		return http.StatusServiceUnavailable, CodeWebRTCUnavailable, "WebRTC state is unavailable"
	case errors.Is(err, ErrNotImplemented):
		return http.StatusNotImplemented, CodeNotImplemented, "voice session dependency is not implemented yet"
	default:
		return http.StatusInternalServerError, ErrorCode("internal_error"), "internal error"
	}
}

// requestIDFromHTTP returns the upstream trace identity or a visible sentinel;
// it never invents a random value that cannot be correlated with server logs.
func requestIDFromHTTP(r *http.Request) string {
	if requestID := r.Header.Get("X-Request-ID"); requestID != "" {
		return requestID
	}
	return "req_missing"
}
