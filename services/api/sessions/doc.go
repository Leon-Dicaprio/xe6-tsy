// Package sessions owns the control-plane lifecycle of an entire voice
// translation session. A Session spans creation, realtime activation, ongoing
// operation, and a terminal ended or failed state; it is not an individual
// speech/translation Turn. Realtime produces Turns and the records module
// persists them under the SessionID managed here.
//
// The package deliberately keeps three state machines separate:
//   - VoiceSession.Status is durable business state owned by this package.
//   - RuntimeState is live media-pipeline state owned by realtime-audio.
//   - ConnectionState is WebRTC state owned by the realtime connection manager.
//
// Handler translates the authenticated HTTP contract into typed use-case
// inputs. Service coordinates business rules through consumer-owned ports.
// PostgresRepository provides the cross-process authority for ownership,
// idempotency, state transitions, compensation, and end recovery. Adapters in
// services/api/realtimeaccess translate language and realtime provider types
// into the projections defined by this package.
//
// The lifecycle relies on two durable coordination records. StartOperation
// binds one idempotent Start request and OperationID to the runtime instance it
// creates; only a runtime reporting that OperationID may activate the business
// session. EndIntent records the requested shutdown before Realtime.Stop so a
// timeout, canceled request, or process restart cannot lose cleanup work.
//
// In-process keyed locking only reduces duplicate work within one API process.
// PostgreSQL row locks, uniqueness constraints, expected-state transitions,
// operation ownership, and recovery leases remain the multi-instance
// consistency boundary.
package sessions
