package sessions

import (
	"context"
	"sync"
)

// keyedLocker serializes lifecycle operations within one process. Repository
// conditional transitions remain the cross-process consistency boundary.
type keyedLocker struct {
	mu    sync.Mutex
	locks map[string]*keyedLockEntry
}

// keyedLockEntry is a one-token semaphore plus a reference count covering the
// current holder and every waiter. A channel is used instead of sync.Mutex so
// waiting for a Session lock can honor context cancellation.
type keyedLockEntry struct {
	token      chan struct{}
	references int
}

// newKeyedLocker creates an empty lock registry. Entries are allocated lazily
// and reclaimed after their final holder or waiter leaves.
func newKeyedLocker() keyedLocker {
	return keyedLocker{locks: make(map[string]*keyedLockEntry)}
}

// newKeyedLockEntry seeds a binary semaphore with its single available token.
func newKeyedLockEntry() *keyedLockEntry {
	entry := &keyedLockEntry{token: make(chan struct{}, 1)}
	entry.token <- struct{}{}
	return entry
}

// lock acquires the process-local lock for key and returns an idempotent unlock
// closure. The caller must invoke the closure exactly as it would unlock a
// mutex; sync.Once additionally makes defensive duplicate calls harmless.
func (l *keyedLocker) lock(ctx context.Context, key string) (func(), error) {
	l.mu.Lock()
	entry := l.locks[key]
	if entry == nil {
		entry = newKeyedLockEntry()
		l.locks[key] = entry
	}
	// Register the waiter before it blocks. This prevents the previous holder
	// from deleting an entry that already has an operation queued behind it.
	entry.references++
	l.mu.Unlock()

	select {
	case <-ctx.Done():
		l.releaseReference(key, entry)
		return nil, ctx.Err()
	case <-entry.token:
		// Cancellation and token delivery can become ready together. Rechecking
		// after acquisition ensures a canceled operation never enters its critical
		// section and immediately returns the token for the next waiter.
		if err := ctx.Err(); err != nil {
			entry.token <- struct{}{}
			l.releaseReference(key, entry)
			return nil, err
		}
		var once sync.Once
		return func() {
			once.Do(func() {
				entry.token <- struct{}{}
				l.releaseReference(key, entry)
			})
		}, nil
	}
}

// releaseReference removes an idle per-key entry without affecting a newer
// entry that may have been installed for the same key.
func (l *keyedLocker) releaseReference(key string, entry *keyedLockEntry) {
	// Waiters increment references before blocking, so zero references makes
	// the entry safe to reclaim without racing a queued operation.
	l.mu.Lock()
	defer l.mu.Unlock()
	entry.references--
	if entry.references == 0 && l.locks[key] == entry {
		delete(l.locks, key)
	}
}
