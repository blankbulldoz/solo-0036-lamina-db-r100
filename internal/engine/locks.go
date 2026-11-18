package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"lamina/internal/datum"
)

// Row locks are process-local, per-key modification qualifications. They do
// not provide predicate locking, range serialization or deadlock freedom.
const (
	defaultLeaseMS = 60000
	minLeaseMS     = 100
	maxLeaseMS     = 600000
	maxWaitMS      = 10000
	maxLocksPerTx  = 1000
	maxTotalLocks  = 4096
)

var errCanceledWait = errors.New("lock wait canceled")

type heldLock struct {
	token    uint64
	owner    string
	relation string
	key      datum.Value
	lease    time.Duration
	acquired time.Time
	expires  time.Time
	timer    *time.Timer
}

type waitRecord struct {
	id       string
	relation string
	key      datum.Value
	since    time.Time
	canceled bool
}

// HeldLock is the public description of a currently held row qualification.
type HeldLock struct {
	ID          string      `json:"id"`
	Relation    string      `json:"relation"`
	Key         datum.Value `json:"key"`
	Transaction string      `json:"transaction"`
	LeaseMS     int         `json:"lease_ms"`
	AcquiredAt  string      `json:"acquired_at"`
	ExpiresAt   string      `json:"expires_at"`
}

// WaitingLock is the public description of a bounded lock wait in progress.
type WaitingLock struct {
	ID          string      `json:"id"`
	Relation    string      `json:"relation"`
	Key         datum.Value `json:"key"`
	Transaction string      `json:"transaction"`
	Holder      string      `json:"holder,omitempty"`
	WaitedMS    int64       `json:"waited_ms"`
}

// LockCounters are process facts; they reset when the instance restarts.
type LockCounters struct {
	Acquired  uint64 `json:"acquired_since_open"`
	Renewed   uint64 `json:"renewed_since_open"`
	Released  uint64 `json:"released_since_open"`
	Expired   uint64 `json:"expired_since_open"`
	Waited    uint64 `json:"waited_since_open"`
	TimedOut  uint64 `json:"timed_out_since_open"`
	Canceled  uint64 `json:"canceled_since_open"`
	Aborted   uint64 `json:"aborted_waits_since_open"`
	Deadlocks uint64 `json:"deadlocks_since_open"`
}

// LockSnapshot reports real held and waiting occupancy, never historical leases.
type LockSnapshot struct {
	Held     []HeldLock    `json:"held"`
	Waiting  []WaitingLock `json:"waiting"`
	Counters LockCounters  `json:"counters"`
}

// LockView is returned by a successful acquisition or renewal.
type LockView struct {
	HeldLock
	Renewed  bool  `json:"renewed"`
	WaitedMS int64 `json:"waited_ms"`
}

// txnLock records the token a transaction expects to still own at commit.
type txnLock struct {
	id       string
	relation string
	key      datum.Value
	token    uint64
}

type lockManager struct {
	mu        sync.Mutex
	locks     map[string]*heldLock
	waits     map[string]*waitRecord
	queues    map[string][]string
	counters  LockCounters
	nextToken uint64
	notify    chan struct{}
}

func newLockManager() *lockManager {
	return &lockManager{
		locks:  map[string]*heldLock{},
		waits:  map[string]*waitRecord{},
		queues: map[string][]string{},
		notify: make(chan struct{}),
	}
}

func lockID(relation string, key datum.Value) string {
	return relation + "/" + key.Key()
}

func rowName(relation string, key datum.Value) string {
	return relation + "/" + key.Key()
}

func (m *lockManager) broadcastLocked() {
	close(m.notify)
	m.notify = make(chan struct{})
}

func (m *lockManager) waitChannel() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.notify
}

func (m *lockManager) expire(id string, token uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.locks[id]
	if !ok || entry.token != token {
		return
	}
	delete(m.locks, id)
	m.counters.Expired++
	m.broadcastLocked()
}

// tryAcquire is non-blocking and must be called while the engine lock is held.
// The blocker string names the lock holder, or the earlier waiter when the
// caller must join the fair queue.
func (m *lockManager) tryAcquire(txID, relation string, key datum.Value, lease time.Duration, now time.Time) (HeldLock, uint64, string, bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := lockID(relation, key)
	if entry, ok := m.locks[id]; ok {
		if entry.owner != txID {
			return HeldLock{}, 0, entry.owner, false, false
		}
		if entry.timer != nil {
			entry.timer.Stop()
		}
		m.nextToken++
		entry.token = m.nextToken
		entry.lease = lease
		entry.expires = now.Add(lease)
		token := entry.token
		entry.timer = time.AfterFunc(lease, func() { m.expire(id, token) })
		m.counters.Renewed++
		return m.viewLocked(entry), token, "", true, false
	}
	if queue := m.queues[id]; len(queue) > 0 && queue[0] != txID {
		return HeldLock{}, 0, queue[0], false, false
	}
	if len(m.locks) >= maxTotalLocks {
		return HeldLock{}, 0, "", false, true
	}
	if queue := m.queues[id]; len(queue) > 0 {
		m.queues[id] = queue[1:]
		if len(m.queues[id]) == 0 {
			delete(m.queues, id)
		}
	}
	m.nextToken++
	entry := &heldLock{token: m.nextToken, owner: txID, relation: relation, key: key, lease: lease, acquired: now, expires: now.Add(lease)}
	token := entry.token
	entry.timer = time.AfterFunc(lease, func() { m.expire(id, token) })
	m.locks[id] = entry
	m.counters.Acquired++
	return m.viewLocked(entry), token, "", true, false
}

func (m *lockManager) viewLocked(entry *heldLock) HeldLock {
	return HeldLock{
		ID:          lockID(entry.relation, entry.key),
		Relation:    entry.relation,
		Key:         entry.key,
		Transaction: entry.owner,
		LeaseMS:     int(entry.lease / time.Millisecond),
		AcquiredAt:  entry.acquired.UTC().Format(time.RFC3339Nano),
		ExpiresAt:   entry.expires.UTC().Format(time.RFC3339Nano),
	}
}

func (m *lockManager) release(id, txID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.releaseLocked(id, txID)
}

func (m *lockManager) releaseLocked(id, txID string) bool {
	entry, ok := m.locks[id]
	if !ok || entry.owner != txID {
		return false
	}
	if entry.timer != nil {
		entry.timer.Stop()
	}
	delete(m.locks, id)
	m.counters.Released++
	m.broadcastLocked()
	return true
}

func (m *lockManager) releaseAll(txID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	released := 0
	for id, entry := range m.locks {
		if entry.owner != txID {
			continue
		}
		if entry.timer != nil {
			entry.timer.Stop()
		}
		delete(m.locks, id)
		released++
	}
	m.counters.Released += uint64(released)
	removedWait := false
	if rec, ok := m.waits[txID]; ok {
		m.removeWaitLocked(txID, rec)
		removedWait = true
	}
	if released > 0 || removedWait {
		m.broadcastLocked()
	}
	return released
}

func (m *lockManager) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, entry := range m.locks {
		if entry.timer != nil {
			entry.timer.Stop()
		}
	}
	m.locks = map[string]*heldLock{}
	m.waits = map[string]*waitRecord{}
	m.queues = map[string][]string{}
	m.broadcastLocked()
}

func (m *lockManager) ownedBy(id string, token uint64, txID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.locks[id]
	return ok && entry.owner == txID && entry.token == token
}

// registerWait returns errCanceledWait if a prior cancellation is still pending.
func (m *lockManager) registerWait(txID, relation string, key datum.Value, now time.Time) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := lockID(relation, key)
	if rec, ok := m.waits[txID]; ok {
		if rec.id != id {
			return false, fmt.Errorf("transaction already waits for another row")
		}
		if rec.canceled {
			return false, errCanceledWait
		}
		return false, nil
	}
	m.queues[id] = append(m.queues[id], txID)
	m.waits[txID] = &waitRecord{id: id, relation: relation, key: key, since: now}
	m.counters.Waited++
	return true, nil
}

func (m *lockManager) removeWaitLocked(txID string, rec *waitRecord) {
	delete(m.waits, txID)
	queue := m.queues[rec.id]
	for i, queued := range queue {
		if queued == txID {
			queue = append(queue[:i], queue[i+1:]...)
			break
		}
	}
	if len(queue) == 0 {
		delete(m.queues, rec.id)
	} else {
		m.queues[rec.id] = queue
	}
}

func (m *lockManager) clearWait(txID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rec, ok := m.waits[txID]; ok {
		m.removeWaitLocked(txID, rec)
		m.broadcastLocked()
	}
}

func (m *lockManager) waitCanceled(txID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.waits[txID]
	return ok && rec.canceled
}

func (m *lockManager) cancelWait(txID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.waits[txID]
	if !ok || rec.canceled {
		return false
	}
	rec.canceled = true
	m.counters.Canceled++
	m.broadcastLocked()
	return true
}

// blockerLocked names the transaction whose progress the waiter depends on:
// the current holder, else the immediate earlier waiter in the fair queue.
func (m *lockManager) blockerLocked(id, txID string) string {
	if entry, ok := m.locks[id]; ok {
		if entry.owner != txID {
			return entry.owner
		}
		return ""
	}
	seenSelf := false
	for _, queued := range m.queues[id] {
		if queued == txID {
			seenSelf = true
			continue
		}
		if !seenSelf {
			return queued
		}
	}
	return ""
}

// waitDeadlock reports whether the waiting transaction is part of a wait cycle.
func (m *lockManager) waitDeadlock(txID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	visited := map[string]bool{txID: true}
	current := txID
	for {
		rec, ok := m.waits[current]
		if !ok {
			return false
		}
		blocker := m.blockerLocked(rec.id, current)
		if blocker == "" {
			return false
		}
		if blocker == txID {
			return true
		}
		if visited[blocker] {
			return false
		}
		visited[blocker] = true
		current = blocker
	}
}

func (m *lockManager) countTimedOut() {
	m.mu.Lock()
	m.counters.TimedOut++
	m.mu.Unlock()
}

func (m *lockManager) countCanceled() {
	m.mu.Lock()
	m.counters.Canceled++
	m.mu.Unlock()
}

func (m *lockManager) countAborted() {
	m.mu.Lock()
	m.counters.Aborted++
	m.mu.Unlock()
}

func (m *lockManager) countDeadlock() {
	m.mu.Lock()
	m.counters.Deadlocks++
	m.mu.Unlock()
}

func (m *lockManager) snapshot() LockSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	out := LockSnapshot{Held: []HeldLock{}, Waiting: []WaitingLock{}, Counters: m.counters}
	for _, entry := range m.locks {
		out.Held = append(out.Held, m.viewLocked(entry))
	}
	sort.Slice(out.Held, func(i, j int) bool { return out.Held[i].ID < out.Held[j].ID })
	for txID, rec := range m.waits {
		wait := WaitingLock{ID: rec.id, Relation: rec.relation, Key: rec.key, Transaction: txID, Holder: m.blockerLocked(rec.id, txID), WaitedMS: now.Sub(rec.since).Milliseconds()}
		out.Waiting = append(out.Waiting, wait)
	}
	sort.Slice(out.Waiting, func(i, j int) bool {
		if out.Waiting[i].ID == out.Waiting[j].ID {
			return out.Waiting[i].Transaction < out.Waiting[j].Transaction
		}
		return out.Waiting[i].ID < out.Waiting[j].ID
	})
	return out
}

func (m *lockManager) occupancy() (held, waiting int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.locks), len(m.waits)
}

func (m *lockManager) countersCopy() LockCounters {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters
}

func (m *lockManager) holderOfID(id string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.locks[id]
	if !ok {
		return "", false
	}
	return entry.owner, true
}

// liveLockCount counts recorded qualifications whose token still names this
// owner. Lapsed entries stay recorded so a later write or commit reports
// lock_expired instead of silently becoming an ordinary write. It is called
// with the engine mutex held.
func (d *DB) liveLockCount(tx *Transaction) int {
	count := 0
	for _, recorded := range tx.locks {
		if d.locks.ownedBy(recorded.id, recorded.token, tx.ID) {
			count++
		}
	}
	return count
}

// checkRowWriteAllowed is called with the engine mutex held. It makes the
// qualification binding for every write path, including callers that never
// use the explicit lock endpoint.
func (d *DB) checkRowWriteAllowed(tx *Transaction, relation, key string) error {
	id := relation + "/" + key
	if recorded, ok := tx.locks[id]; ok {
		if !d.locks.ownedBy(recorded.id, recorded.token, tx.ID) {
			delete(tx.locks, id)
			return fault("lock_expired", "row lock for %s expired before the write; re-acquire it or retry in a new transaction", id)
		}
		latest := d.store.Latest(relation, key)
		if latest > tx.Snapshot {
			return fault("conflict", "row %s changed at sequence %d after transaction snapshot %d; rollback and retry in a new transaction", id, latest, tx.Snapshot)
		}
		return nil
	}
	if holder, held := d.locks.holderOfID(id); held && holder != tx.ID {
		return fault("locked", "row %s is locked by transaction %s; acquire the row lock before writing", id, holder)
	}
	return nil
}

// checkLocksAtCommit verifies that qualifications covering this transaction's
// writes are still live at the moment of publication. A lapsed lease is a
// distinct failure, never a silent acceptance of a stale snapshot.
func (d *DB) checkLocksAtCommit(tx *Transaction) error {
	for table, mutations := range tx.writes {
		for key := range mutations {
			id := table + "/" + key
			if recorded, ok := tx.locks[id]; ok {
				if !d.locks.ownedBy(recorded.id, recorded.token, tx.ID) {
					delete(tx.locks, id)
					return fault("lock_expired", "row lock for %s lapsed before commit; no writes were published", id)
				}
				latest := d.store.Latest(table, key)
				if latest > tx.Snapshot {
					return fault("conflict", "row %s changed at sequence %d after transaction snapshot %d; no writes were published", id, latest, tx.Snapshot)
				}
				continue
			}
			if holder, held := d.locks.holderOfID(id); held && holder != tx.ID {
				return fault("locked", "row %s is locked by transaction %s; no writes were published", id, holder)
			}
		}
	}
	return nil
}

// AcquireLock obtains a bounded, lease-backed exclusive qualification on one
// row key. The engine lock is not held while waiting, so unrelated rows and
// transactions continue to make progress.
func (d *DB) AcquireLock(ctx context.Context, id, relation string, key datum.Value, waitMS, leaseMS int) (LockView, error) {
	if leaseMS == 0 {
		leaseMS = defaultLeaseMS
	}
	if leaseMS < minLeaseMS || leaseMS > maxLeaseMS {
		return LockView{}, fault("invalid", "lease_ms must be %d..%d", minLeaseMS, maxLeaseMS)
	}
	if waitMS < 0 || waitMS > maxWaitMS {
		return LockView{}, fault("invalid", "wait_ms must be 0..%d", maxWaitMS)
	}
	started := time.Now()
	deadline := started.Add(time.Duration(waitMS) * time.Millisecond)
	lease := time.Duration(leaseMS) * time.Millisecond
	var queued bool
	defer func() {
		if queued {
			d.locks.clearWait(id)
		}
	}()
	for {
		d.mu.Lock()
		if err := d.usable(); err != nil {
			d.mu.Unlock()
			if queued {
				d.locks.countAborted()
			}
			return LockView{}, err
		}
		tx, err := d.tx(id)
		if err != nil {
			d.mu.Unlock()
			if queued {
				d.locks.countAborted()
			}
			return LockView{}, err
		}
		if tx.ReadOnly {
			d.mu.Unlock()
			return LockView{}, fault("conflict", "read-only transaction cannot acquire row locks")
		}
		r, err := d.catalog.Relation(relation)
		if err != nil {
			d.mu.Unlock()
			return LockView{}, invalid(err)
		}
		if r.Created > tx.Snapshot {
			d.mu.Unlock()
			return LockView{}, fault("conflict", "relation outside transaction catalog snapshot")
		}
		column, _ := r.Column(r.Primary)
		if key.Null() || key.Kind != column.Type {
			d.mu.Unlock()
			return LockView{}, fault("invalid", "invalid primary value")
		}
		requested := lockID(relation, key)
		if _, exists := tx.locks[requested]; !exists && d.liveLockCount(tx) >= maxLocksPerTx {
			d.mu.Unlock()
			return LockView{}, fault("capacity", "transaction row lock limit %d", maxLocksPerTx)
		}
		if queued && d.locks.waitCanceled(id) {
			d.locks.clearWait(id)
			queued = false
			d.mu.Unlock()
			return LockView{}, fault("canceled", "lock wait canceled by the transaction owner")
		}
		channel := d.locks.waitChannel()
		now := time.Now()
		held, token, holder, acquired, full := d.locks.tryAcquire(id, relation, key, lease, now)
		if acquired {
			latest := d.store.Latest(relation, key.Key())
			if latest > tx.Snapshot {
				d.locks.release(requested, id)
				delete(tx.locks, requested)
				d.locks.clearWait(id)
				queued = false
				d.mu.Unlock()
				return LockView{}, fault("conflict", "row %s changed at sequence %d after transaction snapshot %d; rollback and retry in a new transaction", rowName(relation, key), latest, tx.Snapshot)
			}
			if tx.locks == nil {
				tx.locks = map[string]txnLock{}
			}
			_, renewed := tx.locks[requested]
			tx.locks[requested] = txnLock{id: requested, relation: relation, key: key, token: token}
			d.locks.clearWait(id)
			queued = false
			d.mu.Unlock()
			return LockView{HeldLock: held, Renewed: renewed, WaitedMS: time.Since(started).Milliseconds()}, nil
		}
		if full {
			d.mu.Unlock()
			return LockView{}, fault("capacity", "%d row locks already held", maxTotalLocks)
		}
		if waitMS == 0 {
			d.mu.Unlock()
			return LockView{}, fault("locked", "row %s is locked by transaction %s; use wait_ms to queue", rowName(relation, key), holder)
		}
		if time.Now().After(deadline) {
			d.locks.clearWait(id)
			queued = false
			d.locks.countTimedOut()
			d.mu.Unlock()
			return LockView{}, fault("locked", "lock wait for %s timed out after %dms; current holder %s", rowName(relation, key), time.Since(started).Milliseconds(), holder)
		}
		first, err := d.locks.registerWait(id, relation, key, now)
		if err != nil {
			d.mu.Unlock()
			if errors.Is(err, errCanceledWait) {
				d.locks.clearWait(id)
				queued = false
				return LockView{}, fault("canceled", "lock wait canceled by the transaction owner")
			}
			return LockView{}, fault("conflict", "%s", err)
		}
		queued = true
		if first && d.locks.waitDeadlock(id) {
			d.locks.clearWait(id)
			queued = false
			d.locks.countDeadlock()
			d.mu.Unlock()
			return LockView{}, fault("deadlock", "row lock wait cycle detected for %s; release a held lock or roll back", rowName(relation, key))
		}
		d.mu.Unlock()
		remaining := time.Until(deadline)
		if remaining < 0 {
			remaining = 0
		}
		timer := time.NewTimer(remaining)
		select {
		case <-channel:
			timer.Stop()
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			d.locks.clearWait(id)
			queued = false
			d.locks.countCanceled()
			return LockView{}, fault("canceled", "lock wait canceled by the caller after %dms", time.Since(started).Milliseconds())
		}
	}
}

// LockRequest names one row qualification for a multi-row acquisition.
type LockRequest struct {
	Relation string      `json:"relation"`
	Key      datum.Value `json:"key"`
}

// AcquireLocks takes several row qualifications in canonical (relation, key)
// order under one shared wait deadline. Keys already held by the transaction
// are renewed but never released on failure; keys acquired by this call are
// released if a later key cannot be obtained, so a failed call publishes no
// new qualification and no partial multi-row edit.
func (d *DB) AcquireLocks(ctx context.Context, id string, requests []LockRequest, waitMS, leaseMS int) ([]LockView, error) {
	if len(requests) == 0 {
		return nil, fault("invalid", "at least one row lock request required")
	}
	if len(requests) > maxLocksPerTx {
		return nil, fault("capacity", "at most %d row lock requests per call", maxLocksPerTx)
	}
	unique := map[string]LockRequest{}
	for _, request := range requests {
		if request.Key.Null() {
			return nil, fault("invalid", "invalid primary value")
		}
		unique[lockID(request.Relation, request.Key)] = request
	}
	ordered := make([]LockRequest, 0, len(unique))
	for _, request := range unique {
		ordered = append(ordered, request)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Relation != ordered[j].Relation {
			return ordered[i].Relation < ordered[j].Relation
		}
		return ordered[i].Key.Key() < ordered[j].Key.Key()
	})
	deadline := time.Now().Add(time.Duration(waitMS) * time.Millisecond)
	out := make([]LockView, 0, len(ordered))
	fresh := make([]LockRequest, 0, len(ordered))
	for _, request := range ordered {
		remaining := int(time.Until(deadline).Milliseconds())
		if remaining < 0 {
			remaining = 0
		}
		view, err := d.AcquireLock(ctx, id, request.Relation, request.Key, remaining, leaseMS)
		if err != nil {
			for _, acquired := range fresh {
				_, _ = d.ReleaseLock(id, acquired.Relation, acquired.Key, false)
			}
			return nil, err
		}
		if !view.Renewed {
			fresh = append(fresh, request)
		}
		out = append(out, view)
	}
	return out, nil
}

// ReleaseLock drops one qualification or all qualifications of a transaction.
// Explicit release is idempotent; a later commit is then an ordinary commit.
func (d *DB) ReleaseLock(id, relation string, key datum.Value, all bool) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return 0, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return 0, err
	}
	if all {
		released := len(tx.locks)
		for heldID := range tx.locks {
			d.locks.release(heldID, id)
		}
		tx.locks = map[string]txnLock{}
		return released, nil
	}
	r, err := d.catalog.Relation(relation)
	if err != nil {
		return 0, invalid(err)
	}
	column, _ := r.Column(r.Primary)
	if key.Null() || key.Kind != column.Type {
		return 0, fault("invalid", "invalid primary value")
	}
	requested := lockID(relation, key)
	if _, exists := tx.locks[requested]; !exists {
		return 0, nil
	}
	delete(tx.locks, requested)
	if d.locks.release(requested, id) {
		return 1, nil
	}
	return 0, nil
}

// CancelLockWait marks a pending bounded wait for user cancellation. The
// waiting request returns the distinct "canceled" outcome and retains no lease.
func (d *DB) CancelLockWait(id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return false, err
	}
	if _, err := d.tx(id); err != nil {
		return false, err
	}
	return d.locks.cancelWait(id), nil
}

// Locks reports currently held qualifications and bounded waits.
func (d *DB) Locks() (LockSnapshot, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return LockSnapshot{}, err
	}
	return d.locks.snapshot(), nil
}
