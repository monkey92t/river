package joblease

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivertype"
)

// Manager renews this client's active executions independently of user Work.
// Each Guard owns its watchdog, so blocked database I/O cannot delay cancellation.
type Manager struct {
	baseservice.BaseService
	startstop.BaseStartStop

	config    *Config
	renewFunc func(context.Context, *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error)
	schema    string

	blocked atomic.Bool
	failed  atomic.Bool
	mu      sync.Mutex
	entries map[int64]*managerEntry
}

// NewManager constructs a manager without starting renewal goroutines.
func NewManager(archetype *baseservice.Archetype, config *Config, schema string, renewFunc func(context.Context, *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error)) *Manager {
	return baseservice.Init(archetype, &Manager{
		config:    config,
		renewFunc: renewFunc,
		schema:    schema,
		entries:   make(map[int64]*managerEntry),
	})
}

// CanFetch pauses acquisition while renewals fail or a cancelled Work won't exit.
func (m *Manager) CanFetch() bool {
	if m == nil {
		return true
	}
	return !m.blocked.Load() && !m.failed.Load()
}

// Register begins local supervision using the request's monotonic send time.
// The database's claim record is retained for renewal, never as a local clock.
// Conflicting claim records are checked against the database, never ordered by
// their timestamps or retry counts, before replacing an existing local guard.
// Conflict checks inherit the execution context and are bounded by RenewTimeout.
func (m *Manager) Register(ctx context.Context, job *rivertype.JobRow, sentAt time.Time, cancel context.CancelCauseFunc) *Guard {
	guard := NewGuard(sentAt.Add(m.config.LeaseDuration-m.config.LocalSafetyMargin), cancel)
	if job.LeaseExpiresAt == nil || job.AttemptedAt == nil {
		// A pilot must return both the acquired lease and its claim record.
		guard.Abandon()
	} else {
		m.register(ctx, &managerEntry{id: job.ID, attempt: job.Attempt, attemptedAt: *job.AttemptedAt, guard: guard})
	}
	go func() {
		<-guard.Done()
		m.mu.Lock()
		if entry := m.entries[job.ID]; entry != nil && entry.guard == guard {
			delete(m.entries, job.ID)
		}
		if len(m.entries) == 0 {
			// Let normal acquisition probe database recovery when nothing remains.
			m.failed.Store(false)
		}
		m.mu.Unlock()
		if guard.Finished() {
			return
		}
		timer := time.NewTimer(m.config.CancelGracePeriod)
		defer timer.Stop()
		select {
		case <-guard.WorkReturned():
		case <-timer.C:
			m.blocked.Store(true)
			m.Logger.Error("Job lease lost but Work did not exit; stopping new acquisition until client restart", slog.Int64("job_id", job.ID))
		}
	}()
	return guard
}

// Start keeps renewing until explicitly stopped after producers and completion.
func (m *Manager) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := m.StartInit(ctx)
	if !shouldStart {
		return nil
	}
	m.blocked.Store(false)
	m.failed.Store(false)
	go func() {
		defer stopped()
		started()
		timer := time.NewTimer(m.config.HeartbeatInterval)
		defer timer.Stop()
		backoff := time.Second
		for {
			select {
			case <-ctx.Done():
				m.mu.Lock()
				guards := make([]*Guard, 0, len(m.entries))
				for _, entry := range m.entries {
					guards = append(guards, entry.guard)
				}
				m.mu.Unlock()
				for _, guard := range guards {
					guard.Abandon()
				}
				return
			case <-timer.C:
				delay := m.config.HeartbeatInterval
				if err := m.renew(ctx); err != nil {
					m.Logger.WarnContext(ctx, "Job lease renewal failed", slog.String("error", err.Error()))
					delay = min(backoff, m.config.HeartbeatInterval)
					backoff = min(backoff*2, 8*time.Second)
				} else {
					backoff = time.Second
				}
				timer.Reset(delay)
			}
		}
	}()
	return nil
}

// confirmClaim uses the existing fenced renewal to validate a conflicting claim.
// The request is bounded and cannot revive a guard that expired while waiting.
func (m *Manager) confirmClaim(ctx context.Context, entry *managerEntry) bool {
	ctx, release, err := entry.guard.Begin(ctx)
	if err != nil {
		return false
	}
	defer release()
	sentAt := time.Now()
	rows, err := m.renewFunc(ctx, &riverdriver.JobRenewLeaseManyParams{
		IDs: []int64{entry.id}, AttemptedAt: []time.Time{entry.attemptedAt}, Attempts: []int{entry.attempt},
		LeaseDuration: m.config.LeaseDuration, Schema: m.schema,
	})
	if err != nil {
		m.failed.Store(true)
		m.Logger.WarnContext(ctx, "Job lease claim confirmation failed", slog.Int64("job_id", entry.id), slog.String("error", err.Error()))
		return false
	}
	if ctx.Err() != nil {
		return false
	}
	for _, row := range rows {
		if row.ID == entry.id && row.LeaseExpiresAt != nil {
			return entry.guard.Renew(sentAt.Add(m.config.LeaseDuration - m.config.LocalSafetyMargin))
		}
	}
	return false
}

// register replaces an overlapping guard only after its incoming claim is confirmed.
// Confirmation runs without the manager lock so other jobs can keep renewing.
func (m *Manager) register(ctx context.Context, entry *managerEntry) {
	if ctx.Err() != nil {
		entry.guard.Abandon()
		return
	}
	m.mu.Lock()
	previous := m.entries[entry.id]
	if previous == nil {
		m.entries[entry.id] = entry
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()
	// One budget bounds the entire registration, including rechecks after
	// another registration wins the local slot while a reply is delayed.
	ctx, cancel := context.WithTimeout(ctx, m.config.RenewTimeout)
	defer cancel()
	for {
		if previous != nil && previous.attempt == entry.attempt && previous.attemptedAt.Equal(entry.attemptedAt) {
			entry.guard.Abandon()
			return
		}
		if ctx.Err() != nil || !m.confirmClaim(ctx, entry) {
			entry.guard.Abandon()
			return
		}
		m.mu.Lock()
		if m.entries[entry.id] != previous {
			previous = m.entries[entry.id]
			m.mu.Unlock()
			continue
		}
		m.entries[entry.id] = entry
		m.mu.Unlock()
		if previous != nil {
			previous.guard.Abandon()
		}
		return
	}
}

// renew performs one bounded batch owned by the client, not any single member.
// A member lost after admission may receive one last database renewal, but its
// guard cannot revive. Failed or ambiguous requests never extend guards.
func (m *Manager) renew(ctx context.Context) error {
	m.mu.Lock()
	entries := make([]managerEntry, 0, len(m.entries))
	for _, entry := range m.entries {
		if entry.guard.Check() == nil {
			entries = append(entries, *entry)
		}
	}
	m.mu.Unlock()
	if len(entries) == 0 {
		m.failed.Store(false)
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, m.config.RenewTimeout)
	defer cancel()
	params := &riverdriver.JobRenewLeaseManyParams{LeaseDuration: m.config.LeaseDuration, Schema: m.schema}
	active := entries[:0]
	for _, entry := range entries {
		if entry.guard.Check() != nil {
			continue
		}
		active = append(active, entry)
		params.IDs = append(params.IDs, entry.id)
		params.AttemptedAt = append(params.AttemptedAt, entry.attemptedAt)
		params.Attempts = append(params.Attempts, entry.attempt)
	}
	if len(active) == 0 {
		return nil
	}
	sentAt := time.Now()
	rows, err := m.renewFunc(ctx, params)
	if err != nil {
		m.failed.Store(true)
		return err
	}
	m.failed.Store(false)
	byID := make(map[int64]*rivertype.JobRow, len(rows))
	for _, row := range rows {
		byID[row.ID] = row
	}
	for _, entry := range active {
		row := byID[entry.id]
		if row == nil || row.LeaseExpiresAt == nil {
			// Completion can race renewal. Keep the confirmed deadline while its
			// result arrives; a missing renewal never grants additional time.
			continue
		}
		// Receipts contain only IDs, so bind them to this request's captured
		// guard, never to another claim registered while the request was in flight.
		m.mu.Lock()
		if current := m.entries[entry.id]; current != nil && current.guard == entry.guard {
			entry.guard.Renew(sentAt.Add(m.config.LeaseDuration - m.config.LocalSafetyMargin))
		}
		m.mu.Unlock()
	}
	return nil
}

// managerEntry tracks a single execution, never merely a reusable job ID.
type managerEntry struct {
	id          int64
	attempt     int
	attemptedAt time.Time
	guard       *Guard
}
