// Package joblease coordinates the lifetime of locally executing job leases.
package joblease

import (
	"context"
	"sync"
	"time"

	"github.com/riverqueue/river/rivertype"
)

type contextKey struct{}

// Guard closes admission to local operations when a confirmed lease expires.
// It cannot retract operations already sent to the database.
type Guard struct {
	cancelWork context.CancelCauseFunc
	deadline   time.Time
	finalizing bool
	finished   bool
	limit      time.Time
	mu         sync.Mutex
	signal     context.Context //nolint:containedctx // Owns a cancellation signal, never a caller's request context.
	stop       context.CancelCauseFunc
	timer      *time.Timer
	workDone   chan struct{}
}

// NewGuard starts an independent watchdog using the local monotonic deadline.
func NewGuard(deadline time.Time, cancelWork context.CancelCauseFunc) *Guard {
	signal, stop := context.WithCancelCause(context.Background())
	g := &Guard{cancelWork: cancelWork, deadline: deadline, signal: signal, stop: stop, workDone: make(chan struct{})}
	g.mu.Lock()
	g.timer = time.AfterFunc(time.Until(deadline), g.expire)
	g.mu.Unlock()
	return g
}

// Abandon irreversibly closes admission and cancels the worker and requests.
func (g *Guard) Abandon() {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.abandonLocked()
}

// abandonLocked transitions an unfinished execution to lease loss.
func (g *Guard) abandonLocked() {
	if g.signal.Err() != nil {
		return
	}
	g.stop(rivertype.ErrJobLeaseLost)
	g.timer.Stop()
	if g.cancelWork != nil {
		g.cancelWork(rivertype.ErrJobLeaseLost)
	}
}

// Begin admits an operation and binds its context to this guard's lifetime.
// Callers must release the returned cleanup function after the operation.
func (g *Guard) Begin(ctx context.Context) (context.Context, func(), error) {
	if g == nil {
		return ctx, func() {}, nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkLocked(); err != nil {
		return nil, nil, err
	}
	requestCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(g.signal, func() { cancel(context.Cause(g.signal)) })
	return requestCtx, func() { stop(); cancel(context.Canceled) }, nil
}

// CancelAfter caps the time an uncooperative worker may continue renewing.
func (g *Guard) CancelAfter(grace time.Duration) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.checkLocked() != nil || g.finalizing {
		return
	}
	g.limitLocked(time.Now().Add(grace))
}

// Check rejects expired, abandoned, or already finalized executions.
func (g *Guard) Check() error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.checkLocked()
}

// checkLocked checks elapsed time even if the watchdog has not run yet.
func (g *Guard) checkLocked() error {
	if g.signal.Err() == nil && !time.Now().Before(g.deadlineLocked()) {
		g.abandonLocked()
	}
	return context.Cause(g.signal)
}

// Deadline returns the effective local stop deadline.
func (g *Guard) Deadline() time.Time {
	if g == nil {
		return time.Time{}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.deadlineLocked()
}

// deadlineLocked includes the fixed cancellation or finalization budget.
func (g *Guard) deadlineLocked() time.Time {
	if !g.limit.IsZero() && g.limit.Before(g.deadline) {
		return g.limit
	}
	return g.deadline
}

// Done closes after abandonment or confirmed completion; a nil guard never ends.
func (g *Guard) Done() <-chan struct{} {
	if g == nil {
		return nil
	}
	return g.signal.Done()
}

// expire is independent of renewal I/O and rechecks reset deadlines under lock.
func (g *Guard) expire() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.checkLocked() == nil {
		g.timer.Reset(time.Until(g.deadlineLocked()))
	}
}

// Finalize records that Work returned and bounds pending result confirmation.
func (g *Guard) Finalize(timeout time.Duration) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.finalizing {
		g.finalizing = true
		close(g.workDone)
	}
	if g.checkLocked() == nil {
		g.limitLocked(time.Now().Add(timeout))
	}
}

// Finish records a confirmed database result without reviving lost executions.
func (g *Guard) Finish() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.checkLocked() != nil {
		return false
	}
	g.finished = true
	g.stop(context.Canceled)
	g.timer.Stop()
	return true
}

// Finished distinguishes confirmation from abandonment when Done is closed.
func (g *Guard) Finished() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.finished
}

// limitLocked only shortens the fixed budget, including across renewal.
func (g *Guard) limitLocked(deadline time.Time) {
	if g.limit.IsZero() || deadline.Before(g.limit) {
		g.limit = deadline
	}
	g.timer.Reset(time.Until(g.deadlineLocked()))
}

// Renew accepts a confirmed renewal only before both old and new deadlines.
func (g *Guard) Renew(deadline time.Time) bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.checkLocked() != nil {
		return false
	}
	if !time.Now().Before(deadline) {
		g.abandonLocked()
		return false
	}
	g.deadline = deadline
	g.timer.Reset(time.Until(g.deadlineLocked()))
	return true
}

// WorkReturned closes when the executor returns from business execution.
func (g *Guard) WorkReturned() <-chan struct{} {
	if g == nil {
		return nil
	}
	return g.workDone
}

// FromContext returns the local execution guard, or nil for legacy executions.
func FromContext(ctx context.Context) *Guard {
	guard, _ := ctx.Value(contextKey{}).(*Guard)
	return guard
}

// WithGuard carries an execution guard through worker and completion contexts.
func WithGuard(ctx context.Context, guard *Guard) context.Context {
	return context.WithValue(ctx, contextKey{}, guard)
}
