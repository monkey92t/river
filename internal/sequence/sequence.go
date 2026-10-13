// Package sequence coordinates durable ordered jobs independently of job
// execution. Protocol calls use caller-owned transactions; the background
// service owns its bounded reconciliation transactions.
package sequence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivertype"
)

const (
	// A fixed bound prevents a database full of bad groups from growing memory.
	maxFailureEntries = 1024
	maxHints          = 64
)

// Config limits how much shared database capacity the coordinator can consume.
// There is exactly one transaction in flight per service.
type Config struct {
	BatchSize          int
	PollInterval       time.Duration
	Schema             string
	TransactionTimeout time.Duration
}

// Status reports coordinator health without treating database errors as client
// startup failures. Counters describe committed coordination, not worker output.
type Status struct {
	ConsecutiveErrors int
	GroupsReconciled  uint64
	GroupsRepaired    uint64
	JobsBypassed      uint64
	JobsReleased      uint64
	JobsResolved      uint64
	LastError         string
	LastErrorAt       time.Time
	LastSuccessAt     time.Time
}

// Service discovers work from durable due scans, fair keyset sweeps and orphan
// scans. Optional hints only improve latency and may be dropped safely.
type Service struct {
	baseservice.BaseService
	startstop.BaseStartStop

	Config  *Config
	exec    riverdriver.Executor
	seqExec riverdriver.SequenceExecutor

	dueCursor    string
	failures     map[string]failure
	hints        chan string
	nextSweep    time.Time
	orphanCursor string
	scanCursor   string

	mu     sync.RWMutex
	status Status
}

// New constructs a coordinator without touching the database. Unsupported
// executors are harmless when started, although callers should omit the service.
func New(archetype *baseservice.Archetype, config *Config, exec riverdriver.Executor) *Service {
	configCopy := *config
	if configCopy.BatchSize == 0 {
		configCopy.BatchSize = 32
	}
	if configCopy.PollInterval == 0 {
		configCopy.PollInterval = time.Second
	}
	if configCopy.TransactionTimeout == 0 {
		configCopy.TransactionTimeout = 2 * time.Second
	}
	configCopy.BatchSize = min(max(configCopy.BatchSize, 1), 256)
	configCopy.PollInterval = min(max(configCopy.PollInterval, 10*time.Millisecond), time.Minute)
	configCopy.TransactionTimeout = min(max(configCopy.TransactionTimeout, 100*time.Millisecond), 10*time.Second)
	seqExec, _ := exec.(riverdriver.SequenceExecutor)
	return baseservice.Init(archetype, &Service{
		Config:   &configCopy,
		exec:     exec,
		failures: make(map[string]failure),
		hints:    make(chan string, maxHints),
		seqExec:  seqExec,
	})
}

// Hint requests an early group check without blocking a caller or its commit.
func (s *Service) Hint(key string) {
	select {
	case s.hints <- key:
	default:
	}
}

// Start only starts a goroutine. Operational database failures are isolated
// inside the loop and never stop workers or unrelated maintenance services.
func (s *Service) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}
	go func() {
		defer stopped()
		started()
		if s.seqExec == nil {
			<-ctx.Done()
			return
		}
		ticker := time.NewTicker(s.Config.PollInterval)
		defer ticker.Stop()
		var nextScan time.Time
		consecutiveScanErrors := 0
		for {
			if !time.Now().Before(nextScan) {
				if err := s.runOnce(ctx); err != nil {
					if ctx.Err() != nil {
						return
					}
					consecutiveScanErrors++
					nextScan = time.Now().Add(backoff(consecutiveScanErrors))
					s.recordError(err)
					s.Logger.ErrorContext(ctx, s.Name+": scan failed; ordinary job processing continues", slog.String("error", err.Error()))
				} else {
					consecutiveScanErrors = 0
					nextScan = time.Now().Add(s.Config.PollInterval)
				}
			}
			select {
			case <-ctx.Done():
				return
			case key := <-s.hints:
				if !time.Now().Before(nextScan) || consecutiveScanErrors == 0 {
					s.process(ctx, key, false)
				}
			case <-ticker.C:
			}
		}
	}()
	return nil
}

// Status returns a race-safe copy of current health and cumulative work.
func (s *Service) Status() Status {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

// notifyReleased is strictly after commit and never changes a successful result.
func (s *Service) notifyReleased(ctx context.Context, job *rivertype.JobRow) {
	if job == nil || job.State != rivertype.JobStateAvailable {
		return
	}
	payload, err := json.Marshal(map[string]string{"queue": job.Queue})
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, s.Config.TransactionTimeout)
	defer cancel()
	if err := s.exec.NotifyMany(ctx, &riverdriver.NotifyManyParams{Payload: []string{string(payload)}, Schema: s.Config.Schema, Topic: string(notifier.NotificationTopicInsert)}); err != nil && ctx.Err() == nil {
		s.Logger.WarnContext(ctx, s.Name+": committed release notification failed; fetch polling will recover", slog.String("error", err.Error()))
	}
}

// process contains failure to one key. A poisoned first page cannot starve later
// keys because every discovery source uses a cursor independent of success.
func (s *Service) process(ctx context.Context, key string, repair bool) {
	if ctx.Err() != nil {
		return
	}
	if entry, ok := s.failures[key]; ok && time.Now().Before(entry.until) {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, s.Config.TransactionTimeout)
	defer cancel()
	result, err := s.transact(ctx, key, repair)
	if err != nil {
		if ctx.Err() == context.Canceled {
			return
		}
		entry := s.failures[key]
		entry.count++
		entry.until = time.Now().Add(backoff(entry.count))
		if len(s.failures) >= maxFailureEntries {
			for oldKey := range s.failures {
				delete(s.failures, oldKey)
				break
			}
		}
		s.failures[key] = entry
		s.recordError(err)
		s.Logger.WarnContext(ctx, s.Name+": group reconciliation failed", slog.String("sequence_key", key), slog.String("error", err.Error()))
		return
	}
	delete(s.failures, key)
	s.mu.Lock()
	s.status.ConsecutiveErrors = 0
	s.status.LastSuccessAt = time.Now()
	if !result.Skipped {
		s.status.GroupsReconciled++
		if repair {
			s.status.GroupsRepaired++
		}
		if result.Bypassed > 0 {
			s.status.JobsBypassed += uint64(result.Bypassed)
		}
		if result.JobsResolved > 0 {
			s.status.JobsResolved += uint64(result.JobsResolved)
		}
		if result.JobReleased != nil {
			s.status.JobsReleased++
		}
	}
	s.mu.Unlock()
	s.notifyReleased(ctx, result.JobReleased)
}

// recordError preserves the latest diagnostic independently of later successes.
func (s *Service) recordError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status.ConsecutiveErrors++
	s.status.LastError = err.Error()
	s.status.LastErrorAt = time.Now()
}

// runOnce advances bounded pages even when all groups in a page are unhealthy.
func (s *Service) runOnce(ctx context.Context) error {
	now := time.Now()
	scanCtx, cancel := context.WithTimeout(ctx, s.Config.TransactionTimeout)
	due, err := s.seqExec.SequenceGroupList(scanCtx, &riverdriver.SequenceGroupListParams{AfterKey: s.dueCursor, DueBefore: &now, Limit: s.Config.BatchSize, Schema: s.Config.Schema})
	cancel()
	if err != nil {
		return err
	}
	if len(due) == s.Config.BatchSize {
		s.dueCursor = due[len(due)-1].Key
	} else {
		s.dueCursor = ""
	}
	seen := make(map[string]bool, len(due))
	for _, group := range due {
		seen[group.Key] = true
		s.process(ctx, group.Key, false)
	}
	if now.Before(s.nextSweep) {
		return nil
	}
	s.nextSweep = now.Add(30 * s.Config.PollInterval)
	scanCtx, cancel = context.WithTimeout(ctx, s.Config.TransactionTimeout)
	groups, err := s.seqExec.SequenceGroupList(scanCtx, &riverdriver.SequenceGroupListParams{AfterKey: s.scanCursor, Limit: s.Config.BatchSize, Schema: s.Config.Schema})
	cancel()
	if err != nil {
		return err
	}
	if len(groups) == s.Config.BatchSize {
		s.scanCursor = groups[len(groups)-1].Key
	} else {
		s.scanCursor = ""
	}
	for _, group := range groups {
		if !seen[group.Key] {
			s.process(ctx, group.Key, false)
		}
	}
	scanCtx, cancel = context.WithTimeout(ctx, s.Config.TransactionTimeout)
	orphans, err := s.seqExec.SequenceOrphanList(scanCtx, &riverdriver.SequenceOrphanListParams{AfterKey: s.orphanCursor, Limit: s.Config.BatchSize, Schema: s.Config.Schema})
	cancel()
	if err != nil {
		return err
	}
	if len(orphans) == s.Config.BatchSize {
		s.orphanCursor = orphans[len(orphans)-1]
	} else {
		s.orphanCursor = ""
	}
	for _, key := range orphans {
		s.process(ctx, key, true)
	}
	return nil
}

// transact retries only transaction failures whose rollback is known. A commit
// error is never replayed because its result may be ambiguous.
func (s *Service) transact(ctx context.Context, key string, repair bool) (*ReconcileResult, error) {
	for attempt := range 2 {
		tx, err := s.exec.Begin(ctx)
		if err != nil {
			return nil, err
		}
		result, innerErr := s.transactInner(ctx, tx, key, repair)
		if innerErr != nil {
			rollbackErr := dbutil.RollbackWithoutCancel(ctx, tx)
			if rollbackErr == nil && attempt == 0 && retryableTransactionError(innerErr) {
				continue
			}
			return nil, innerErr
		}
		if err := tx.Commit(ctx); err != nil {
			_ = dbutil.RollbackWithoutCancel(ctx, tx)
			return nil, fmt.Errorf("commit sequence reconciliation: %w", err)
		}
		return result, nil
	}
	return nil, ErrStateConflict
}

// transactInner contains only database actions safe to repeat after rollback.
func (s *Service) transactInner(ctx context.Context, tx riverdriver.ExecutorTx, key string, repair bool) (*ReconcileResult, error) {
	exec, ok := tx.(riverdriver.SequenceExecutor)
	if !ok {
		return nil, ErrNotSupported
	}
	if err := exec.SequenceSetLocalTimeouts(ctx); err != nil {
		return nil, err
	}
	if repair {
		if _, err := Repair(ctx, tx, s.Config.Schema, key); err != nil {
			return nil, err
		}
	}
	return Reconcile(ctx, tx, s.Config.Schema, key)
}

// failure is a bounded, in-memory retry hint; durable scans remain authoritative.
type failure struct {
	count int
	until time.Time
}

// backoff bounds retries with jitter without delaying normal job processing.
func backoff(count int) time.Duration {
	delay := min(time.Second<<min(max(count-1, 0), 5), 30*time.Second)
	return delay * time.Duration(750+rand.IntN(251)) / 1000
}

// retryableTransactionError recognizes only explicit serialization/deadlock
// failures, never connection loss or an uncertain commit.
func retryableTransactionError(err error) bool {
	var state interface{ SQLState() string }
	return errors.As(err, &state) && (state.SQLState() == "40001" || state.SQLState() == "40P01")
}

// Coordination errors leave both the job and its execution history intact.
var (
	ErrAlreadyResolved   = errors.New("river: sequence member is already resolved; insert a new job instead")
	ErrBlocked           = errors.New("river: sequence group is blocked")
	ErrBypass            = errors.New("river: sequence group is in bypass mode")
	ErrNeedsRepair       = errors.New("river: sequence group requires repair")
	ErrNotDrained        = errors.New("river: sequence group still contains unfinished or unresolved jobs")
	ErrNotHead           = errors.New("river: sequence member is not the unresolved head")
	ErrNotSupported      = errors.New("river: driver does not support sequences")
	ErrPositionExhausted = errors.New("river: sequence position exhausted")
	ErrStateConflict     = errors.New("river: sequence state changed or is inconsistent")
)

const (
	// AdvanceLimit bounds terminal processing and bypass work in one transaction.
	AdvanceLimit         = 32
	checkInterval        = time.Second
	blockedCheckInterval = 30 * time.Second
)

// Attachment identifies a new, already inserted pending job. Duplicate inserts
// must be excluded so that existing jobs are never reassigned to a sequence.
type Attachment struct {
	ContinueOnCancelled bool
	ContinueOnDiscarded bool
	JobID               int64
	Key                 string
}

// Attach assigns positions to new jobs in input order. Prepare must be called
// before inserting the jobs; group locks are reacquired defensively here.
func Attach(ctx context.Context, tx riverdriver.ExecutorTx, schema string, attachments []Attachment) ([]*rivertype.JobRow, error) {
	exec, err := executor(tx)
	if err != nil {
		return nil, err
	}
	keys := make([]string, len(attachments))
	for i, attachment := range attachments {
		keys[i] = attachment.Key
	}
	groups, err := prepare(ctx, exec, schema, keys)
	if err != nil {
		return nil, err
	}
	jobs := make([]*rivertype.JobRow, 0, len(attachments))
	for _, attachment := range attachments {
		group := groups[attachment.Key]
		if group.NextPosition == math.MaxInt64 {
			return nil, ErrPositionExhausted
		}
		job, err := exec.SequenceJobAttach(ctx, &riverdriver.SequenceJobAttachParams{
			ContinueOnCancelled: attachment.ContinueOnCancelled,
			ContinueOnDiscarded: attachment.ContinueOnDiscarded,
			ID:                  attachment.JobID, Key: attachment.Key, Position: group.NextPosition, Schema: schema,
		})
		if err != nil {
			return nil, fmt.Errorf("attach sequence member %d: %w", attachment.JobID, err)
		}
		group.NextPosition++
		jobs = append(jobs, job)
	}
	for _, key := range sortedKeys(keys) {
		group := groups[key]
		group.NextCheckAt = new(time.Now())
		if _, err := update(ctx, exec, schema, group); err != nil {
			return nil, err
		}
	}
	return jobs, nil
}

// Bypass persistently waives a group's ordering. A reconciler releases its
// waiting members in bounded batches without altering execution history.
func Bypass(ctx context.Context, tx riverdriver.ExecutorTx, schema, key string) error {
	exec, err := executor(tx)
	if err != nil {
		return err
	}
	group, _, err := repair(ctx, exec, schema, key)
	if err != nil {
		return err
	}
	clearBarrier(group)
	group.Mode = "bypass"
	group.NextCheckAt = new(time.Now())
	_, err = update(ctx, exec, schema, group)
	return err
}

// Prepare serializes all groups in a stable order before normal insertion. It
// never locks job rows, so a long business insert transaction cannot hold up
// completion of an existing head merely by joining the same group.
func Prepare(ctx context.Context, tx riverdriver.ExecutorTx, schema string, keys []string) error {
	exec, err := executor(tx)
	if err != nil {
		return err
	}
	_, err = prepare(ctx, exec, schema, keys)
	return err
}

// ReconcileResult describes work done inside the caller's transaction. Results
// only become durable after that transaction commits.
type ReconcileResult struct {
	Bypassed     int
	Group        *riverdriver.SequenceGroup
	JobReleased  *rivertype.JobRow
	JobsResolved int
	Skipped      bool
}

// Reconcile advances at most one group's bounded frontier. The group may be
// skipped when another transaction owns its lock, but its head is never skipped.
// No caller should invoke this from a worker's completion transaction.
func Reconcile(ctx context.Context, tx riverdriver.ExecutorTx, schema, key string) (*ReconcileResult, error) {
	exec, err := executor(tx)
	if err != nil {
		return nil, err
	}
	group, err := exec.SequenceGroupGet(ctx, &riverdriver.SequenceGroupGetParams{Key: key, Lock: true, Schema: schema, SkipLocked: true})
	if errors.Is(err, rivertype.ErrNotFound) {
		return &ReconcileResult{Skipped: true}, nil
	}
	if err != nil {
		return nil, err
	}
	result := &ReconcileResult{Group: group}
	now := time.Now()
	group.NextCheckAt = new(now.Add(checkInterval))
	switch group.Mode {
	case "blocked":
		group.NextCheckAt = new(now.Add(blockedCheckInterval))
	case "bypass":
		result.Bypassed, err = exec.SequenceJobBypass(ctx, &riverdriver.SequenceJobBypassParams{Key: key, Limit: AdvanceLimit, Now: now, Schema: schema})
		if err != nil {
			return nil, err
		}
		if result.Bypassed > 0 {
			group.LastProgressAt = now
		}
		if result.Bypassed < AdvanceLimit {
			group.NextCheckAt = nil
		}
	case "ordered":
		for range AdvanceLimit {
			job, err := exec.SequenceJobHead(ctx, &riverdriver.SequenceGroupKeyParams{Key: key, Lock: true, Schema: schema})
			if errors.Is(err, rivertype.ErrNotFound) {
				stats, statsErr := exec.SequenceJobStats(ctx, &riverdriver.SequenceGroupKeyParams{Key: key, Schema: schema})
				if statsErr != nil {
					return nil, statsErr
				}
				if stats.ResolvedNonterminal != 0 {
					return nil, ErrNeedsRepair
				}
				group.NextCheckAt = nil
				break
			}
			if err != nil {
				return nil, err
			}
			if err := validateMember(job, key); err != nil {
				return nil, err
			}
			switch terminalDecision(job) {
			case decisionInvalid:
				return nil, ErrStateConflict
			case decisionContinue:
				if _, err := exec.SequenceJobResolve(ctx, mutation(schema, key, job.ID, now)); err != nil {
					return nil, err
				}
				group.LastProgressAt = now
				result.JobsResolved++
				continue
			case decisionBlock:
				setBarrier(group, job, now)
			case decisionWait:
				if job.State == rivertype.JobStatePending {
					stats, err := exec.SequenceJobStats(ctx, &riverdriver.SequenceGroupKeyParams{Key: key, Schema: schema})
					if err != nil {
						return nil, err
					}
					if stats.Active != 0 {
						return nil, ErrNeedsRepair
					}
					result.JobReleased, err = exec.SequenceJobRelease(ctx, mutation(schema, key, job.ID, now))
					if err != nil {
						return nil, err
					}
					group.LastProgressAt = now
				}
			}
			break
		}
	default:
		return nil, ErrStateConflict
	}
	result.Group, err = update(ctx, exec, schema, group)
	return result, err
}

// Repair recreates a missing group and advances its allocator beyond every
// retained member, including resolved history. It never silently enables bypass.
func Repair(ctx context.Context, tx riverdriver.ExecutorTx, schema, key string) (*riverdriver.SequenceGroup, error) {
	exec, err := executor(tx)
	if err != nil {
		return nil, err
	}
	group, _, err := repair(ctx, exec, schema, key)
	return group, err
}

// Restore allows new ordered inserts only after bypass has fully drained. All
// retained nonterminal jobs count, even those whose ordering was already waived.
func Restore(ctx context.Context, tx riverdriver.ExecutorTx, schema, key string) error {
	exec, err := executor(tx)
	if err != nil {
		return err
	}
	group, stats, err := repair(ctx, exec, schema, key)
	if err != nil {
		return err
	}
	if group.Mode == "blocked" {
		return ErrBlocked
	}
	if stats.Nonterminal != 0 || stats.Unresolved != 0 {
		return ErrNotDrained
	}
	clearBarrier(group)
	group.Mode = "ordered"
	group.NextCheckAt = nil
	_, err = update(ctx, exec, schema, group)
	return err
}

// Resume acknowledges exactly the terminal barrier observed by an operator.
// Deleting that job does not erase the barrier, but an explicit resume can.
func Resume(ctx context.Context, tx riverdriver.ExecutorTx, schema, key string, expectedJobID, expectedPosition int64) error {
	exec, err := executor(tx)
	if err != nil {
		return err
	}
	group, err := exec.SequenceGroupGet(ctx, &riverdriver.SequenceGroupGetParams{Key: key, Lock: true, Schema: schema})
	if err != nil {
		return err
	}
	if group.Mode != "blocked" || group.BlockedJobID == nil || group.BlockedPosition == nil || *group.BlockedJobID != expectedJobID || *group.BlockedPosition != expectedPosition {
		return ErrStateConflict
	}
	job, err := exec.SequenceJobGet(ctx, &riverdriver.SequenceJobGetParams{ID: expectedJobID, Key: key, Schema: schema})
	if err != nil && !errors.Is(err, rivertype.ErrNotFound) {
		return err
	}
	now := time.Now()
	if job != nil {
		if err := validateMember(job, key); err != nil {
			return err
		}
		if *job.SequencePosition != expectedPosition || (job.State != rivertype.JobStateCancelled && job.State != rivertype.JobStateDiscarded) || job.SequenceResolvedAt != nil {
			return ErrStateConflict
		}
		if _, err := exec.SequenceJobResolve(ctx, mutation(schema, key, job.ID, now)); err != nil {
			return err
		}
	}
	clearBarrier(group)
	group.Mode = "ordered"
	group.NextCheckAt = new(now)
	group.LastProgressAt = now
	_, err = update(ctx, exec, schema, group)
	return err
}

// Retry preserves River retry semantics for the current unresolved member. It
// never revives historical members or lets a pending successor overtake its head.
// An unset retry time is resolved by the database, matching ordinary retries.
func Retry(ctx context.Context, tx riverdriver.ExecutorTx, params *riverdriver.JobRetryParams) (*rivertype.JobRow, error) {
	job, err := tx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: params.ID, Schema: params.Schema})
	if err != nil {
		return nil, err
	}
	if job.SequenceKey == nil {
		return tx.JobRetry(ctx, params)
	}
	exec, err := executor(tx)
	if err != nil {
		return nil, err
	}
	key := *job.SequenceKey
	group, _, err := repair(ctx, exec, params.Schema, key)
	if err != nil {
		return nil, err
	}
	if group.Mode == "bypass" {
		return nil, ErrBypass
	}
	job, err = exec.SequenceJobGet(ctx, &riverdriver.SequenceJobGetParams{ID: job.ID, Key: key, Schema: params.Schema})
	if err != nil {
		return nil, err
	}
	if err := validateMember(job, key); err != nil {
		return nil, err
	}
	if job.SequenceResolvedAt != nil {
		return nil, ErrAlreadyResolved
	}
	if group.Mode == "blocked" && (group.BlockedJobID == nil || group.BlockedPosition == nil || *group.BlockedJobID != job.ID || *group.BlockedPosition != *job.SequencePosition) {
		return nil, ErrBlocked
	}
	head, err := exec.SequenceJobHead(ctx, &riverdriver.SequenceGroupKeyParams{Key: key, Lock: true, Schema: params.Schema})
	if err != nil {
		return nil, err
	}
	if head.ID != job.ID {
		return nil, ErrNotHead
	}
	if group.Mode == "blocked" && job.State != rivertype.JobStateCancelled && job.State != rivertype.JobStateDiscarded {
		return nil, ErrStateConflict
	}
	stats, err := exec.SequenceJobStats(ctx, &riverdriver.SequenceGroupKeyParams{Key: key, Schema: params.Schema})
	if err != nil {
		return nil, err
	}
	allowedActive := int64(0)
	if isActive(job.State) {
		allowedActive = 1
	}
	if stats.Active > allowedActive {
		return nil, ErrNeedsRepair
	}
	now := time.Now()
	if params.Now != nil {
		now = *params.Now
	}
	job, err = exec.SequenceJobRetry(ctx, &riverdriver.SequenceJobRetryParams{
		ID: job.ID, Key: key, Now: params.Now, Schema: params.Schema,
	})
	if err != nil {
		return nil, err
	}
	clearBarrier(group)
	group.Mode = "ordered"
	group.NextCheckAt = new(now)
	if _, err := update(ctx, exec, params.Schema, group); err != nil {
		return nil, err
	}
	return job, nil
}

// clearBarrier removes only the persisted coordination decision.
func clearBarrier(group *riverdriver.SequenceGroup) {
	group.BlockedAt = nil
	group.BlockedJobID = nil
	group.BlockedPosition = nil
	group.BlockedReason = nil
}

// executor checks the optional storage capability without affecting core paths.
func executor(tx riverdriver.ExecutorTx) (riverdriver.SequenceExecutor, error) {
	exec, ok := tx.(riverdriver.SequenceExecutor)
	if !ok {
		return nil, ErrNotSupported
	}
	return exec, nil
}

// isActive identifies execution states that retain a group's ordering slot.
func isActive(state rivertype.JobState) bool {
	return state == rivertype.JobStateAvailable || state == rivertype.JobStateRunning || state == rivertype.JobStateScheduled || state == rivertype.JobStateRetryable
}

// mutation centralizes the explicit group scope of job updates.
func mutation(schema, key string, id int64, now time.Time) *riverdriver.SequenceJobMutationParams {
	return &riverdriver.SequenceJobMutationParams{ID: id, Key: key, Now: now, Schema: schema}
}

// prepare locks unique keys in stable order and rejects unsafe admission.
func prepare(ctx context.Context, exec riverdriver.SequenceExecutor, schema string, keys []string) (map[string]*riverdriver.SequenceGroup, error) {
	groups := make(map[string]*riverdriver.SequenceGroup, len(keys))
	counts := make(map[string]int64, len(keys))
	for _, key := range keys {
		counts[key]++
	}
	for _, key := range sortedKeys(keys) {
		group, stats, err := repair(ctx, exec, schema, key)
		if err != nil {
			return nil, err
		}
		if group.Mode == "bypass" {
			return nil, ErrBypass
		}
		if stats.ResolvedNonterminal != 0 {
			return nil, ErrNeedsRepair
		}
		// Reserve enough allocator range before any job INSERT, even when a
		// later uniqueness conflict might make that reservation unnecessary.
		if counts[key] > math.MaxInt64-group.NextPosition {
			return nil, ErrPositionExhausted
		}
		groups[key] = group
	}
	return groups, nil
}

// repair uses fresh statements after acquiring the group lock, which is needed
// for READ COMMITTED visibility when another inserter just committed.
func repair(ctx context.Context, exec riverdriver.SequenceExecutor, schema, key string) (*riverdriver.SequenceGroup, *riverdriver.SequenceJobStats, error) {
	if key == "" || len(key) >= 512 {
		return nil, nil, ErrStateConflict
	}
	var group *riverdriver.SequenceGroup
	for range 3 {
		if err := exec.SequenceGroupEnsure(ctx, &riverdriver.SequenceGroupKeyParams{Key: key, Schema: schema}); err != nil {
			return nil, nil, err
		}
		var err error
		group, err = exec.SequenceGroupGet(ctx, &riverdriver.SequenceGroupGetParams{Key: key, Lock: true, Schema: schema})
		if errors.Is(err, rivertype.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		break
	}
	if group == nil {
		return nil, nil, ErrNeedsRepair
	}
	stats, err := exec.SequenceJobStats(ctx, &riverdriver.SequenceGroupKeyParams{Key: key, Schema: schema})
	if err != nil {
		return nil, nil, err
	}
	// MaxInt64 is an exhausted allocator sentinel. Administrative recovery must
	// remain possible even when no additional member can be admitted.
	nextPosition := stats.MaxPosition
	if nextPosition < math.MaxInt64 {
		nextPosition++
	}
	group.NextPosition = max(group.NextPosition, nextPosition)
	if stats.Unresolved > 0 && group.NextCheckAt == nil {
		group.NextCheckAt = new(time.Now())
	}
	if group.Mode == "ordered" {
		// This read deliberately does not lock a job. All supported administrative
		// retries also acquire the group lock; core completion only moves forward.
		head, err := exec.SequenceJobHead(ctx, &riverdriver.SequenceGroupKeyParams{Key: key, Schema: schema})
		if err != nil && !errors.Is(err, rivertype.ErrNotFound) {
			return nil, nil, err
		}
		if head != nil {
			if err := validateMember(head, key); err != nil {
				return nil, nil, err
			}
			if terminalDecision(head) == decisionBlock {
				setBarrier(group, head, time.Now())
			}
		}
	}
	group, err = update(ctx, exec, schema, group)
	return group, stats, err
}

// setBarrier persists terminal policy independently of the job's retention.
func setBarrier(group *riverdriver.SequenceGroup, job *rivertype.JobRow, now time.Time) {
	reason := string(job.State)
	group.Mode = "blocked"
	group.BlockedAt = new(now)
	group.BlockedJobID = &job.ID
	group.BlockedPosition = job.SequencePosition
	group.BlockedReason = &reason
	group.NextCheckAt = new(now.Add(blockedCheckInterval))
}

// sortedKeys returns a stable lock order without changing caller-owned slices.
func sortedKeys(keys []string) []string {
	keys = slices.Clone(keys)
	slices.Sort(keys)
	return slices.Compact(keys)
}

type decision int

const (
	decisionInvalid decision = iota
	decisionBlock
	decisionContinue
	decisionWait
)

// terminalDecision applies the finishing member's persisted policy, never its
// successor's policy. Cancellation requests remain active until finalized.
func terminalDecision(job *rivertype.JobRow) decision {
	switch job.State {
	case rivertype.JobStateCompleted:
		return decisionContinue
	case rivertype.JobStateCancelled:
		if job.SequenceContinueOnCancelled != nil && *job.SequenceContinueOnCancelled {
			return decisionContinue
		}
		return decisionBlock
	case rivertype.JobStateDiscarded:
		if job.SequenceContinueOnDiscarded != nil && *job.SequenceContinueOnDiscarded {
			return decisionContinue
		}
		return decisionBlock
	case rivertype.JobStatePending, rivertype.JobStateAvailable, rivertype.JobStateRunning, rivertype.JobStateRetryable, rivertype.JobStateScheduled:
		return decisionWait
	default:
		return decisionInvalid
	}
}

// update persists coordination fields without changing any execution result.
func update(ctx context.Context, exec riverdriver.SequenceExecutor, schema string, group *riverdriver.SequenceGroup) (*riverdriver.SequenceGroup, error) {
	return exec.SequenceGroupUpdate(ctx, &riverdriver.SequenceGroupUpdateParams{Group: group, Schema: schema})
}

// validateMember rejects damaged membership instead of pretending it completed.
func validateMember(job *rivertype.JobRow, key string) error {
	if job.SequenceKey == nil || *job.SequenceKey != key || job.SequencePosition == nil || *job.SequencePosition <= 0 || job.SequenceContinueOnCancelled == nil || job.SequenceContinueOnDiscarded == nil {
		return ErrStateConflict
	}
	return nil
}
