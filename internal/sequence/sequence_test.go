package sequence

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivertype"
)

// TestService verifies that coordinator failures remain operationally isolated.
func TestService(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) (*Service, *serviceExecutor) {
		t.Helper()
		exec := &serviceExecutor{groups: make(map[string]*riverdriver.SequenceGroup)}
		service := New(baseservice.NewArchetype(slog.New(slog.DiscardHandler)), &Config{BatchSize: 1, PollInterval: 10 * time.Millisecond, TransactionTimeout: 200 * time.Millisecond}, exec)
		return service, exec
	}

	t.Run("AmbiguousCommitIsNeverReplayed", func(t *testing.T) {
		t.Parallel()

		service, exec := setup(t)
		exec.addGroup("group")
		exec.commitError = errors.New("connection lost during commit")
		_, err := service.transact(t.Context(), "group", false)
		require.ErrorIs(t, err, exec.commitError)
		require.Equal(t, int64(1), exec.begins.Load())
		require.Equal(t, int64(1), exec.rollbacks.Load())
	})

	t.Run("DatabaseFailureDoesNotFailStartAndRecovers", func(t *testing.T) {
		t.Parallel()

		service, exec := setup(t)
		exec.addGroup("group")
		exec.failScans.Store(true)
		require.NoError(t, service.Start(t.Context()))
		t.Cleanup(service.Stop)
		select {
		case <-service.Started():
		case <-time.After(time.Second):
			t.Fatal("coordinator startup waited for a database")
		}
		require.Eventually(t, func() bool { return service.Status().LastError != "" }, time.Second, time.Millisecond)
		exec.failScans.Store(false)
		require.Eventually(t, func() bool { return service.Status().GroupsReconciled > 0 }, 3*time.Second, 10*time.Millisecond)
		require.Equal(t, 0, service.Status().ConsecutiveErrors)
		require.NotZero(t, service.Status().LastSuccessAt)
	})

	t.Run("FailedFirstPageCannotStarveLaterKeys", func(t *testing.T) {
		t.Parallel()

		service, exec := setup(t)
		for _, key := range []string{"a", "b", "c"} {
			exec.addGroup(key)
		}
		exec.failedKey = "a"
		for range 3 {
			require.NoError(t, service.runOnce(t.Context()))
		}
		require.Equal(t, []string{"a", "b", "c"}, exec.visited)
		require.Equal(t, uint64(2), service.Status().GroupsReconciled)
		require.Contains(t, service.failures, "a")
	})

	t.Run("FullSweepAndOrphanScanRepairMissedDueMarks", func(t *testing.T) {
		t.Parallel()

		service, exec := setup(t)
		exec.addGroup("idle").NextCheckAt = nil
		exec.orphans = []string{"orphan"}
		require.NoError(t, service.runOnce(t.Context()))
		require.Equal(t, uint64(2), service.Status().GroupsReconciled)
		require.Equal(t, uint64(1), service.Status().GroupsRepaired)
		require.Contains(t, exec.groups, "orphan")
		require.Equal(t, []string{"idle", "orphan", "orphan"}, exec.visited)
	})

	t.Run("HintsAreBoundedAndNeverBlock", func(t *testing.T) {
		t.Parallel()

		service, _ := setup(t)
		for range maxHints * 3 {
			service.Hint("key")
		}
		require.Len(t, service.hints, maxHints)
	})

	t.Run("NotificationFailureDoesNotUndoCommittedRelease", func(t *testing.T) {
		t.Parallel()

		service, exec := setup(t)
		exec.addGroup("group")
		key, position, continueOnTerminal := "group", int64(1), false
		exec.head = &rivertype.JobRow{ID: 1, Queue: "default", State: rivertype.JobStatePending, SequenceKey: &key, SequencePosition: &position, SequenceContinueOnCancelled: &continueOnTerminal, SequenceContinueOnDiscarded: &continueOnTerminal}
		exec.notifyError = errors.New("notification unavailable")
		service.process(t.Context(), "group", false)
		require.Equal(t, int64(1), exec.commits.Load())
		require.Equal(t, int64(1), exec.notifications.Load())
		require.Equal(t, uint64(1), service.Status().JobsReleased)
		require.Empty(t, service.Status().LastError)
		require.Empty(t, service.failures)
	})

	t.Run("SerializationFailureRetriesOnlyAfterRollback", func(t *testing.T) {
		t.Parallel()

		service, exec := setup(t)
		exec.addGroup("group")
		exec.firstGetError = sqlStateError("40001")
		result, err := service.transact(t.Context(), "group", false)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, int64(2), exec.begins.Load())
		require.Equal(t, int64(1), exec.rollbacks.Load())
		require.Equal(t, int64(1), exec.commits.Load())
	})

	t.Run("StopCancelsConnectionAcquisition", func(t *testing.T) {
		t.Parallel()

		service, exec := setup(t)
		exec.addGroup("group")
		exec.blockBegin = true
		require.NoError(t, service.Start(t.Context()))
		require.Eventually(t, func() bool { return exec.begins.Load() > 0 }, time.Second, time.Millisecond)
		stopped := service.Stopped()
		service.Stop()
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("coordinator did not stop after cancellation")
		}
	})
}

// TestBackoff checks that transient failures cannot become an unbounded delay.
func TestBackoff(t *testing.T) {
	t.Parallel()

	for _, count := range []int{0, 1, 10, 100000} {
		delay := backoff(count)
		require.GreaterOrEqual(t, delay, 750*time.Millisecond)
		require.LessOrEqual(t, delay, 30*time.Second)
	}
	require.True(t, retryableTransactionError(sqlStateError("40P01")))
	require.False(t, retryableTransactionError(sqlStateError("55P03")))
	require.False(t, retryableTransactionError(context.DeadlineExceeded))
}

// serviceExecutor observes ordering at transaction boundaries; root integration
// tests exercise these same paths against both real Postgres adapters.
type serviceExecutor struct {
	riverdriver.Executor
	riverdriver.SequenceExecutor

	begins        atomic.Int64
	commits       atomic.Int64
	failScans     atomic.Bool
	gets          atomic.Int64
	notifications atomic.Int64
	rollbacks     atomic.Int64

	blockBegin    bool
	commitError   error
	failedKey     string
	firstGetError error
	groups        map[string]*riverdriver.SequenceGroup
	head          *rivertype.JobRow
	mu            sync.Mutex
	notifyError   error
	orphans       []string
	visited       []string
}

// addGroup seeds an immediately due group before starting a service.
func (exec *serviceExecutor) addGroup(key string) *riverdriver.SequenceGroup {
	now := time.Now()
	group := &riverdriver.SequenceGroup{Key: key, Mode: "ordered", NextPosition: 1, NextCheckAt: &now}
	exec.groups[key] = group
	return group
}

// Begin can simulate exhausted pools without coupling startup to pool capacity.
func (exec *serviceExecutor) Begin(ctx context.Context) (riverdriver.ExecutorTx, error) {
	exec.begins.Add(1)
	if exec.blockBegin {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &serviceTx{serviceExecutor: exec}, nil
}

// NotifyMany verifies that a notification follows a committed transaction.
func (exec *serviceExecutor) NotifyMany(context.Context, *riverdriver.NotifyManyParams) error {
	if exec.commits.Load() == 0 {
		return errors.New("notification preceded commit")
	}
	exec.notifications.Add(1)
	return exec.notifyError
}

// SequenceGroupEnsure recreates a missing fixture group.
func (exec *serviceExecutor) SequenceGroupEnsure(_ context.Context, params *riverdriver.SequenceGroupKeyParams) error {
	exec.mu.Lock()
	defer exec.mu.Unlock()
	if exec.groups[params.Key] == nil {
		exec.addGroup(params.Key)
	}
	return nil
}

// SequenceGroupGet records each attempted group, including deliberate failures.
func (exec *serviceExecutor) SequenceGroupGet(_ context.Context, params *riverdriver.SequenceGroupGetParams) (*riverdriver.SequenceGroup, error) {
	exec.mu.Lock()
	defer exec.mu.Unlock()
	exec.visited = append(exec.visited, params.Key)
	if exec.gets.Add(1) == 1 && exec.firstGetError != nil {
		return nil, exec.firstGetError
	}
	if params.Key == exec.failedKey {
		return nil, errors.New("poisoned group")
	}
	if group := exec.groups[params.Key]; group != nil {
		copied := *group
		return &copied, nil
	}
	return nil, rivertype.ErrNotFound
}

// SequenceGroupList implements keyset paging and optional durable due filtering.
func (exec *serviceExecutor) SequenceGroupList(_ context.Context, params *riverdriver.SequenceGroupListParams) ([]*riverdriver.SequenceGroup, error) {
	if exec.failScans.Load() {
		return nil, errors.New("sequence table unavailable")
	}
	exec.mu.Lock()
	defer exec.mu.Unlock()
	keys := make([]string, 0, len(exec.groups))
	for key, group := range exec.groups {
		if key <= params.AfterKey {
			continue
		}
		if params.DueBefore != nil && (group.NextCheckAt == nil || group.NextCheckAt.After(*params.DueBefore)) {
			continue
		}
		keys = append(keys, key)
	}
	slices.Sort(keys)
	var groups []*riverdriver.SequenceGroup
	for _, key := range keys[:min(len(keys), params.Limit)] {
		copied := *exec.groups[key]
		groups = append(groups, &copied)
	}
	return groups, nil
}

// SequenceGroupUpdate keeps the fixture's authoritative group state.
func (exec *serviceExecutor) SequenceGroupUpdate(_ context.Context, params *riverdriver.SequenceGroupUpdateParams) (*riverdriver.SequenceGroup, error) {
	exec.mu.Lock()
	defer exec.mu.Unlock()
	exec.groups[params.Group.Key] = params.Group
	return params.Group, nil
}

// SequenceJobHead defaults to an idle group, unless a release fixture is present.
func (exec *serviceExecutor) SequenceJobHead(context.Context, *riverdriver.SequenceGroupKeyParams) (*rivertype.JobRow, error) {
	if exec.head == nil {
		return nil, rivertype.ErrNotFound
	}
	return exec.head, nil
}

// SequenceJobRelease grants eligibility without simulating business completion.
func (exec *serviceExecutor) SequenceJobRelease(context.Context, *riverdriver.SequenceJobMutationParams) (*rivertype.JobRow, error) {
	exec.head.State = rivertype.JobStateAvailable
	return exec.head, nil
}

// SequenceJobStats models a group without conflicting active members.
func (exec *serviceExecutor) SequenceJobStats(context.Context, *riverdriver.SequenceGroupKeyParams) (*riverdriver.SequenceJobStats, error) {
	return &riverdriver.SequenceJobStats{}, nil
}

// SequenceOrphanList returns a separate paged source of missing groups.
func (exec *serviceExecutor) SequenceOrphanList(_ context.Context, params *riverdriver.SequenceOrphanListParams) ([]string, error) {
	var keys []string
	for _, key := range exec.orphans {
		if key > params.AfterKey {
			keys = append(keys, key)
		}
	}
	return keys[:min(len(keys), params.Limit)], nil
}

// SequenceSetLocalTimeouts is called only on owned fixture transactions.
func (exec *serviceExecutor) SequenceSetLocalTimeouts(context.Context) error { return nil }

// serviceTx records commit and rollback separately so uncertain commits cannot
// accidentally be counted as safe, retryable query failures.
type serviceTx struct {
	*serviceExecutor
}

// Commit records a commit attempt and can simulate an ambiguous result.
func (tx *serviceTx) Commit(context.Context) error {
	tx.commits.Add(1)
	return tx.commitError
}

// Rollback confirms rollback before the service considers retrying.
func (tx *serviceTx) Rollback(context.Context) error { tx.rollbacks.Add(1); return nil }

// sqlStateError models the portable SQLSTATE contract of Postgres errors.
type sqlStateError string

// Error returns a diagnostic state.
func (err sqlStateError) Error() string { return string(err) }

// SQLState returns the explicit transaction failure category.
func (err sqlStateError) SQLState() string { return string(err) }

// TestPrepareAndAttach verifies admission and persistent allocation invariants.
func TestPrepareAndAttach(t *testing.T) {
	t.Parallel()

	t.Run("AllocatorNeverRewindsAcrossRetainedHistory", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.jobs[1] = protocolJob(1, "group", 90, rivertype.JobStateCompleted)
		tx.jobs[1].SequenceResolvedAt = new(time.Now())
		tx.jobs[2] = &rivertype.JobRow{ID: 2, State: rivertype.JobStatePending}
		require.NoError(t, Prepare(t.Context(), tx, "tenant", []string{"group"}))
		require.Equal(t, int64(91), tx.groups["group"].NextPosition)
		jobs, err := Attach(t.Context(), tx, "tenant", []Attachment{{JobID: 2, Key: "group"}})
		require.NoError(t, err)
		require.Equal(t, int64(91), *jobs[0].SequencePosition)
		require.Equal(t, int64(92), tx.groups["group"].NextPosition)
		require.Empty(t, tx.lockedJobs, "admission must not lock existing worker rows")
	})

	t.Run("ExhaustedPosition", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.jobs[1] = protocolJob(1, "group", math.MaxInt64, rivertype.JobStatePending)
		require.ErrorIs(t, Prepare(t.Context(), tx, "", []string{"group"}), ErrPositionExhausted)
	})

	t.Run("PreflightIncludesWholeBatchBeforeAnyAttach", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.addGroup("group").NextPosition = math.MaxInt64 - 1
		tx.jobs[1] = &rivertype.JobRow{ID: 1, State: rivertype.JobStatePending}
		tx.jobs[2] = &rivertype.JobRow{ID: 2, State: rivertype.JobStatePending}
		require.ErrorIs(t, Prepare(t.Context(), tx, "", []string{"group", "group"}), ErrPositionExhausted)
		_, err := Attach(t.Context(), tx, "", []Attachment{{JobID: 1, Key: "group"}, {JobID: 2, Key: "group"}})
		require.ErrorIs(t, err, ErrPositionExhausted)
		require.Nil(t, tx.jobs[1].SequenceKey)
		require.Nil(t, tx.jobs[2].SequenceKey)
	})

	t.Run("RejectsBypassAndResolvedNonterminal", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.groups["bypass"] = &riverdriver.SequenceGroup{Key: "bypass", Mode: "bypass", NextPosition: 1}
		require.ErrorIs(t, Prepare(t.Context(), tx, "", []string{"bypass"}), ErrBypass)
		tx.jobs[1] = protocolJob(1, "damaged", 1, rivertype.JobStateRunning)
		tx.jobs[1].SequenceResolvedAt = new(time.Now())
		require.ErrorIs(t, Prepare(t.Context(), tx, "", []string{"damaged"}), ErrNeedsRepair)
	})

	t.Run("SortedLocksAndInputOrderPositions", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		for _, id := range []int64{1, 2, 3} {
			tx.jobs[id] = &rivertype.JobRow{ID: id, State: rivertype.JobStatePending}
		}
		keys := []string{"z", "a", "z"}
		require.NoError(t, Prepare(t.Context(), tx, "", keys))
		require.Equal(t, []string{"a", "z"}, tx.lockedGroups)
		require.Equal(t, []string{"z", "a", "z"}, keys)
		jobs, err := Attach(t.Context(), tx, "", []Attachment{{JobID: 1, Key: "z"}, {JobID: 2, Key: "a"}, {JobID: 3, Key: "z"}})
		require.NoError(t, err)
		require.Equal(t, int64(1), *jobs[0].SequencePosition)
		require.Equal(t, int64(1), *jobs[1].SequencePosition)
		require.Equal(t, int64(2), *jobs[2].SequencePosition)
	})
}

// TestReconcile exercises the complete state decision table and bounded work.
func TestReconcile(t *testing.T) {
	t.Parallel()

	t.Run("ActiveAnomalyPreservesPendingAndWorker", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.addGroup("group")
		tx.jobs[1] = protocolJob(1, "group", 1, rivertype.JobStatePending)
		tx.jobs[2] = protocolJob(2, "group", 2, rivertype.JobStateRunning)
		tx.jobs[2].SequenceResolvedAt = new(time.Now())
		_, err := Reconcile(t.Context(), tx, "", "group")
		require.ErrorIs(t, err, ErrNeedsRepair)
		require.Equal(t, rivertype.JobStatePending, tx.jobs[1].State)
		require.Equal(t, rivertype.JobStateRunning, tx.jobs[2].State)
	})

	t.Run("BlockedSurvivesDeletedBarrier", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		group := tx.addGroup("group")
		barrier := protocolJob(1, "group", 1, rivertype.JobStateCancelled)
		setBarrier(group, barrier, time.Now())
		tx.jobs[2] = protocolJob(2, "group", 2, rivertype.JobStatePending)
		result, err := Reconcile(t.Context(), tx, "", "group")
		require.NoError(t, err)
		require.Equal(t, "blocked", result.Group.Mode)
		require.Nil(t, result.JobReleased)
		require.Empty(t, tx.lockedJobs)
	})

	t.Run("EveryExecutionStateRetainsItsSlot", func(t *testing.T) {
		t.Parallel()

		for _, state := range []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateRetryable, rivertype.JobStateRunning, rivertype.JobStateScheduled} {
			tx := newProtocolTx()
			tx.addGroup("group")
			tx.jobs[1] = protocolJob(1, "group", 1, state)
			tx.jobs[2] = protocolJob(2, "group", 2, rivertype.JobStatePending)
			result, err := Reconcile(t.Context(), tx, "", "group")
			require.NoError(t, err)
			require.Nil(t, result.JobReleased, state)
			require.Equal(t, state, tx.jobs[1].State)
			require.Equal(t, rivertype.JobStatePending, tx.jobs[2].State)
			require.NotNil(t, result.Group.NextCheckAt)
		}
	})

	t.Run("LockedHeadIsNeverSkipped", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.addGroup("group")
		tx.jobs[1] = protocolJob(1, "group", 1, rivertype.JobStatePending)
		tx.jobs[2] = protocolJob(2, "group", 2, rivertype.JobStatePending)
		tx.headLockError = errors.New("head row is busy")
		_, err := Reconcile(t.Context(), tx, "", "group")
		require.ErrorIs(t, err, tx.headLockError)
		require.Equal(t, []int64{1}, tx.lockedJobs)
		require.Equal(t, rivertype.JobStatePending, tx.jobs[2].State)
	})

	t.Run("MissingGroupIsSkipped", func(t *testing.T) {
		t.Parallel()

		result, err := Reconcile(t.Context(), newProtocolTx(), "", "missing")
		require.NoError(t, err)
		require.True(t, result.Skipped)
	})

	t.Run("NoUnresolvedDoesNotHideResolvedPendingAnomaly", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.addGroup("group")
		tx.jobs[1] = protocolJob(1, "group", 1, rivertype.JobStatePending)
		tx.jobs[1].SequenceResolvedAt = new(time.Now())
		_, err := Reconcile(t.Context(), tx, "", "group")
		require.ErrorIs(t, err, ErrNeedsRepair)
		require.Equal(t, rivertype.JobStatePending, tx.jobs[1].State)
	})

	t.Run("PerMemberPolicyAndBoundedTerminalAdvance", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.addGroup("group")
		for id := int64(1); id <= AdvanceLimit+1; id++ {
			tx.jobs[id] = protocolJob(id, "group", id*2, rivertype.JobStateCompleted)
		}
		lastID := int64(AdvanceLimit + 2)
		tx.jobs[lastID] = protocolJob(lastID, "group", lastID*2, rivertype.JobStatePending)
		result, err := Reconcile(t.Context(), tx, "", "group")
		require.NoError(t, err)
		require.Equal(t, AdvanceLimit, result.JobsResolved)
		require.Nil(t, result.JobReleased)
		result, err = Reconcile(t.Context(), tx, "", "group")
		require.NoError(t, err)
		require.Equal(t, 1, result.JobsResolved)
		require.Equal(t, lastID, result.JobReleased.ID)
	})

	t.Run("TerminalPolicyBelongsToPredecessor", func(t *testing.T) {
		t.Parallel()

		for _, state := range []rivertype.JobState{rivertype.JobStateCancelled, rivertype.JobStateDiscarded} {
			tx := newProtocolTx()
			tx.addGroup("group")
			tx.jobs[1] = protocolJob(1, "group", 1, state)
			tx.jobs[2] = protocolJob(2, "group", 2, rivertype.JobStatePending)
			*tx.jobs[2].SequenceContinueOnCancelled = true
			*tx.jobs[2].SequenceContinueOnDiscarded = true
			result, err := Reconcile(t.Context(), tx, "", "group")
			require.NoError(t, err)
			require.Equal(t, "blocked", result.Group.Mode)
			require.Nil(t, tx.jobs[1].SequenceResolvedAt)
			require.Equal(t, rivertype.JobStatePending, tx.jobs[2].State)
		}
	})
}

// TestResumeRestoreRetry verifies that administrative operations cannot leapfrog.
func TestResumeRestoreRetry(t *testing.T) {
	t.Parallel()

	t.Run("BypassRemainsAvailableAfterAllocatorExhaustion", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.jobs[1] = protocolJob(1, "group", math.MaxInt64, rivertype.JobStateRunning)
		require.NoError(t, Bypass(t.Context(), tx, "", "group"))
		require.Equal(t, "bypass", tx.groups["group"].Mode)
		require.Equal(t, rivertype.JobStateRunning, tx.jobs[1].State)
	})

	t.Run("RepairRetainsTerminalPolicy", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.jobs[1] = protocolJob(1, "group", 50, rivertype.JobStateDiscarded)
		group, err := Repair(t.Context(), tx, "", "group")
		require.NoError(t, err)
		require.Equal(t, int64(51), group.NextPosition)
		require.Equal(t, "blocked", group.Mode)
		require.Equal(t, int64(1), *group.BlockedJobID)
	})

	t.Run("RestoreCountsResolvedRunningMembers", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.addGroup("group").Mode = "bypass"
		tx.jobs[1] = protocolJob(1, "group", 1, rivertype.JobStateRunning)
		tx.jobs[1].SequenceResolvedAt = new(time.Now())
		require.ErrorIs(t, Restore(t.Context(), tx, "", "group"), ErrNotDrained)
		tx.jobs[1].State = rivertype.JobStateCompleted
		require.NoError(t, Restore(t.Context(), tx, "", "group"))
		require.Equal(t, "ordered", tx.groups["group"].Mode)
	})

	t.Run("ResumeChecksObservedBarrierAndCurrentState", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		group := tx.addGroup("group")
		tx.jobs[1] = protocolJob(1, "group", 10, rivertype.JobStateDiscarded)
		setBarrier(group, tx.jobs[1], time.Now())
		require.ErrorIs(t, Resume(t.Context(), tx, "", "group", 2, 10), ErrStateConflict)
		tx.jobs[1].State = rivertype.JobStateRunning
		require.ErrorIs(t, Resume(t.Context(), tx, "", "group", 1, 10), ErrStateConflict)
		require.Nil(t, tx.jobs[1].SequenceResolvedAt)
		tx.jobs[1].State = rivertype.JobStateDiscarded
		require.NoError(t, Resume(t.Context(), tx, "", "group", 1, 10))
		require.NotNil(t, tx.jobs[1].SequenceResolvedAt)
		require.Equal(t, rivertype.JobStateDiscarded, tx.jobs[1].State)
		require.Equal(t, "ordered", group.Mode)
	})

	t.Run("ResumeExplicitlyRemovesDeletedBarrier", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		group := tx.addGroup("group")
		setBarrier(group, protocolJob(1, "group", 10, rivertype.JobStateCancelled), time.Now())
		require.NoError(t, Resume(t.Context(), tx, "", "group", 1, 10))
		require.Equal(t, "ordered", group.Mode)
	})

	t.Run("RetryRejectsSuccessorAndHistory", func(t *testing.T) {
		t.Parallel()

		tx := newProtocolTx()
		tx.addGroup("group")
		tx.jobs[1] = protocolJob(1, "group", 1, rivertype.JobStateRunning)
		tx.jobs[2] = protocolJob(2, "group", 2, rivertype.JobStatePending)
		_, err := Retry(t.Context(), tx, &riverdriver.JobRetryParams{ID: 2})
		require.ErrorIs(t, err, ErrNotHead)
		tx.jobs[1].SequenceResolvedAt = new(time.Now())
		tx.jobs[1].State = rivertype.JobStateCompleted
		_, err = Retry(t.Context(), tx, &riverdriver.JobRetryParams{ID: 1})
		require.ErrorIs(t, err, ErrAlreadyResolved)
	})
}

// TestTerminalDecision fixes the continuation policy independently of scheduling.
func TestTerminalDecision(t *testing.T) {
	t.Parallel()

	for _, state := range []rivertype.JobState{rivertype.JobStateCancelled, rivertype.JobStateDiscarded} {
		job := protocolJob(1, "group", 1, state)
		require.Equal(t, decisionBlock, terminalDecision(job))
		*job.SequenceContinueOnCancelled = true
		*job.SequenceContinueOnDiscarded = true
		require.Equal(t, decisionContinue, terminalDecision(job))
	}
	require.Equal(t, decisionInvalid, terminalDecision(&rivertype.JobRow{State: "unexpected"}))
}

// protocolTx is an isolated in-memory protocol fixture; integration tests cover
// actual transaction visibility, locks and rollback for both Postgres drivers.
type protocolTx struct {
	riverdriver.ExecutorTx
	riverdriver.SequenceExecutor

	groups        map[string]*riverdriver.SequenceGroup
	headLockError error
	jobs          map[int64]*rivertype.JobRow
	lockedGroups  []string
	lockedJobs    []int64
}

// newProtocolTx constructs independent fixture state for parallel cases.
func newProtocolTx() *protocolTx {
	return &protocolTx{groups: make(map[string]*riverdriver.SequenceGroup), jobs: make(map[int64]*rivertype.JobRow)}
}

// addGroup creates an ordered group with an unused allocator.
func (tx *protocolTx) addGroup(key string) *riverdriver.SequenceGroup {
	group := &riverdriver.SequenceGroup{Key: key, Mode: "ordered", NextPosition: 1}
	tx.groups[key] = group
	return group
}

// JobGetByID reads the fixture without taking a sequence lock.
func (tx *protocolTx) JobGetByID(_ context.Context, params *riverdriver.JobGetByIDParams) (*rivertype.JobRow, error) {
	if job := tx.jobs[params.ID]; job != nil {
		return job, nil
	}
	return nil, rivertype.ErrNotFound
}

// SequenceGroupEnsure models idempotent group creation.
func (tx *protocolTx) SequenceGroupEnsure(_ context.Context, params *riverdriver.SequenceGroupKeyParams) error {
	if tx.groups[params.Key] == nil {
		tx.addGroup(params.Key)
	}
	return nil
}

// SequenceGroupGet records the order in which protocol locks are acquired.
func (tx *protocolTx) SequenceGroupGet(_ context.Context, params *riverdriver.SequenceGroupGetParams) (*riverdriver.SequenceGroup, error) {
	if params.Lock {
		tx.lockedGroups = append(tx.lockedGroups, params.Key)
	}
	if group := tx.groups[params.Key]; group != nil {
		return group, nil
	}
	return nil, rivertype.ErrNotFound
}

// SequenceGroupUpdate persists only coordination fields in the fixture.
func (tx *protocolTx) SequenceGroupUpdate(_ context.Context, params *riverdriver.SequenceGroupUpdateParams) (*riverdriver.SequenceGroup, error) {
	tx.groups[params.Group.Key] = params.Group
	return params.Group, nil
}

// SequenceJobAttach refuses to reassign existing fixture members.
func (tx *protocolTx) SequenceJobAttach(_ context.Context, params *riverdriver.SequenceJobAttachParams) (*rivertype.JobRow, error) {
	job := tx.jobs[params.ID]
	if job == nil || job.SequenceKey != nil || job.State != rivertype.JobStatePending {
		return nil, rivertype.ErrNotFound
	}
	job.SequenceKey = &params.Key
	job.SequencePosition = &params.Position
	job.SequenceContinueOnCancelled = &params.ContinueOnCancelled
	job.SequenceContinueOnDiscarded = &params.ContinueOnDiscarded
	return job, nil
}

// SequenceJobGet records a lock on the exact requested member.
func (tx *protocolTx) SequenceJobGet(_ context.Context, params *riverdriver.SequenceJobGetParams) (*rivertype.JobRow, error) {
	tx.lockedJobs = append(tx.lockedJobs, params.ID)
	if job := tx.jobs[params.ID]; job != nil && job.SequenceKey != nil && *job.SequenceKey == params.Key {
		return job, nil
	}
	return nil, rivertype.ErrNotFound
}

// SequenceJobHead always selects the earliest unresolved position, including gaps.
func (tx *protocolTx) SequenceJobHead(_ context.Context, params *riverdriver.SequenceGroupKeyParams) (*rivertype.JobRow, error) {
	var jobs []*rivertype.JobRow
	for _, job := range tx.jobs {
		if job.SequenceKey != nil && *job.SequenceKey == params.Key && job.SequenceResolvedAt == nil {
			jobs = append(jobs, job)
		}
	}
	if len(jobs) == 0 {
		return nil, rivertype.ErrNotFound
	}
	slices.SortFunc(jobs, func(a, b *rivertype.JobRow) int {
		if *a.SequencePosition < *b.SequencePosition {
			return -1
		}
		return 1
	})
	if params.Lock {
		tx.lockedJobs = append(tx.lockedJobs, jobs[0].ID)
		if tx.headLockError != nil {
			return nil, tx.headLockError
		}
	}
	return jobs[0], nil
}

// SequenceJobRelease preserves a future schedule when granting eligibility.
func (tx *protocolTx) SequenceJobRelease(_ context.Context, params *riverdriver.SequenceJobMutationParams) (*rivertype.JobRow, error) {
	job := tx.jobs[params.ID]
	job.State = rivertype.JobStateAvailable
	if job.ScheduledAt.After(params.Now) {
		job.State = rivertype.JobStateScheduled
	}
	return job, nil
}

// SequenceJobResolve marks only the coordination obligation as retired.
func (tx *protocolTx) SequenceJobResolve(_ context.Context, params *riverdriver.SequenceJobMutationParams) (*rivertype.JobRow, error) {
	job := tx.jobs[params.ID]
	job.SequenceResolvedAt = &params.Now
	return job, nil
}

// SequenceJobStats includes resolved execution history in anomaly checks.
func (tx *protocolTx) SequenceJobStats(_ context.Context, params *riverdriver.SequenceGroupKeyParams) (*riverdriver.SequenceJobStats, error) {
	stats := &riverdriver.SequenceJobStats{}
	for _, job := range tx.jobs {
		if job.SequenceKey == nil || *job.SequenceKey != params.Key {
			continue
		}
		stats.MaxPosition = max(stats.MaxPosition, *job.SequencePosition)
		if isActive(job.State) || (job.State == rivertype.JobStatePending && job.SequenceResolvedAt != nil) {
			stats.Active++
		}
		if terminalDecision(job) == decisionWait {
			stats.Nonterminal++
			if job.SequenceResolvedAt != nil {
				stats.ResolvedNonterminal++
			}
		}
		if job.SequenceResolvedAt == nil {
			stats.Unresolved++
		}
	}
	return stats, nil
}

// protocolJob creates a valid member with conservative terminal policies.
func protocolJob(id int64, key string, position int64, state rivertype.JobState) *rivertype.JobRow {
	continueCancelled, continueDiscarded := false, false
	return &rivertype.JobRow{ID: id, State: state, SequenceKey: &key, SequencePosition: &position, SequenceContinueOnCancelled: &continueCancelled, SequenceContinueOnDiscarded: &continueDiscarded}
}
