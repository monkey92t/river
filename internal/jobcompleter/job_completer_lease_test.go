package jobcompleter

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/joblease"
	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

func TestBatchCompleter_Lease(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		completer *BatchCompleter
		exec      *partialExecutorMock
		updates   chan []CompleterJobUpdated
	}
	setup := func(t *testing.T) *testBundle {
		t.Helper()

		exec := &partialExecutorMock{}
		updates := make(chan []CompleterJobUpdated, 10)
		completer := NewBatchCompleter(riversharedtest.BaseServiceArchetype(t), "", exec, &riverpilot.StandardPilot{}, updates)
		completer.disableSleep = true
		return &testBundle{completer: completer, exec: exec, updates: updates}
	}
	enqueue := func(t *testing.T, bundle *testBundle, id int64, guard *joblease.Guard) {
		t.Helper()
		require.NoError(t, bundle.completer.JobSetStateIfRunning(joblease.WithGuard(context.Background(), guard), &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(id, time.Now(), nil)))
	}

	t.Run("ConfirmedSubBatchPublishesBeforeLaterFailure", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		bundle.completer.completionMaxSize = 1
		guards := make(map[int64]*joblease.Guard)
		for _, id := range []int64{1, 2} {
			guard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
			defer guard.Abandon()
			guards[id] = guard
			enqueue(t, bundle, id, guard)
		}
		var confirmedID int64
		expectedErr := errors.New("second sub-batch failed")
		bundle.exec.JobSetStateIfRunningManyFunc = func(_ context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
			if confirmedID == 0 {
				confirmedID = params.ID[0]
				return []*rivertype.JobRow{{ID: confirmedID, State: rivertype.JobStateCompleted}}, nil
			}
			return nil, expectedErr
		}

		require.ErrorIs(t, bundle.completer.handleBatch(context.Background()), expectedErr)
		require.True(t, guards[confirmedID].Finished())
		require.Len(t, bundle.updates, 1)
		events := <-bundle.updates
		require.Len(t, events, 1)
		require.Equal(t, confirmedID, events[0].Job.ID)
		require.Len(t, bundle.completer.setStateParams, 1)
		require.NotContains(t, bundle.completer.setStateParams, confirmedID)
	})

	t.Run("ExpiredQueueEntryIsDropped", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		guard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
		defer guard.Abandon()
		enqueue(t, bundle, 1, guard)
		guard.Abandon()

		require.NoError(t, bundle.completer.handleBatch(context.Background()))
		require.False(t, bundle.exec.JobSetStateIfRunningManyCalled)
		require.Empty(t, bundle.completer.setStateParams)
		require.Empty(t, bundle.updates)
	})

	t.Run("InFlightCancellationRequeuesLiveMembers", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			bundle := setup(t)
			lost := joblease.NewGuard(time.Now().Add(time.Minute), nil)
			defer lost.Abandon()
			live := joblease.NewGuard(time.Now().Add(time.Minute), nil)
			defer live.Abandon()
			enqueue(t, bundle, 1, lost)
			enqueue(t, bundle, 2, live)
			calls := 0
			bundle.exec.JobSetStateIfRunningManyFunc = func(ctx context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
				calls++
				if calls == 1 {
					lost.Abandon()
					<-ctx.Done()
					return nil, ctx.Err()
				}
				require.Equal(t, []int64{2}, params.ID)
				return []*rivertype.JobRow{{ID: 2, State: rivertype.JobStateCompleted}}, nil
			}

			require.ErrorIs(t, bundle.completer.handleBatch(context.Background()), rivertype.ErrJobLeaseLost)
			require.Equal(t, 1, calls)
			require.Len(t, bundle.completer.setStateParams, 1)
			require.Same(t, live, bundle.completer.setStateParams[2].Guard)
			require.NoError(t, bundle.completer.handleBatch(context.Background()))
			require.True(t, live.Finished())
			require.Len(t, bundle.updates, 1)
		})
	})

	t.Run("LateConfirmationDoesNotPublishOrRevive", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		guard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
		defer guard.Abandon()
		enqueue(t, bundle, 1, guard)
		bundle.exec.JobSetStateIfRunningManyFunc = func(context.Context, *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
			guard.Abandon()
			return []*rivertype.JobRow{{ID: 1, State: rivertype.JobStateCompleted}}, nil
		}

		require.NoError(t, bundle.completer.handleBatch(context.Background()))
		require.False(t, guard.Finished())
		require.Empty(t, bundle.updates)
	})

	t.Run("OldRetryCannotReplaceNewExecution", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		oldGuard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
		defer oldGuard.Abandon()
		newGuard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
		defer newGuard.Abandon()
		enqueue(t, bundle, 1, oldGuard)
		bundle.exec.JobSetStateIfRunningManyFunc = func(context.Context, *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
			oldGuard.Abandon()
			enqueue(t, bundle, 1, newGuard)
			return nil, errors.New("connection lost")
		}

		require.ErrorIs(t, bundle.completer.handleBatch(context.Background()), rivertype.ErrJobLeaseLost)
		require.Len(t, bundle.completer.setStateParams, 1)
		require.Same(t, newGuard, bundle.completer.setStateParams[1].Guard)
	})

	t.Run("RejectedClaimStopsRenewal", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		guard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
		defer guard.Abandon()
		guard.Finalize(30 * time.Second)
		enqueue(t, bundle, 1, guard)
		require.False(t, guard.Finished())
		require.NoError(t, guard.Check())
		bundle.exec.JobSetStateIfRunningManyFunc = func(context.Context, *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
			return nil, nil // No row can also mean the execution claim was rejected.
		}

		require.NoError(t, bundle.completer.handleBatch(context.Background()))
		require.False(t, guard.Finished())
		require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
		require.Empty(t, bundle.updates)
	})
}

// TestBatchCompleter_ClaimConditions checks identity propagation through splitting
// and retries without publishing an event for a rejected member.
func TestBatchCompleter_ClaimConditions(t *testing.T) {
	t.Parallel()

	exec := &partialExecutorMock{}
	updates := make(chan []CompleterJobUpdated, 10)
	completer := NewBatchCompleter(riversharedtest.BaseServiceArchetype(t), "", exec, &riverpilot.StandardPilot{}, updates)
	completer.completionMaxSize = 1
	completer.disableSleep = true
	guards := make(map[int64]*joblease.Guard)
	claimedAt := time.Now()
	for _, id := range []int64{1, 2} {
		guard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
		defer guard.Abandon()
		guards[id] = guard
		params := riverdriver.JobSetStateCompleted(id, time.Now(), nil)
		params.SetExpectedClaim(&rivertype.JobRow{Attempt: int(id), AttemptedAt: &claimedAt, LeaseExpiresAt: new(time.Now().Add(time.Minute))})
		require.NoError(t, completer.JobSetStateIfRunning(joblease.WithGuard(context.Background(), guard), &jobstats.JobStatistics{}, params))
	}
	calls := 0
	exec.JobSetStateIfRunningManyFunc = func(_ context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
		require.Len(t, params.ID, 1)
		require.Equal(t, int(params.ID[0]), *params.ExpectedAttempt[0])
		require.Equal(t, claimedAt, *params.ExpectedAttemptedAt[0])
		calls++
		if calls == 1 {
			return nil, errors.New("response lost")
		}
		if params.ID[0] == 1 {
			return nil, nil // The stale member must not produce an event.
		}
		return []*rivertype.JobRow{{ID: 2, State: rivertype.JobStateCompleted}}, nil
	}

	require.NoError(t, completer.handleBatch(context.Background()))
	require.Equal(t, 3, calls)
	require.ErrorIs(t, guards[1].Check(), rivertype.ErrJobLeaseLost)
	require.True(t, guards[2].Finished())
	require.Len(t, updates, 1)
	events := <-updates
	require.Len(t, events, 1)
	require.Equal(t, int64(2), events[0].Job.ID)
}

// TestInlineCompleter_ClaimRejected verifies that zero-row lease rejection isn't
// presented as a successful completion.
func TestInlineCompleter_ClaimRejected(t *testing.T) {
	t.Parallel()

	guard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
	defer guard.Abandon()
	updates := make(chan []CompleterJobUpdated, 1)
	exec := &partialExecutorMock{JobSetStateIfRunningManyFunc: func(_ context.Context, params *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
		require.Equal(t, 3, *params.ExpectedAttempt[0])
		return nil, nil
	}}
	completer := NewInlineCompleter(riversharedtest.BaseServiceArchetype(t), "", exec, &riverpilot.StandardPilot{}, updates)
	params := riverdriver.JobSetStateCompleted(1, time.Now(), nil)
	params.SetExpectedClaim(&rivertype.JobRow{Attempt: 3, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(time.Minute))})
	err := completer.JobSetStateIfRunning(joblease.WithGuard(context.Background(), guard), &jobstats.JobStatistics{}, params)
	require.ErrorIs(t, err, rivertype.ErrJobLeaseLost)
	require.False(t, guard.Finished())
	require.Empty(t, updates)
}

func TestInlineCompleter_LeaseStopsRetry(t *testing.T) {
	t.Parallel()

	guard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
	defer guard.Abandon()
	calls := 0
	exec := &partialExecutorMock{JobSetStateIfRunningManyFunc: func(context.Context, *riverdriver.JobSetStateIfRunningManyParams) ([]*rivertype.JobRow, error) {
		calls++
		guard.Abandon()
		return nil, errors.New("connection lost")
	}}
	updates := make(chan []CompleterJobUpdated, 1)
	completer := NewInlineCompleter(riversharedtest.BaseServiceArchetype(t), "", exec, &riverpilot.StandardPilot{}, updates)
	completer.disableSleep = true
	ctx := joblease.WithGuard(context.Background(), guard)

	err := completer.JobSetStateIfRunning(ctx, &jobstats.JobStatistics{}, riverdriver.JobSetStateCompleted(1, time.Now(), nil))

	require.ErrorIs(t, err, rivertype.ErrJobLeaseLost)
	require.Equal(t, 1, calls)
	require.Empty(t, updates)
}
