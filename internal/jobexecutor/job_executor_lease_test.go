package jobexecutor

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/joblease"
	"github.com/riverqueue/river/internal/jobstats"
	"github.com/riverqueue/river/internal/pluginlookup"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverinternaltest/retrypolicytest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

// leaseResultCompleter records the execution result admitted by the executor.
type leaseResultCompleter struct {
	jobcompleter.JobCompleter

	params *riverdriver.JobSetStateIfRunningParams
}

// JobSetStateIfRunning captures the claim conditions attached to the result.
func (c *leaseResultCompleter) JobSetStateIfRunning(_ context.Context, _ *jobstats.JobStatistics, params *riverdriver.JobSetStateIfRunningParams) error {
	c.params = params
	return nil
}

func TestJobExecutor_LeaseAbandoned(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		result func() error
	}{
		{name: "Cancel", result: func() error { return rivertype.JobCancel(errors.New("cancel")) }},
		{name: "Error", result: func() error { return errors.New("failed") }},
		{name: "Panic", result: func() error { panic("failed") }},
		{name: "Snooze", result: func() error { return &rivertype.JobSnoozeError{} }},
		{name: "Success", result: func() error { return nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			guard := joblease.NewGuard(time.Now().Add(time.Minute), cancel)
			defer guard.Abandon()
			ctx = joblease.WithGuard(ctx, guard)
			executor := newLeaseTestExecutor(t, cancel, func() error {
				guard.Abandon()
				return test.result()
			})
			jobDone := false
			executor.ProducerCallbacks.JobDone = func(*rivertype.JobRow) { jobDone = true }

			// A nil completer makes any attempted late result submission fail.
			executor.Execute(ctx)

			require.True(t, jobDone)
			require.ErrorIs(t, context.Cause(ctx), rivertype.ErrJobLeaseLost)
		})
	}
}

// TestJobExecutor_LeaseClaimOnResults covers every execution-result path,
// including transitions that refund Attempt while preserving ExpectedAttempt.
func TestJobExecutor_LeaseClaimOnResults(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		result   func() error
		softStop bool
		state    rivertype.JobState
	}{
		{name: "Cancel", result: func() error { return rivertype.JobCancel(errors.New("cancel")) }, state: rivertype.JobStateCancelled},
		{name: "Error", result: func() error { return errors.New("failed") }, state: rivertype.JobStateRetryable},
		{name: "Panic", result: func() error { panic("failed") }, state: rivertype.JobStateRetryable},
		{name: "Snooze", result: func() error { return &rivertype.JobSnoozeError{Duration: time.Hour} }, state: rivertype.JobStateScheduled},
		{name: "SoftStop", result: func() error { return context.Canceled }, softStop: true, state: rivertype.JobStateAvailable},
		{name: "Success", result: func() error { return nil }, state: rivertype.JobStateCompleted},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			guard := joblease.NewGuard(time.Now().Add(time.Minute), cancel)
			defer guard.Abandon()
			executor := newLeaseTestExecutor(t, cancel, func() error {
				if test.softStop {
					cancel(rivercommon.ErrStop)
				}
				return test.result()
			})
			executor.ClientRetryPolicy = &retrypolicytest.RetryPolicyNoJitter{}
			executor.JobRow.Attempt = 3
			executor.JobRow.AttemptedAt = new(time.Now())
			executor.JobRow.LeaseExpiresAt = new(time.Now().Add(time.Minute))
			executor.JobRow.MaxAttempts = 10
			completer := &leaseResultCompleter{}
			executor.Completer = completer

			executor.Execute(joblease.WithGuard(ctx, guard))

			require.NotNil(t, completer.params)
			require.Equal(t, test.state, completer.params.State)
			require.Equal(t, 3, *completer.params.ExpectedAttempt)
			require.Equal(t, executor.JobRow.AttemptedAt, completer.params.ExpectedAttemptedAt)
			if test.name == "Snooze" || test.softStop {
				require.Equal(t, 2, *completer.params.Attempt)
			}
		})
	}
}

func TestJobExecutor_LeaseExpiredBeforeWork(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	guard := joblease.NewGuard(time.Now().Add(-time.Second), cancel)
	defer guard.Abandon()
	executor := newLeaseTestExecutor(t, cancel, func() error {
		t.Fatal("expired execution started Work")
		return nil
	})

	executor.Execute(joblease.WithGuard(ctx, guard))

	require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
}

func TestJobExecutor_LeaseRejectsMultipleJobResults(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	guard := joblease.NewGuard(time.Now().Add(time.Minute), cancel)
	defer guard.Abandon()
	executor := newLeaseTestExecutor(t, cancel, func() error {
		return &errorBundle{jobs: []*rivertype.JobRow{{ID: 1}, {ID: 2}}}
	})

	executor.Execute(joblease.WithGuard(ctx, guard))

	require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
}

func TestJobExecutor_LeaseStopsForUncooperativeTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		guard := joblease.NewGuard(time.Now().Add(time.Hour), cancel)
		defer guard.Abandon()
		executor := newLeaseTestExecutor(t, cancel, func() error {
			time.Sleep(5 * time.Second)
			require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
			return nil
		})
		executor.ClientJobTimeout = time.Second
		executor.LeaseCancelGracePeriod = 2 * time.Second

		executor.Execute(joblease.WithGuard(ctx, guard))
	})
}

// newLeaseTestExecutor builds an executor whose completer must never be called.
func newLeaseTestExecutor(t *testing.T, cancel context.CancelCauseFunc, work func() error) *JobExecutor {
	t.Helper()

	executor := baseservice.Init(riversharedtest.BaseServiceArchetype(t), &JobExecutor{
		CancelFunc:         cancel,
		JobRow:             &rivertype.JobRow{ID: 1, ScheduledAt: time.Now()},
		PluginLookupByJob:  pluginlookup.NewJobPluginLookup(nil),
		PluginLookupGlobal: pluginlookup.NewPluginLookup(nil),
		WorkUnit:           &customizableWorkUnit{work: work},
	})
	executor.ProducerCallbacks.JobDone = func(*rivertype.JobRow) {}
	executor.ProducerCallbacks.Stuck = func(context.Context, *rivertype.JobRow) {}
	executor.ProducerCallbacks.Unstuck = func() {}
	return executor
}
