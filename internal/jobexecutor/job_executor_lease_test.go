package jobexecutor

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/joblease"
	"github.com/riverqueue/river/internal/pluginlookup"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

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
