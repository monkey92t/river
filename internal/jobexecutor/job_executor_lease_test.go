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
	"github.com/riverqueue/river/internal/workunit"
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

// timeoutOrderWorkUnit verifies that timeout resolution sees decoded job args.
type timeoutOrderWorkUnit struct {
	workunit.WorkUnit

	t           *testing.T
	unmarshaled bool
}

// Timeout checks the ordering required by workers with argument-dependent timeouts.
func (w *timeoutOrderWorkUnit) Timeout() time.Duration {
	require.True(w.t, w.unmarshaled)
	return w.WorkUnit.Timeout()
}

// UnmarshalJob records successful decoding before timeout resolution.
func (w *timeoutOrderWorkUnit) UnmarshalJob() error {
	err := w.WorkUnit.UnmarshalJob()
	w.unmarshaled = err == nil
	return err
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

func TestJobExecutor_TimeoutCoversExecutionChain(t *testing.T) {
	t.Parallel()

	for _, leased := range []bool{false, true} {
		leaseName := "WithoutLease"
		if leased {
			leaseName = "WithLease"
		}
		for _, cooperative := range []bool{true, false} {
			behavior := "Cooperative"
			if !cooperative {
				behavior = "Uncooperative"
			}
			for _, phase := range []string{"MiddlewareAfterWork", "MiddlewareBeforeWork", "WorkBegin", "WorkEnd"} {
				t.Run(leaseName+"/"+behavior+"/"+phase, func(t *testing.T) {
					t.Parallel()

					synctest.Test(t, func(t *testing.T) {
						config := (&joblease.Config{CancelGracePeriod: 2 * time.Second, HeartbeatInterval: 200 * time.Millisecond}).WithDefaults()
						ctx, cancel := context.WithCancelCause(context.Background())
						var guard *joblease.Guard
						var manager *joblease.Manager
						renewals := 0
						if leased {
							manager = joblease.NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(_ context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
								renewals++
								return []*rivertype.JobRow{{ID: params.IDs[0], LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}}, nil
							})
							require.NoError(t, manager.Start(context.Background()))
							guard = manager.Register(ctx, &rivertype.JobRow{ID: 1, Attempt: 3, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), cancel)
							ctx = joblease.WithGuard(ctx, guard)
						}
						defer func() {
							guard.Finalize(config.FinalizeTimeout)
							guard.Abandon()
							if manager != nil {
								manager.Stop()
							}
							cancel(nil)
							synctest.Wait()
						}()
						startedAt := time.Now()
						workStarted := false
						block := func(ctx context.Context) error {
							deadline, hasDeadline := ctx.Deadline()
							require.True(t, hasDeadline)
							require.Equal(t, startedAt.Add(time.Second), deadline)
							if cooperative {
								select {
								case <-ctx.Done():
								case <-time.After(5 * time.Second):
									return errors.New("execution deadline did not cancel phase")
								}
							} else {
								// Ignore cancellation long enough to exhaust the lease cancellation budget.
								time.Sleep(5 * time.Second)
							}
							return ctx.Err()
						}
						executor := newLeaseTestExecutor(t, cancel, func() error { workStarted = true; return nil })
						executor.ClientJobTimeout = time.Second
						executor.ClientRetryPolicy = &retrypolicytest.RetryPolicyNoJitter{}
						executor.JobRow.Attempt = 3
						executor.JobRow.AttemptedAt = new(startedAt)
						if leased {
							executor.JobRow.LeaseExpiresAt = new(startedAt.Add(config.LeaseDuration))
						}
						executor.JobRow.MaxAttempts = 10
						executor.LeaseCancelGracePeriod = config.CancelGracePeriod
						completer := &leaseResultCompleter{}
						executor.Completer = completer
						switch phase {
						case "MiddlewareAfterWork", "MiddlewareBeforeWork":
							executor.PluginLookupGlobal = pluginlookup.NewPluginLookup([]any{&testMiddleware{work: func(ctx context.Context, _ *rivertype.JobRow, next func(context.Context) error) error {
								if phase == "MiddlewareAfterWork" {
									if err := next(ctx); err != nil {
										return err
									}
								}
								return block(ctx)
							}}})
						case "WorkBegin":
							executor.PluginLookupGlobal = pluginlookup.NewPluginLookup([]any{HookWorkBeginFunc(func(ctx context.Context, _ *rivertype.JobRow) error { return block(ctx) })})
						case "WorkEnd":
							executor.PluginLookupGlobal = pluginlookup.NewPluginLookup([]any{HookWorkEndFunc(func(ctx context.Context, _ *rivertype.JobRow, _ error) error { return block(ctx) })})
						default:
							t.Fatalf("unknown execution phase: %s", phase)
						}
						executor.Execute(ctx)
						synctest.Wait()
						require.Equal(t, phase == "MiddlewareAfterWork" || phase == "WorkEnd", workStarted)
						if leased && !cooperative {
							require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
							require.Nil(t, completer.params, "abandoned executions must not submit results")
							renewalsAtReturn := renewals
							time.Sleep(config.HeartbeatInterval * 3)
							synctest.Wait()
							require.Equal(t, renewalsAtReturn, renewals)
						} else {
							require.NotNil(t, completer.params)
							require.Equal(t, rivertype.JobStateRetryable, completer.params.State)
							require.Contains(t, string(completer.params.ErrData), context.DeadlineExceeded.Error())
						}
						if leased {
							require.Positive(t, renewals, "heartbeats must work before timeout")
						}
					})
				})
			}
		}
	}
}

func TestJobExecutor_TimeoutOptions(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		client  time.Duration
		worker  time.Duration
		timeout time.Duration
	}{
		{"ClientDisabled", -1, 0, -1},
		{"DetachedMiddleware", time.Second, 0, time.Second},
		{"Inherited", 2 * time.Second, 0, 2 * time.Second},
		{"WorkerDisabled", time.Second, -1, -1},
		{"WorkerOverridesClient", time.Second, 3 * time.Second, 3 * time.Second},
		{"WorkerOverridesDisabledClient", -1, time.Second, time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				executor := newLeaseTestExecutor(t, cancel, func() error { return nil })
				executor.ClientJobTimeout = test.client
				unit, ok := executor.WorkUnit.(*customizableWorkUnit)
				require.True(t, ok)
				unit.timeout = test.worker
				executor.WorkUnit = &timeoutOrderWorkUnit{WorkUnit: unit, t: t}
				completer := &leaseResultCompleter{}
				executor.Completer = completer
				startedAt := time.Now()
				if test.name == "DetachedMiddleware" {
					unit.middleware = []rivertype.WorkerMiddleware{&testMiddleware{work: func(ctx context.Context, _ *rivertype.JobRow, next func(context.Context) error) error {
						// Time spent before doInner must not restart the execution budget.
						time.Sleep(time.Second / 4)
						return next(context.WithoutCancel(ctx))
					}}}
				}
				executor.PluginLookupGlobal = pluginlookup.NewPluginLookup([]any{HookWorkBeginFunc(func(ctx context.Context, _ *rivertype.JobRow) error {
					deadline, hasDeadline := ctx.Deadline()
					if test.timeout > 0 {
						require.True(t, hasDeadline)
						require.Equal(t, startedAt.Add(test.timeout), deadline)
						time.Sleep(test.timeout / 2)
					} else {
						require.False(t, hasDeadline)
						time.Sleep(3 * time.Second)
					}
					return ctx.Err()
				})})
				executor.Execute(ctx)
				require.NotNil(t, completer.params)
				require.Equal(t, rivertype.JobStateCompleted, completer.params.State)
				synctest.Wait()
			})
		})
	}
}

func TestJobExecutor_TimeoutPreservesEarlyFailureRetryPolicy(t *testing.T) {
	t.Parallel()

	for _, phase := range []string{"MiddlewareBeforeWork", "WorkBegin"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.Background())
				defer cancel(nil)
				executor := newLeaseTestExecutor(t, cancel, func() error { t.Error("early failure must not start Work"); return nil })
				executor.ClientJobTimeout = time.Second
				executor.ClientRetryPolicy = &retrypolicytest.RetryPolicyNoJitter{}
				executor.JobRow.Attempt = 3
				executor.JobRow.AttemptedAt = new(time.Now())
				executor.JobRow.MaxAttempts = 10
				unit, ok := executor.WorkUnit.(*customizableWorkUnit)
				require.True(t, ok)
				unit.nextRetry = func() time.Time {
					t.Error("early failures must continue using the client retry policy")
					return time.Now().Add(time.Hour)
				}
				if phase == "WorkBegin" {
					executor.PluginLookupGlobal = pluginlookup.NewPluginLookup([]any{HookWorkBeginFunc(func(context.Context, *rivertype.JobRow) error { return errors.New("early failure") })})
				} else {
					unit.middleware = []rivertype.WorkerMiddleware{&testMiddleware{work: func(context.Context, *rivertype.JobRow, func(context.Context) error) error {
						return errors.New("early failure")
					}}}
				}
				completer := &leaseResultCompleter{}
				executor.Completer = completer
				executor.Execute(ctx)
				require.NotNil(t, completer.params)
				require.Equal(t, rivertype.JobStateRetryable, completer.params.State)
				require.Equal(t, executor.ClientRetryPolicy.NextRetry(executor.JobRow), *completer.params.ScheduledAt)
				synctest.Wait()
			})
		})
	}
}

func TestJobExecutor_TimeoutRespectsMiddlewareCancellation(t *testing.T) {
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
		executor.ClientJobTimeout = time.Hour
		executor.LeaseCancelGracePeriod = 2 * time.Second
		unit, ok := executor.WorkUnit.(*customizableWorkUnit)
		require.True(t, ok)
		unit.middleware = []rivertype.WorkerMiddleware{&testMiddleware{work: func(ctx context.Context, _ *rivertype.JobRow, next func(context.Context) error) error {
			ctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			return next(ctx)
		}}}
		executor.Execute(joblease.WithGuard(ctx, guard))
		require.ErrorIs(t, context.Cause(ctx), rivertype.ErrJobLeaseLost)
	})
}

// newLeaseTestExecutor builds an executor with overridable completion handling.
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
