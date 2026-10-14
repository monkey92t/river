package river

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/joblease"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivertype"
)

func TestJobLeaseClient(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		client *Client[pgx.Tx]
		driver *leaseFailureDriver
	}
	setup := func(t *testing.T, workFunc func(context.Context, *Job[noOpArgs]) error) *testBundle {
		t.Helper()

		ctx := context.Background()
		rawDriver := riverpgxv5.New(riversharedtest.DBPool(ctx, t))
		driver := &leaseFailureDriver{Driver: rawDriver}
		config := newTestConfig(t, riverdbtest.TestSchema(ctx, t, rawDriver, nil))
		config.Workers = NewWorkers()
		AddWorker(config.Workers, WorkFunc(workFunc))
		config.Queues = map[string]QueueConfig{QueueDefault: {MaxWorkers: 1}}
		config.JobTimeout = -1
		config.LeaderElectionDisabled = true
		config.JobLease = &JobLeaseConfig{
			CancelGracePeriod: 50 * time.Millisecond,
			FinalizeTimeout:   time.Second,
			HeartbeatInterval: 50 * time.Millisecond,
			LeaseDuration:     time.Second,
			LocalSafetyMargin: 200 * time.Millisecond,
			RenewTimeout:      100 * time.Millisecond,
		}
		client, err := NewClient[pgx.Tx](driver, config)
		require.NoError(t, err)
		startClient(ctx, t, client)
		return &testBundle{client: client, driver: driver}
	}

	t.Run("CommittedRenewalWithLostResponseKeepsWorkRunning", func(t *testing.T) {
		t.Parallel()

		var started testsignal.TestSignal[*rivertype.JobRow]
		started.Init(t)
		finish := make(chan struct{})
		bundle := setup(t, func(ctx context.Context, job *Job[noOpArgs]) error {
			started.Signal(job.JobRow)
			select {
			case <-finish:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		// Release Work before the client's cleanup even if an assertion fails.
		t.Cleanup(func() { close(finish) })
		bundle.driver.loseResponse.Store(true)
		ctx := context.Background()
		inserted, err := bundle.client.Insert(ctx, noOpArgs{}, nil)
		require.NoError(t, err)
		initial := started.WaitOrTimeout()
		require.NotNil(t, initial.LeaseExpiresAt)
		require.Eventually(t, func() bool {
			job, err := bundle.client.JobGet(ctx, inserted.Job.ID)
			return err == nil && job.LeaseExpiresAt != nil && job.LeaseExpiresAt.After(initial.LeaseExpiresAt.Add(time.Second))
		}, 5*time.Second, 20*time.Millisecond)
		require.False(t, bundle.driver.loseResponse.Load(), "the committed renewal response was discarded")
		job, err := bundle.client.JobGet(ctx, inserted.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, job.State)
		require.Equal(t, initial.Attempt, job.Attempt)
		require.Equal(t, initial.AttemptedAt, job.AttemptedAt)
		require.Equal(t, inserted.Job.ScheduledAt, job.ScheduledAt)
		finish <- struct{}{}
		require.NoError(t, bundle.client.Stop(ctx))
		job, err = bundle.client.JobGet(ctx, inserted.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, job.State)
		require.Nil(t, job.LeaseExpiresAt)
	})

	t.Run("HealthyLongWorkKeepsOriginalSchedule", func(t *testing.T) {
		t.Parallel()

		var started testsignal.TestSignal[*rivertype.JobRow]
		started.Init(t)
		finish := make(chan struct{})
		bundle := setup(t, func(ctx context.Context, job *Job[noOpArgs]) error {
			started.Signal(job.JobRow)
			select {
			case <-finish:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		// Release Work before the client's cleanup even if an assertion fails.
		t.Cleanup(func() { close(finish) })
		ctx := context.Background()
		inserted, err := bundle.client.Insert(ctx, noOpArgs{}, nil)
		require.NoError(t, err)
		initial := started.WaitOrTimeout()
		require.NotNil(t, initial.LeaseExpiresAt)
		require.Eventually(t, func() bool {
			job, err := bundle.client.JobGet(ctx, inserted.Job.ID)
			return err == nil && job.LeaseExpiresAt != nil && job.LeaseExpiresAt.After(initial.LeaseExpiresAt.Add(time.Second))
		}, 5*time.Second, 20*time.Millisecond)
		job, err := bundle.client.JobGet(ctx, inserted.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, job.State)
		require.Equal(t, initial.Attempt, job.Attempt)
		require.Equal(t, initial.AttemptedAt, job.AttemptedAt)
		require.Equal(t, inserted.Job.ScheduledAt, job.ScheduledAt)
		// Completion while draining must retain renewal through its DB write.
		stopDone := make(chan error, 1)
		go func() { stopDone <- bundle.client.Stop(ctx) }()
		finish <- struct{}{}
		require.NoError(t, riversharedtest.WaitOrTimeout(t, stopDone))
		job, err = bundle.client.JobGet(ctx, inserted.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateCompleted, job.State)
		require.Nil(t, job.LeaseExpiresAt)
	})

	t.Run("LostLeaseCancelsWorkAndDiscardsLateSuccess", func(t *testing.T) {
		t.Parallel()

		var cancelled testsignal.TestSignal[error]
		cancelled.Init(t)
		bundle := setup(t, func(ctx context.Context, _ *Job[noOpArgs]) error {
			<-ctx.Done()
			cancelled.Signal(context.Cause(ctx))
			return nil
		})
		bundle.driver.fail.Store(true)
		ctx := context.Background()
		inserted, err := bundle.client.Insert(ctx, noOpArgs{}, nil)
		require.NoError(t, err)
		require.ErrorIs(t, cancelled.WaitOrTimeout(), ErrJobLeaseLost)
		require.NoError(t, bundle.client.Stop(ctx))
		job, err := bundle.client.JobGet(ctx, inserted.Job.ID)
		require.NoError(t, err)
		require.Equal(t, rivertype.JobStateRunning, job.State)
		require.NotNil(t, job.LeaseExpiresAt)
		require.Empty(t, job.Errors)
	})
}

func TestJobLeaseConfig(t *testing.T) {
	t.Parallel()

	t.Run("DefaultsAndTimeoutIndependence", func(t *testing.T) {
		t.Parallel()

		config := (&Config{JobLease: &JobLeaseConfig{}, JobTimeout: 10 * time.Hour, RescueStuckJobsAfter: time.Hour}).WithDefaults()
		require.NoError(t, config.validate())
		require.Equal(t, 20*time.Second, config.JobLease.HeartbeatInterval)
		require.Equal(t, 120*time.Second, config.JobLease.LeaseDuration)
		require.Equal(t, 60*time.Second, config.JobLease.RescueGracePeriod)
		config.JobLease = nil
		require.ErrorContains(t, config.validate(), "RescueStuckJobsAfter cannot be less than JobTimeout")
	})

	t.Run("RejectsUnsafeDurations", func(t *testing.T) {
		t.Parallel()

		config := (&JobLeaseConfig{LeaseDuration: 30 * time.Second}).WithDefaults()
		require.ErrorContains(t, config.Validate(), "LeaseDuration must exceed")
		config = (&JobLeaseConfig{LocalSafetyMargin: 5 * time.Second}).WithDefaults()
		require.ErrorContains(t, config.Validate(), "LocalSafetyMargin must be greater")
		config = (&JobLeaseConfig{HeartbeatInterval: -time.Second}).WithDefaults()
		require.ErrorContains(t, config.Validate(), "HeartbeatInterval must be greater")
	})
}

func TestJobLeaseProducerInstance(t *testing.T) {
	t.Parallel()

	_, state, err := (&riverpilot.StandardPilot{}).ProducerInit(context.Background(), nil, nil)
	require.NoError(t, err)
	producer := &producer{activeJobs: make(map[int64]*jobexecutor.JobExecutor), state: state}
	oldExecutor := &jobexecutor.JobExecutor{JobRow: &rivertype.JobRow{ID: 1}}
	newExecutor := &jobexecutor.JobExecutor{JobRow: &rivertype.JobRow{ID: 1}}
	producer.addActiveJob(1, oldExecutor)
	producer.addActiveJob(1, newExecutor)
	producer.removeActiveJob(oldExecutor)
	require.Same(t, newExecutor, producer.activeJobs[1])
	require.Equal(t, int32(1), producer.numJobsActive.Load())
	producer.removeActiveJob(newExecutor)
	require.Empty(t, producer.activeJobs)
	require.Zero(t, producer.numJobsActive.Load())
}

func TestJobLeaseRejectsLateWorkerWrites(t *testing.T) {
	t.Parallel()

	guard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
	guard.Abandon()
	ctx := joblease.WithGuard(context.WithoutCancel(context.Background()), guard)
	job := &Job[noOpArgs]{JobRow: &rivertype.JobRow{ID: 1, State: rivertype.JobStateRunning}}
	_, err := JobCompleteTx[*riverpgxv5.Driver](ctx, pgx.Tx(nil), job)
	require.ErrorIs(t, err, ErrJobLeaseLost)
	_, err = ResumableSetStepTx[*riverpgxv5.Driver](ctx, pgx.Tx(nil), job)
	require.ErrorIs(t, err, ErrJobLeaseLost)
	_, err = ResumableSetStepCursorTx[*riverpgxv5.Driver](ctx, pgx.Tx(nil), job, 1)
	require.ErrorIs(t, err, ErrJobLeaseLost)
	_, err = (&Client[pgx.Tx]{}).jobUpdate(ctx, nil, job.ID, &JobUpdateParams{Output: "late"})
	require.ErrorIs(t, err, ErrJobLeaseLost)
	require.ErrorIs(t, RecordOutput(ctx, "late"), ErrJobLeaseLost)
	require.ErrorIs(t, ResumableSetCursor(ctx, 1), ErrJobLeaseLost)

	job.LeaseExpiresAt = new(time.Now().Add(time.Minute))
	_, err = JobCompleteTx[*riverpgxv5.Driver](context.Background(), pgx.Tx(nil), job)
	require.ErrorIs(t, err, ErrJobLeaseLost)
	_, err = ResumableSetStepTx[*riverpgxv5.Driver](context.Background(), pgx.Tx(nil), job)
	require.ErrorIs(t, err, ErrJobLeaseLost)
}

// leaseFailureDriver injects renewal failures without disrupting other queries.
type leaseFailureDriver struct {
	*riverpgxv5.Driver

	fail         atomic.Bool
	loseResponse atomic.Bool
}

// GetExecutor wraps only lease renewals, preserving the regular job operations.
func (d *leaseFailureDriver) GetExecutor() riverdriver.Executor {
	return &leaseFailureExecutor{Executor: d.Driver.GetExecutor(), fail: &d.fail, loseResponse: &d.loseResponse}
}

// leaseFailureExecutor simulates a worker that cannot confirm database heartbeats.
type leaseFailureExecutor struct {
	riverdriver.Executor

	fail         *atomic.Bool
	loseResponse *atomic.Bool
}

// JobRenewLeaseMany injects connection failures or discards a committed response.
func (e *leaseFailureExecutor) JobRenewLeaseMany(ctx context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
	if e.fail.Load() {
		return nil, errors.New("injected renewal connection failure")
	}
	rows, err := e.Executor.JobRenewLeaseMany(ctx, params)
	if err == nil && len(rows) > 0 && e.loseResponse.Swap(false) {
		return nil, errors.New("injected response loss after renewal commit")
	}
	return rows, err
}
