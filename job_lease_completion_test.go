package river

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/joblease"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

// TestJobLeaseReclaimedJobCancelsPreviousExecution exercises completion followed
// by an administrative retry while the previous worker is still cleaning up.
func TestJobLeaseReclaimedJobCancelsPreviousExecution(t *testing.T) {
	t.Parallel()

	for _, clockRollback := range []bool{true, false} {
		name := "OrdinaryRetry"
		if clockRollback {
			name = "DatabaseClockRollback"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			bundle := setupSequenceIntegration(t)
			ctx := context.Background()
			inserted, err := bundle.client.Insert(ctx, noOpArgs{}, nil)
			require.NoError(t, err)
			exec := bundle.driver.GetExecutor()
			claim := func(now *time.Time) *rivertype.JobRow {
				claimed, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
					ClientID: "claim-ownership-test", LeaseDuration: 2 * time.Minute, MaxAttemptedBy: 10,
					MaxToLock: 1, Now: now, Queue: QueueDefault, Schema: bundle.schema,
				})
				require.NoError(t, err)
				require.Len(t, claimed.Jobs, 1)
				require.Equal(t, inserted.Job.ID, claimed.Jobs[0].ID)
				return claimed.Jobs[0]
			}
			config := (&joblease.Config{HeartbeatInterval: 20 * time.Millisecond}).WithDefaults()
			manager := joblease.NewManager(riversharedtest.BaseServiceArchetype(t), config, bundle.schema, exec.JobRenewLeaseMany)
			oldClaim := claim(nil)
			workCtx, cancelWork := context.WithCancelCause(ctx)
			defer cancelWork(nil)
			oldGuard := manager.Register(workCtx, oldClaim, time.Now().Add(-time.Second), cancelWork)
			defer oldGuard.Abandon()
			defer oldGuard.Finalize(config.FinalizeTimeout)
			oldDeadline := oldGuard.Deadline()
			workCtx = context.WithValue(workCtx, rivercommon.ContextKeyClient{}, bundle.client)
			workCtx = joblease.WithGuard(workCtx, oldGuard)
			tx, err := bundle.pool.Begin(workCtx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)
			_, err = JobCompleteTx[*riverpgxv5.Driver](workCtx, tx, &Job[noOpArgs]{JobRow: oldClaim})
			require.NoError(t, err)
			require.NoError(t, tx.Commit(ctx))
			// A has committed completion, but has not yet returned from Work.
			var newClaimAt *time.Time
			if clockRollback {
				// Override the database's statement time to model a clock rollback.
				// Lease expiration still uses the real database clock.
				newClaimAt = new(oldClaim.AttemptedAt.Add(-time.Second))
				_, err = exec.JobRetry(ctx, &riverdriver.JobRetryParams{ID: oldClaim.ID, Now: newClaimAt, Schema: bundle.schema})
			} else {
				_, err = bundle.client.JobRetry(ctx, oldClaim.ID)
			}
			require.NoError(t, err)
			newClaim := claim(newClaimAt)
			if clockRollback {
				require.True(t, newClaim.AttemptedAt.Before(*oldClaim.AttemptedAt))
			}
			require.Equal(t, oldClaim.Attempt+1, newClaim.Attempt)
			newGuard := manager.Register(ctx, newClaim, time.Now().Add(-time.Second), nil)
			defer newGuard.Abandon()
			defer newGuard.Finalize(config.FinalizeTimeout)
			require.ErrorIs(t, context.Cause(workCtx), ErrJobLeaseLost)
			require.ErrorIs(t, oldGuard.Check(), ErrJobLeaseLost)
			// A responds to cancellation only after B has been registered.
			oldGuard.Finalize(config.FinalizeTimeout)
			newDeadline := newGuard.Deadline()
			require.NoError(t, manager.Start(ctx))
			defer manager.Stop()
			require.Eventually(t, func() bool { return newGuard.Deadline().After(newDeadline) }, 3*time.Second, 5*time.Millisecond)
			require.Equal(t, oldDeadline, oldGuard.Deadline())
			require.NoError(t, newGuard.Check())
			require.True(t, manager.CanFetch())
			job, err := bundle.client.JobGet(ctx, newClaim.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, job.State)
			require.Equal(t, newClaim.Attempt, job.Attempt)
			require.Equal(t, newClaim.AttemptedAt, job.AttemptedAt)
		})
	}
}

// TestJobLeaseStaleCompletionKeepsSequenceBlocked models an already-admitted completion
// whose database-side execution is delayed until after rescue and a new claim.
func TestJobLeaseStaleCompletionKeepsSequenceBlocked(t *testing.T) {
	t.Parallel()

	bundle := setupSequenceIntegration(t)
	ctx := context.Background()
	rows, err := bundle.client.InsertMany(ctx, []InsertManyParams{
		{Args: sequenceKeyTestArgs{AccountID: 100, Action: "head"}},
		{Args: sequenceKeyTestArgs{AccountID: 100, Action: "tail"}},
	})
	require.NoError(t, err)
	key := *rows[0].Job.SequenceKey
	_, err = bundle.client.SequenceReconcile(ctx, key)
	require.NoError(t, err)
	exec := bundle.driver.GetExecutor()
	claim := func(clientID string) *rivertype.JobRow {
		claimed, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
			ClientID: clientID, LeaseDuration: time.Minute, MaxAttemptedBy: 10,
			MaxToLock: 1, Queue: QueueDefault, Schema: bundle.schema,
		})
		require.NoError(t, err)
		require.Len(t, claimed.Jobs, 1)
		require.Equal(t, rows[0].Job.ID, claimed.Jobs[0].ID)
		return claimed.Jobs[0]
	}
	oldClaim := claim("old-execution")
	guard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
	t.Cleanup(guard.Abandon)
	_, release, err := guard.Begin(ctx)
	require.NoError(t, err)
	defer release()
	// Capture the request admitted while A still holds its lease.
	completed := riverdriver.JobSetStateCompleted(oldClaim.ID, time.Now(), nil)
	oldRequest := &riverdriver.JobSetStateIfRunningManyParams{
		ID: []int64{completed.ID}, ExpectedAttempt: []*int{new(oldClaim.Attempt)}, ExpectedAttemptedAt: []*time.Time{oldClaim.AttemptedAt}, Attempt: []*int{completed.Attempt},
		ErrData: [][]byte{completed.ErrData}, FinalizedAt: []*time.Time{completed.FinalizedAt},
		MetadataDoMerge: []bool{completed.MetadataDoMerge}, MetadataUpdates: [][]byte{completed.MetadataUpdates},
		ScheduledAt: []*time.Time{completed.ScheduledAt}, State: []rivertype.JobState{completed.State}, Schema: bundle.schema,
	}
	guard.Abandon()
	require.ErrorIs(t, guard.Check(), ErrJobLeaseLost)
	// Advance only the persisted lease to exercise rescue without waiting.
	_, err = bundle.pool.Exec(ctx, "UPDATE "+pgx.Identifier{bundle.schema, "river_job"}.Sanitize()+" SET lease_expires_at=clock_timestamp()-interval '2 minutes' WHERE id=$1", oldClaim.ID)
	require.NoError(t, err)
	expired, err := bundle.client.JobGet(ctx, oldClaim.ID)
	require.NoError(t, err)
	_, err = exec.JobRescueMany(ctx, &riverdriver.JobRescueManyParams{
		ID: []int64{oldClaim.ID}, Error: [][]byte{[]byte(`{"error":"rescued"}`)}, FinalizedAt: []*time.Time{nil},
		LeaseExpiresAt: []*time.Time{expired.LeaseExpiresAt}, LeaseGracePeriod: time.Minute,
		ScheduledAt: []time.Time{oldClaim.ScheduledAt}, State: []string{"available"},
		StuckHorizon: time.Now().Add(-time.Hour), Schema: bundle.schema,
	})
	require.NoError(t, err)
	newClaim := claim("new-execution")
	require.Equal(t, oldClaim.Attempt+1, newClaim.Attempt)
	_, err = bundle.client.SequenceReconcile(ctx, key)
	require.NoError(t, err)
	tail, err := bundle.client.JobGet(ctx, rows[1].Job.ID)
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStatePending, tail.State)
	// A's old renewal is rejected, whereas its old completion has no identity
	// parameters. Model the latter running after cancellation failed to retract it.
	renewed, err := exec.JobRenewLeaseMany(ctx, &riverdriver.JobRenewLeaseManyParams{
		IDs: []int64{oldClaim.ID}, AttemptedAt: []time.Time{*oldClaim.AttemptedAt}, Attempts: []int{oldClaim.Attempt},
		LeaseDuration: time.Minute, Schema: bundle.schema,
	})
	require.NoError(t, err)
	require.Empty(t, renewed)
	rejected, err := exec.JobSetStateIfRunningMany(ctx, oldRequest)
	require.NoError(t, err)
	require.Empty(t, rejected)
	stored, err := bundle.client.JobGet(ctx, newClaim.ID)
	require.NoError(t, err)
	_, err = bundle.client.SequenceReconcile(ctx, key)
	require.NoError(t, err)
	tail, err = bundle.client.JobGet(ctx, rows[1].Job.ID)
	require.NoError(t, err)
	t.Logf("old attempt=%d; new attempt=%d; state after old completion=%s; successor=%s", oldClaim.Attempt, newClaim.Attempt, stored.State, tail.State)
	require.Equal(t, rivertype.JobStateRunning, stored.State, "the old completion must not finalize B's execution")
	require.Equal(t, rivertype.JobStatePending, tail.State, "the successor must wait for B's own completion")
}

// TestJobLeaseCompleteTxRejectsStaleClaim checks the transactional completion path.
func TestJobLeaseCompleteTxRejectsStaleClaim(t *testing.T) {
	t.Parallel()

	bundle := setupSequenceIntegration(t)
	ctx := context.WithValue(context.Background(), rivercommon.ContextKeyClient{}, bundle.client)
	rows, err := bundle.client.InsertMany(ctx, []InsertManyParams{{Args: sequenceKeyTestArgs{AccountID: 100, Action: "head"}}})
	require.NoError(t, err)
	_, err = bundle.client.SequenceReconcile(ctx, *rows[0].Job.SequenceKey)
	require.NoError(t, err)
	exec := bundle.driver.GetExecutor()
	claimed, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{ClientID: "worker", LeaseDuration: time.Minute, MaxAttemptedBy: 10, MaxToLock: 1, Queue: QueueDefault, Schema: bundle.schema})
	require.NoError(t, err)
	require.Len(t, claimed.Jobs, 1)
	job := claimed.Jobs[0]
	guard := joblease.NewGuard(time.Now().Add(time.Minute), nil)
	t.Cleanup(guard.Abandon)
	ctx = joblease.WithGuard(ctx, guard)
	// Change only the persisted claim to model a newer execution while the
	// caller's local state still admits its old transactional request.
	_, err = bundle.pool.Exec(ctx, "UPDATE "+pgx.Identifier{bundle.schema, "river_job"}.Sanitize()+" SET attempt=attempt+1 WHERE id=$1", job.ID)
	require.NoError(t, err)
	tx, err := bundle.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = JobCompleteTx[*riverpgxv5.Driver](ctx, tx, &Job[sequenceKeyTestArgs]{JobRow: job})
	require.ErrorIs(t, err, ErrJobLeaseLost)
	require.NoError(t, tx.Rollback(context.Background()))
	stored, err := bundle.client.JobGet(context.Background(), job.ID)
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStateRunning, stored.State)
	require.Equal(t, job.Attempt+1, stored.Attempt)
	require.NotNil(t, stored.LeaseExpiresAt)
}
