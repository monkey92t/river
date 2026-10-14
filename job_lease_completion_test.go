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
	"github.com/riverqueue/river/rivertype"
)

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
