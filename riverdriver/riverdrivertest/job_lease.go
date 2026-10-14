package riverdrivertest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivertype"
)

// exerciseJobLease verifies that lease ownership and rescue use database time
// without changing job ordering, attempt accounting, or legacy rescue behavior.
func exerciseJobLease[TTx any](ctx context.Context, t *testing.T, driverWithSchema func(context.Context, *testing.T, *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string), executorWithTx func(context.Context, *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	setExpiry := func(t *testing.T, exec riverdriver.Executor, driver riverdriver.Driver[TTx], id int64, expiry time.Time) *rivertype.JobRow {
		t.Helper()

		if driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
			require.NoError(t, exec.Exec(ctx, "UPDATE river_job SET lease_expires_at = ? WHERE id = ?", expiry.UTC().Round(time.Millisecond).Format("2006-01-02 15:04:05.000"), id))
		} else {
			require.NoError(t, exec.Exec(ctx, "UPDATE river_job SET lease_expires_at = $1 WHERE id = $2", expiry, id))
		}
		job, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: id})
		require.NoError(t, err)
		return job
	}

	claim := func(t *testing.T, exec riverdriver.Executor, duration time.Duration) *rivertype.JobRow {
		t.Helper()

		job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{})
		result, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
			ClientID:       testClientID,
			LeaseDuration:  duration,
			MaxAttemptedBy: 10,
			MaxToLock:      1,
			Queue:          job.Queue,
		})
		require.NoError(t, err)
		require.Len(t, result.Jobs, 1)
		require.Equal(t, job.ID, result.Jobs[0].ID)
		return result.Jobs[0]
	}

	rescue := func(t *testing.T, exec riverdriver.Executor, job *rivertype.JobRow, grace time.Duration) {
		t.Helper()

		_, err := exec.JobRescueMany(ctx, &riverdriver.JobRescueManyParams{
			ID:               []int64{job.ID},
			Error:            [][]byte{[]byte(`{"error":"rescued"}`)},
			FinalizedAt:      []*time.Time{nil},
			LeaseExpiresAt:   []*time.Time{job.LeaseExpiresAt},
			LeaseGracePeriod: grace,
			ScheduledAt:      []time.Time{job.ScheduledAt},
			State:            []string{string(rivertype.JobStateAvailable)},
			StuckHorizon:     time.Now().Add(-time.Hour),
		})
		require.NoError(t, err)
	}

	t.Run("JobLease", func(t *testing.T) {
		t.Parallel()

		t.Run("BusyJobDoesNotBlockOtherRenewals", func(t *testing.T) {
			t.Parallel()

			driver, schema := driverWithSchema(ctx, t, nil)
			if driver.DatabaseName() != riverdriver.DatabaseNamePostgres {
				t.Skip("Postgres row locks; SQLite serializes writes")
			}
			exec := driver.GetExecutor()
			first := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Schema: schema})
			second := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Schema: schema})
			claimed, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{ClientID: testClientID, LeaseDuration: time.Minute, MaxAttemptedBy: 10, MaxToLock: 2, Queue: first.Queue, Schema: schema})
			require.NoError(t, err)
			require.Len(t, claimed.Jobs, 2)
			byID := make(map[int64]*rivertype.JobRow, 2)
			for _, job := range claimed.Jobs {
				byID[job.ID] = job
			}
			tx, err := exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, tx.Rollback(ctx)) })
			var lockedID int64
			require.NoError(t, tx.QueryRow(ctx, "SELECT id FROM "+migrationSchemaPrefix(schema)+"river_job WHERE id = $1 FOR UPDATE", first.ID).Scan(&lockedID))

			renewCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			jobs, err := exec.JobRenewLeaseMany(renewCtx, &riverdriver.JobRenewLeaseManyParams{
				IDs:           []int64{first.ID, second.ID},
				AttemptedAt:   []time.Time{*byID[first.ID].AttemptedAt, *byID[second.ID].AttemptedAt},
				Attempts:      []int{byID[first.ID].Attempt, byID[second.ID].Attempt},
				LeaseDuration: 2 * time.Minute,
				Schema:        schema,
			})
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			require.Equal(t, second.ID, jobs[0].ID)
		})

		t.Run("ClaimAndRenewPreservesSchedulingAndAttempt", func(t *testing.T) {
			t.Parallel()

			exec, _ := executorWithTx(ctx, t)
			before := time.Now()
			job := claim(t, exec, time.Minute)
			require.NotNil(t, job.LeaseExpiresAt)
			require.WithinDuration(t, before.Add(time.Minute), *job.LeaseExpiresAt, 5*time.Second)

			renewed, err := exec.JobRenewLeaseMany(ctx, &riverdriver.JobRenewLeaseManyParams{
				IDs:           []int64{job.ID},
				AttemptedAt:   []time.Time{*job.AttemptedAt},
				Attempts:      []int{job.Attempt},
				LeaseDuration: 2 * time.Minute,
			})
			require.NoError(t, err)
			require.Len(t, renewed, 1)
			require.True(t, renewed[0].LeaseExpiresAt.After(*job.LeaseExpiresAt))
			require.Equal(t, job.Attempt, renewed[0].Attempt)
			require.Equal(t, job.AttemptedAt, renewed[0].AttemptedAt)
			require.Equal(t, job.AttemptedBy, renewed[0].AttemptedBy)
			require.Equal(t, job.ScheduledAt, renewed[0].ScheduledAt)
		})

		t.Run("ClaimLeaseUsesDatabaseClock", func(t *testing.T) {
			t.Parallel()

			exec, _ := executorWithTx(ctx, t)
			fakeNow := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &fakeNow})
			before := time.Now()
			claimed, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{ClientID: testClientID, LeaseDuration: time.Minute, MaxAttemptedBy: 10, MaxToLock: 1, Now: &fakeNow, Queue: job.Queue})
			require.NoError(t, err)
			require.Len(t, claimed.Jobs, 1)
			require.Equal(t, fakeNow, *claimed.Jobs[0].AttemptedAt)
			require.WithinDuration(t, before.Add(time.Minute), *claimed.Jobs[0].LeaseExpiresAt, 5*time.Second)
		})

		t.Run("ExpiredLeaseCannotRenew", func(t *testing.T) {
			t.Parallel()

			exec, driver := executorWithTx(ctx, t)
			job := claim(t, exec, time.Minute)
			job = setExpiry(t, exec, driver, job.ID, time.Now().Add(-time.Second))
			renewed, err := exec.JobRenewLeaseMany(ctx, &riverdriver.JobRenewLeaseManyParams{
				IDs:           []int64{job.ID},
				AttemptedAt:   []time.Time{*job.AttemptedAt},
				Attempts:      []int{job.Attempt},
				LeaseDuration: time.Minute,
			})
			require.NoError(t, err)
			require.Empty(t, renewed)
		})

		t.Run("InvalidRenewalParameters", func(t *testing.T) {
			t.Parallel()

			for _, invalid := range []string{"AttemptedAtLength", "AttemptsLength", "LeaseDuration"} {
				t.Run(invalid, func(t *testing.T) {
					t.Parallel()

					exec, _ := executorWithTx(ctx, t)
					job := claim(t, exec, time.Minute)
					params := &riverdriver.JobRenewLeaseManyParams{
						IDs:           []int64{job.ID},
						AttemptedAt:   []time.Time{*job.AttemptedAt},
						Attempts:      []int{job.Attempt},
						LeaseDuration: time.Minute,
					}
					switch invalid {
					case "AttemptedAtLength":
						params.AttemptedAt = nil
					case "AttemptsLength":
						params.Attempts = nil
					case "LeaseDuration":
						params.LeaseDuration = 0
					}
					_, err := exec.JobRenewLeaseMany(ctx, params)
					require.ErrorContains(t, err, "lease renewal requires")
				})
			}
		})

		t.Run("LegacyClaimHasNoLease", func(t *testing.T) {
			t.Parallel()

			exec, _ := executorWithTx(ctx, t)
			require.Nil(t, claim(t, exec, 0).LeaseExpiresAt)
		})

		t.Run("RenewalRejectsMismatchedClaim", func(t *testing.T) {
			t.Parallel()

			for _, mismatch := range []string{"Attempt", "AttemptedAt"} {
				t.Run(mismatch, func(t *testing.T) {
					t.Parallel()

					exec, _ := executorWithTx(ctx, t)
					job := claim(t, exec, time.Minute)
					attempt := job.Attempt
					attemptedAt := *job.AttemptedAt
					if mismatch == "Attempt" {
						attempt++
					} else {
						attemptedAt = attemptedAt.Add(-time.Second)
					}
					jobs, err := exec.JobRenewLeaseMany(ctx, &riverdriver.JobRenewLeaseManyParams{
						IDs:           []int64{job.ID},
						AttemptedAt:   []time.Time{attemptedAt},
						Attempts:      []int{attempt},
						LeaseDuration: 2 * time.Minute,
					})
					require.NoError(t, err)
					require.Empty(t, jobs)

					stored, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
					require.NoError(t, err)
					require.Equal(t, job.LeaseExpiresAt, stored.LeaseExpiresAt)
				})
			}
		})

		t.Run("RenewalRejectsPreviousClaimAfterRescue", func(t *testing.T) {
			t.Parallel()

			exec, driver := executorWithTx(ctx, t)
			previous := claim(t, exec, time.Minute)
			expired := setExpiry(t, exec, driver, previous.ID, time.Now().Add(-time.Minute))
			rescue(t, exec, expired, 0)

			// Reuse the original timestamp to exercise the attempt check even
			// when database time has not advanced between the two claims.
			claimed, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID:       testClientID,
				LeaseDuration:  time.Minute,
				MaxAttemptedBy: 10,
				MaxToLock:      1,
				Now:            previous.AttemptedAt,
				Queue:          previous.Queue,
			})
			require.NoError(t, err)
			require.Len(t, claimed.Jobs, 1)
			current := claimed.Jobs[0]
			require.Equal(t, previous.ID, current.ID)
			require.Equal(t, previous.AttemptedAt, current.AttemptedAt)
			require.Equal(t, previous.Attempt+1, current.Attempt)

			params := &riverdriver.JobRenewLeaseManyParams{
				IDs:           []int64{previous.ID},
				AttemptedAt:   []time.Time{*previous.AttemptedAt},
				Attempts:      []int{previous.Attempt},
				LeaseDuration: 2 * time.Minute,
			}
			jobs, err := exec.JobRenewLeaseMany(ctx, params)
			require.NoError(t, err)
			require.Empty(t, jobs)

			params.Attempts[0] = current.Attempt
			jobs, err = exec.JobRenewLeaseMany(ctx, params)
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			require.Equal(t, current.ID, jobs[0].ID)
		})

		t.Run("RenewalRetriesAfterUnconfirmedResponse", func(t *testing.T) {
			t.Parallel()

			exec, _ := executorWithTx(ctx, t)
			job := claim(t, exec, time.Minute)
			params := &riverdriver.JobRenewLeaseManyParams{
				IDs:           []int64{job.ID},
				AttemptedAt:   []time.Time{*job.AttemptedAt},
				Attempts:      []int{job.Attempt},
				LeaseDuration: 2 * time.Minute,
			}
			// Discard the first response, as if the database committed but the
			// response was lost. Retry using only the original claim snapshot.
			_, err := exec.JobRenewLeaseMany(ctx, params)
			require.NoError(t, err)
			stored, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.True(t, stored.LeaseExpiresAt.After(*job.LeaseExpiresAt))

			jobs, err := exec.JobRenewLeaseMany(ctx, params)
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			require.Equal(t, job.ID, jobs[0].ID)
			require.False(t, jobs[0].LeaseExpiresAt.Before(*stored.LeaseExpiresAt))
			require.Equal(t, job.AttemptedAt, jobs[0].AttemptedAt)
			require.Equal(t, job.Attempt, jobs[0].Attempt)
		})

		t.Run("RescueRechecksExpiryAndGrace", func(t *testing.T) {
			t.Parallel()

			exec, driver := executorWithTx(ctx, t)
			job := claim(t, exec, time.Minute)
			// A candidate from a prior expiry must not rescue a newer lease,
			// even when both expiries are already beyond the rescue grace.
			old := setExpiry(t, exec, driver, job.ID, time.Now().Add(-3*time.Minute))
			current := setExpiry(t, exec, driver, job.ID, time.Now().Add(-2*time.Minute))
			rescue(t, exec, old, time.Minute)
			stored, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, stored.State)
			require.Equal(t, current.LeaseExpiresAt, stored.LeaseExpiresAt)

			// A legacy candidate must never rescue a newly leased execution.
			legacy := *current
			legacy.LeaseExpiresAt = nil
			rescue(t, exec, &legacy, time.Minute)
			stored, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, stored.State)

			rescue(t, exec, current, 3*time.Minute)
			stored, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, stored.State)

			rescue(t, exec, current, time.Minute)
			stored, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, stored.State)
			require.Nil(t, stored.LeaseExpiresAt)
			require.Equal(t, job.Attempt, stored.Attempt)
			require.Equal(t, job.ScheduledAt, stored.ScheduledAt)
			require.Len(t, stored.Errors, 1)
		})

		t.Run("ScanUsesLeaseForRecentAttemptsAndLegacyHorizon", func(t *testing.T) {
			t.Parallel()

			exec, driver := executorWithTx(ctx, t)
			now := time.Now().UTC()
			oldAttempt := now.Add(-2 * time.Hour)
			running := rivertype.JobStateRunning
			legacyOld := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &oldAttempt, State: &running})
			_ = testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &now, State: &running})
			valid := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &oldAttempt, State: &running})
			setExpiry(t, exec, driver, valid.ID, now.Add(time.Minute))
			grace := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &oldAttempt, State: &running})
			setExpiry(t, exec, driver, grace.ID, now.Add(-30*time.Second))
			expired := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{AttemptedAt: &now, State: &running})
			setExpiry(t, exec, driver, expired.ID, now.Add(-2*time.Minute))

			params := &riverdriver.JobGetStuckParams{LeaseGracePeriod: time.Minute, Max: 1, StuckHorizon: now.Add(-time.Hour)}
			jobs, err := exec.JobGetStuck(ctx, params)
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			require.Equal(t, legacyOld.ID, jobs[0].ID)
			params.AfterID = jobs[0].ID
			jobs, err = exec.JobGetStuck(ctx, params)
			require.NoError(t, err)
			require.Len(t, jobs, 1)
			require.Equal(t, expired.ID, jobs[0].ID)
			params.AfterID = jobs[0].ID
			jobs, err = exec.JobGetStuck(ctx, params)
			require.NoError(t, err)
			require.Empty(t, jobs)
		})

		t.Run("StateTransitionsClearLease", func(t *testing.T) {
			t.Parallel()

			for _, state := range []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded, rivertype.JobStateRetryable, rivertype.JobStateScheduled} {
				t.Run(string(state), func(t *testing.T) {
					t.Parallel()

					exec, _ := executorWithTx(ctx, t)
					job := claim(t, exec, time.Minute)
					var finalizedAt *time.Time
					if state == rivertype.JobStateCancelled || state == rivertype.JobStateCompleted || state == rivertype.JobStateDiscarded {
						finalizedAt = new(time.Now().UTC())
					}
					jobs, err := exec.JobSetStateIfRunningMany(ctx, &riverdriver.JobSetStateIfRunningManyParams{
						ID:              []int64{job.ID},
						Attempt:         []*int{nil},
						ErrData:         [][]byte{nil},
						FinalizedAt:     []*time.Time{finalizedAt},
						MetadataDoMerge: []bool{false},
						MetadataUpdates: [][]byte{nil},
						ScheduledAt:     []*time.Time{nil},
						State:           []rivertype.JobState{state},
					})
					require.NoError(t, err)
					require.Len(t, jobs, 1)
					require.Equal(t, state, jobs[0].State)
					require.Nil(t, jobs[0].LeaseExpiresAt)
				})
			}
		})

		t.Run("UpdateFullClearsOnlyWhenLeavingRunning", func(t *testing.T) {
			t.Parallel()

			exec, _ := executorWithTx(ctx, t)
			job := claim(t, exec, time.Minute)
			updated, err := exec.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{ID: job.ID, MetadataDoUpdate: true, Metadata: []byte(`{"step":1}`)})
			require.NoError(t, err)
			require.Equal(t, job.LeaseExpiresAt, updated.LeaseExpiresAt)
			updated, err = exec.JobUpdateFull(ctx, &riverdriver.JobUpdateFullParams{ID: job.ID, StateDoUpdate: true, State: rivertype.JobStateAvailable})
			require.NoError(t, err)
			require.Nil(t, updated.LeaseExpiresAt)
		})
	})
}
