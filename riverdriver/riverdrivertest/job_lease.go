package riverdrivertest

import (
	"context"
	"strings"
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

	completionParams := func(job *rivertype.JobRow, state rivertype.JobState) *riverdriver.JobSetStateIfRunningManyParams {
		return &riverdriver.JobSetStateIfRunningManyParams{
			ID: []int64{job.ID}, Attempt: []*int{nil}, ErrData: [][]byte{nil},
			ExpectedAttempt: []*int{new(job.Attempt)}, ExpectedAttemptedAt: []*time.Time{job.AttemptedAt},
			FinalizedAt: []*time.Time{new(time.Now().UTC())}, MetadataDoMerge: []bool{true},
			MetadataUpdates: [][]byte{[]byte(`{"completion":"original"}`)}, ScheduledAt: []*time.Time{nil},
			State: []rivertype.JobState{state},
		}
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
			require.Equal(t, job.ID, renewed[0].ID)
			require.NotNil(t, renewed[0].LeaseExpiresAt)
			require.True(t, renewed[0].LeaseExpiresAt.After(*job.LeaseExpiresAt))
			// Lease confirmations omit job data; inspect storage to verify it is unchanged.
			stored, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, renewed[0].LeaseExpiresAt, stored.LeaseExpiresAt)
			require.Equal(t, job.Attempt, stored.Attempt)
			require.Equal(t, job.AttemptedAt, stored.AttemptedAt)
			require.Equal(t, job.AttemptedBy, stored.AttemptedBy)
			require.Equal(t, job.ScheduledAt, stored.ScheduledAt)
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

		t.Run("CompletionBatchKeepsValidAndLegacyMembers", func(t *testing.T) {
			t.Parallel()

			exec, driver := executorWithTx(ctx, t)
			stale := claim(t, exec, time.Minute)
			stale = setExpiry(t, exec, driver, stale.ID, time.Now().Add(-time.Second))
			valid := claim(t, exec, time.Minute)
			legacy := claim(t, exec, 0)
			batch := &riverdriver.JobSetStateIfRunningManyParams{}
			for _, job := range []*rivertype.JobRow{stale, valid, legacy} {
				params := completionParams(job, rivertype.JobStateCompleted)
				batch.ID = append(batch.ID, params.ID[0])
				batch.Attempt = append(batch.Attempt, nil)
				batch.ErrData = append(batch.ErrData, nil)
				batch.ExpectedAttempt = append(batch.ExpectedAttempt, params.ExpectedAttempt[0])
				batch.ExpectedAttemptedAt = append(batch.ExpectedAttemptedAt, params.ExpectedAttemptedAt[0])
				batch.FinalizedAt = append(batch.FinalizedAt, params.FinalizedAt[0])
				batch.MetadataDoMerge = append(batch.MetadataDoMerge, false)
				batch.MetadataUpdates = append(batch.MetadataUpdates, nil)
				batch.ScheduledAt = append(batch.ScheduledAt, nil)
				batch.State = append(batch.State, rivertype.JobStateCompleted)
			}
			batch.ExpectedAttempt[2], batch.ExpectedAttemptedAt[2] = nil, nil
			jobs, err := exec.JobSetStateIfRunningMany(ctx, batch)
			require.NoError(t, err)
			require.Len(t, jobs, 2)
			ids := []int64{jobs[0].ID, jobs[1].ID}
			require.ElementsMatch(t, []int64{valid.ID, legacy.ID}, ids)
			for _, job := range jobs {
				require.Equal(t, rivertype.JobStateCompleted, job.State)
			}
			stored, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: stale.ID})
			require.NoError(t, err)
			require.Equal(t, stale, stored)
		})

		t.Run("CompletionRejectsExpiredLease", func(t *testing.T) {
			t.Parallel()

			exec, driver := executorWithTx(ctx, t)
			job := claim(t, exec, time.Minute)
			job = setExpiry(t, exec, driver, job.ID, time.Now().Add(-time.Second))
			jobs, err := exec.JobSetStateIfRunningMany(ctx, completionParams(job, rivertype.JobStateCompleted))
			require.NoError(t, err)
			require.Empty(t, jobs)
			stored, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, job, stored)
		})

		t.Run("CompletionRejectsMismatchedClaim", func(t *testing.T) {
			t.Parallel()

			for _, mismatch := range []string{"Attempt", "AttemptedAt", "Missing"} {
				t.Run(mismatch, func(t *testing.T) {
					t.Parallel()

					exec, _ := executorWithTx(ctx, t)
					job := claim(t, exec, time.Minute)
					params := completionParams(job, rivertype.JobStateCompleted)
					switch mismatch {
					case "Attempt":
						params.ExpectedAttempt[0] = new(job.Attempt + 1)
					case "AttemptedAt":
						params.ExpectedAttemptedAt[0] = new(job.AttemptedAt.Add(-time.Second))
					case "Missing":
						params.ExpectedAttempt = nil
						params.ExpectedAttemptedAt = nil
					}
					jobs, err := exec.JobSetStateIfRunningMany(ctx, params)
					require.NoError(t, err)
					require.Empty(t, jobs)
					stored, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
					require.NoError(t, err)
					require.Equal(t, job, stored)
				})
			}
		})

		t.Run("CompletionRejectsPreviousClaimAfterRescue", func(t *testing.T) {
			t.Parallel()

			for _, state := range []rivertype.JobState{rivertype.JobStateAvailable, rivertype.JobStateCancelled, rivertype.JobStateCompleted, rivertype.JobStateDiscarded, rivertype.JobStateRetryable, rivertype.JobStateScheduled} {
				t.Run(string(state), func(t *testing.T) {
					t.Parallel()

					exec, driver := executorWithTx(ctx, t)
					previous := claim(t, exec, time.Minute)
					params := completionParams(previous, state)
					// Exercise all values that an old error or Snooze could overwrite.
					params.Attempt[0] = new(max(previous.Attempt-1, 0))
					params.ErrData[0] = []byte(`{"error":"stale error"}`)
					params.ScheduledAt[0] = new(time.Now().Add(time.Hour))
					expired := setExpiry(t, exec, driver, previous.ID, time.Now().Add(-time.Minute))
					rescue(t, exec, expired, 0)
					claimed, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
						ClientID: testClientID, LeaseDuration: time.Minute, MaxAttemptedBy: 10,
						MaxToLock: 1, Now: previous.AttemptedAt, Queue: previous.Queue,
					})
					require.NoError(t, err)
					require.Len(t, claimed.Jobs, 1)
					current := claimed.Jobs[0]
					require.Equal(t, previous.AttemptedAt, current.AttemptedAt)
					require.Equal(t, previous.Attempt+1, current.Attempt)
					jobs, err := exec.JobSetStateIfRunningMany(ctx, params)
					require.NoError(t, err)
					require.Empty(t, jobs)
					stored, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: current.ID})
					require.NoError(t, err)
					require.Equal(t, current, stored)
					// Even B's subsequent completion must not make A's metadata eligible.
					jobs, err = exec.JobSetStateIfRunningMany(ctx, completionParams(current, rivertype.JobStateCompleted))
					require.NoError(t, err)
					require.Len(t, jobs, 1)
					before := jobs[0]
					jobs, err = exec.JobSetStateIfRunningMany(ctx, params)
					require.NoError(t, err)
					require.Empty(t, jobs)
					stored, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: current.ID})
					require.NoError(t, err)
					require.Equal(t, before, stored)
				})
			}
		})

		t.Run("CompletionRetryDoesNotRewriteConfirmedResult", func(t *testing.T) {
			t.Parallel()

			for _, snooze := range []bool{false, true} {
				name := "Completed"
				if snooze {
					name = "Snoozed"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					exec, _ := executorWithTx(ctx, t)
					job := claim(t, exec, time.Minute)
					params := completionParams(job, rivertype.JobStateCompleted)
					if snooze {
						params.State[0] = rivertype.JobStateScheduled
						params.FinalizedAt[0] = nil
						params.Attempt[0] = new(job.Attempt - 1)
						params.ScheduledAt[0] = new(time.Now().Add(time.Hour))
					}
					jobs, err := exec.JobSetStateIfRunningMany(ctx, params)
					require.NoError(t, err)
					require.Len(t, jobs, 1)
					before := jobs[0]
					// Losing a response allows confirmation, but no extra metadata write.
					params.MetadataUpdates[0] = []byte(`{"completion":"changed"}`)
					jobs, err = exec.JobSetStateIfRunningMany(ctx, params)
					require.NoError(t, err)
					require.Len(t, jobs, 1)
					require.Equal(t, before, jobs[0])
				})
			}
		})

		t.Run("CompletionWaitsForLocksBeforeCheckingLease", func(t *testing.T) {
			t.Parallel()

			driver, schema := driverWithSchema(ctx, t, nil)
			if driver.DatabaseName() != riverdriver.DatabaseNamePostgres {
				t.Skip("Postgres row locks; SQLite serializes writes")
			}
			exec := driver.GetExecutor()
			seed := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{Schema: schema})
			claimed, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID: testClientID, LeaseDuration: time.Minute, MaxAttemptedBy: 10,
				MaxToLock: 1, Queue: seed.Queue, Schema: schema,
			})
			require.NoError(t, err)
			require.Len(t, claimed.Jobs, 1)
			job := claimed.Jobs[0]
			// Commit the short lease before taking an otherwise read-only lock.
			// The tuple stays unchanged while the completion request waits.
			require.NoError(t, exec.Exec(ctx, "UPDATE "+migrationSchemaPrefix(schema)+"river_job SET lease_expires_at=clock_timestamp()+interval '2 seconds' WHERE id=$1", job.ID))
			blocker, err := exec.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = blocker.Rollback(ctx) })
			var lockedID int64
			require.NoError(t, blocker.QueryRow(ctx, "SELECT id FROM "+migrationSchemaPrefix(schema)+"river_job WHERE id = $1 FOR UPDATE", job.ID).Scan(&lockedID))
			params := completionParams(job, rivertype.JobStateCompleted)
			params.Schema = schema
			type result struct {
				rows []*rivertype.JobRow
				err  error
			}
			// A buffered result channel lets the query finish during cleanup even
			// if a lock-wait assertion fails before the result is consumed.
			done := make(chan result, 1)
			requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			requestTx, err := exec.Begin(requestCtx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = requestTx.Rollback(ctx) })
			var backendPID int
			require.NoError(t, requestTx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&backendPID))
			go func() {
				rows, err := requestTx.JobSetStateIfRunningMany(requestCtx, params)
				done <- result{rows: rows, err: err}
			}()
			require.Eventually(t, func() bool {
				var waiting bool
				err := exec.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND pid=$1)", backendPID).Scan(&waiting)
				return err == nil && waiting
			}, time.Second, 10*time.Millisecond)
			// Time deliberately advances beyond the granted lease while SQL waits.
			time.Sleep(2 * time.Second)
			require.NoError(t, blocker.Commit(ctx))
			res := <-done
			require.NoError(t, res.err)
			require.Empty(t, res.rows)
			require.NoError(t, requestTx.Rollback(ctx))
			stored, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID, Schema: schema})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateRunning, stored.State)
			require.Equal(t, job.Metadata, stored.Metadata)
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

		t.Run("InvalidCompletionClaimParameters", func(t *testing.T) {
			t.Parallel()

			for _, invalid := range []string{"AttemptLength", "AttemptedAtLength", "IncompletePair"} {
				t.Run(invalid, func(t *testing.T) {
					t.Parallel()

					exec, _ := executorWithTx(ctx, t)
					job := claim(t, exec, time.Minute)
					params := completionParams(job, rivertype.JobStateCompleted)
					switch invalid {
					case "AttemptLength":
						params.ExpectedAttempt = nil
					case "AttemptedAtLength":
						params.ExpectedAttemptedAt = nil
					case "IncompletePair":
						params.ExpectedAttemptedAt[0] = nil
					}
					_, err := exec.JobSetStateIfRunningMany(ctx, params)
					require.ErrorContains(t, err, "completion requires")
				})
			}
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
			stored, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			require.Equal(t, job.AttemptedAt, stored.AttemptedAt)
			require.Equal(t, job.Attempt, stored.Attempt)
		})

		t.Run("RenewalReturnsOnlyLeaseConfirmation", func(t *testing.T) {
			t.Parallel()

			exec, _ := executorWithTx(ctx, t)
			payload := strings.Repeat("x", 1<<20)
			inserted := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{
				Attempt:     new(2),
				EncodedArgs: []byte(`{"payload":"` + payload + `"}`),
				Errors:      [][]byte{[]byte(`{"attempt":1,"at":"2026-10-13T00:00:00Z","error":"previous failure"}`)},
				Metadata:    []byte(`{"payload":"` + payload + `"}`),
			})
			claimed, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID: testClientID, LeaseDuration: time.Minute, MaxAttemptedBy: 10, MaxToLock: 1, Queue: inserted.Queue,
			})
			require.NoError(t, err)
			require.Len(t, claimed.Jobs, 1)
			job := claimed.Jobs[0]
			require.Equal(t, inserted.ID, job.ID)
			require.Greater(t, len(job.EncodedArgs), 1<<20)
			require.Greater(t, len(job.Metadata), 1<<20)

			rows, err := exec.JobRenewLeaseMany(ctx, &riverdriver.JobRenewLeaseManyParams{
				IDs: []int64{job.ID}, AttemptedAt: []time.Time{*job.AttemptedAt}, Attempts: []int{job.Attempt}, LeaseDuration: 2 * time.Minute,
			})
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.NotNil(t, rows[0].LeaseExpiresAt)
			require.True(t, rows[0].LeaseExpiresAt.After(*job.LeaseExpiresAt))
			require.Equal(t, &rivertype.JobRow{ID: job.ID, LeaseExpiresAt: rows[0].LeaseExpiresAt}, rows[0])

			// Only lease expiry may change; payload, history, claim, and scheduling stay intact.
			stored, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: job.ID})
			require.NoError(t, err)
			expected := *job
			expected.LeaseExpiresAt = rows[0].LeaseExpiresAt
			require.Equal(t, &expected, stored)
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
						ID:                  []int64{job.ID},
						ExpectedAttempt:     []*int{new(job.Attempt)},
						ExpectedAttemptedAt: []*time.Time{job.AttemptedAt},
						Attempt:             []*int{nil},
						ErrData:             [][]byte{nil},
						FinalizedAt:         []*time.Time{finalizedAt},
						MetadataDoMerge:     []bool{false},
						MetadataUpdates:     [][]byte{nil},
						ScheduledAt:         []*time.Time{nil},
						State:               []rivertype.JobState{state},
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
