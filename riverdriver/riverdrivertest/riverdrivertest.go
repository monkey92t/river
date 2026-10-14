package riverdrivertest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/testfactory"
	"github.com/riverqueue/river/rivertype"
)

// Exercise fully exercises a driver. The driver's listener is exercised if
// supported.
func Exercise[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string),
	executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx]),
) {
	t.Helper()

	{
		driver, _ := driverWithSchema(ctx, t, nil)
		if driver.SupportsListener() {
			exerciseListener(ctx, t, driverWithSchema)
		} else {
			t.Logf("Driver does not support listener; skipping listener tests")
		}
	}

	exerciseDriverPool(ctx, t, driverWithSchema, executorWithTx)
	exerciseMigration(ctx, t, driverWithSchema, executorWithTx)
	exerciseNotification(ctx, t, executorWithTx)
	exerciseSQLFragments(ctx, t, executorWithTx)
	exerciseExecutorTx(ctx, t, driverWithSchema, executorWithTx)
	exerciseSchemaIntrospection(ctx, t, driverWithSchema, executorWithTx)
	exerciseSchemaName(ctx, t, driverWithSchema)
	exerciseJobInsert(ctx, t, driverWithSchema, executorWithTx)
	exerciseJobLease(ctx, t, driverWithSchema, executorWithTx)
	exerciseJobRead(ctx, t, executorWithTx)
	exerciseJobUpdate(ctx, t, executorWithTx)
	exerciseJobDelete(ctx, t, executorWithTx)
	exerciseLeader(ctx, t, executorWithTx)
	exerciseQueue(ctx, t, executorWithTx)
	exerciseSequenceStorage(ctx, t, executorWithTx)
}

const testClientID = "test-client-id"

func exerciseDriverPool[TTx any](ctx context.Context, t *testing.T,
	driverWithSchema func(ctx context.Context, t *testing.T, opts *riverdbtest.TestSchemaOpts) (riverdriver.Driver[TTx], string),
	executorWithTx func(ctx context.Context, t *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx]),
) {
	t.Helper()

	t.Run("InitDriver", func(t *testing.T) {
		t.Parallel()

		exec, _ := executorWithTx(ctx, t)
		require.NoError(t, exec.InitDriver(ctx))
		require.NoError(t, exec.InitDriver(ctx))
	})

	t.Run("Ping", func(t *testing.T) {
		t.Parallel()

		exec, _ := executorWithTx(ctx, t)
		require.NoError(t, exec.InitDriver(ctx))
		require.NoError(t, exec.Ping(ctx))

		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		require.ErrorIs(t, exec.Ping(cancelledCtx), context.Canceled)
	})

	t.Run("PoolIsSet", func(t *testing.T) {
		t.Parallel()

		t.Run("PoolIsSetOnDriverWithSchema", func(t *testing.T) {
			t.Parallel()

			driver, _ := driverWithSchema(ctx, t, nil)
			require.True(t, driver.PoolIsSet())
		})
	})

	t.Run("PoolSet", func(t *testing.T) {
		t.Parallel()

		t.Run("PoolSetNotImplementedOrAlreadySetError", func(t *testing.T) {
			t.Parallel()

			driver, _ := driverWithSchema(ctx, t, nil)
			err := driver.PoolSet(struct{}{})
			require.Error(t, err)
			if !errors.Is(err, riverdriver.ErrNotImplemented) {
				require.EqualError(t, err, "cannot PoolSet when internal pool is already non-nil")
			}
		})
	})

	t.Run("SupportsListenNotify", func(t *testing.T) {
		t.Parallel()

		_, driver := executorWithTx(ctx, t)

		switch driver.DatabaseName() {
		case riverdriver.DatabaseNamePostgres:
			require.True(t, driver.SupportsListenNotify())
		case riverdriver.DatabaseNameSQLite:
			require.True(t, driver.SupportsListenNotify())
		default:
			require.FailNow(t, "Don't know how to check SupportsListenNotify for: "+driver.DatabaseName())
		}
	})
}

func requireMissingRelation(t *testing.T, err error, schema, missingRelation string) {
	t.Helper()

	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		require.Equal(t, pgerrcode.UndefinedTable, pgErr.Code)
		require.Equal(t, fmt.Sprintf(`relation "%s.%s" does not exist`, schema, missingRelation), pgErr.Message)
	} else {
		// lib/pq: pq: relation %s.%s does not exist
		// SQLite: no such table: %s.%s
		// Turso: turso: error: Invalid argument supplied: no such database: %s
		require.Regexp(t, fmt.Sprintf(`(pq: relation "%s\.%s" does not exist|no such table: %s\.%s|no such database: %s)`, schema, missingRelation, schema, missingRelation, schema), err.Error())
	}
}

// sqliteJobJSONColumns are the columns of a SQLite job row that hold JSON.
//
//nolint:gochecknoglobals
var sqliteJobJSONColumns = []string{"args", "attempted_by", "errors", "metadata", "tags"}

// sqliteMalformedValue is text that isn't valid JSON. Stored in one of a SQLite
// job row's JSON columns, it's rejected with a "malformed JSON" error by any of
// SQLite's JSON functions that touch it.
const sqliteMalformedValue = "not json"

// sqliteJobColumnText returns a column of a SQLite job row cast to text, which
// can be used to check that a value that isn't valid JSON (and so can't be
// read through the driver) was left in place.
func sqliteJobColumnText(ctx context.Context, t *testing.T, exec riverdriver.Executor, jobID int64, column string) string {
	t.Helper()

	var value string
	require.NoError(t, exec.QueryRow(ctx, "SELECT cast("+column+" AS text) FROM river_job WHERE id = ?", jobID).Scan(&value))
	return value
}

// sqliteSetJobColumnMalformed overwrites a column of a SQLite job row with
// sqliteMalformedValue as text, simulating a JSON column changed out of band to
// a value that isn't valid JSON. Postgres' column types don't allow the
// equivalent.
func sqliteSetJobColumnMalformed(ctx context.Context, t *testing.T, exec riverdriver.Executor, jobID int64, column string) {
	t.Helper()

	require.NoError(t, exec.Exec(ctx, "UPDATE river_job SET "+column+" = ? WHERE id = ?", sqliteMalformedValue, jobID))
}

// sqliteSetJobJSONColumn overwrites a JSON column of a SQLite job row with the
// given JSON, simulating a row changed out of band into a shape that River
// can't decode. Postgres' column types don't allow the equivalent.
func sqliteSetJobJSONColumn(ctx context.Context, t *testing.T, exec riverdriver.Executor, jobID int64, column, jsonValue string) {
	t.Helper()

	require.NoError(t, exec.Exec(ctx, "UPDATE river_job SET "+column+" = jsonb(?) WHERE id = ?", jsonValue, jobID))
}

// exerciseSequenceStorage checks optional storage behavior across both Postgres adapters.
func exerciseSequenceStorage[TTx any](ctx context.Context, t *testing.T, executorWithTx func(context.Context, *testing.T) (riverdriver.Executor, riverdriver.Driver[TTx])) {
	t.Helper()

	setup := func(t *testing.T) (riverdriver.Executor, riverdriver.SequenceExecutor) {
		t.Helper()
		exec, driver := executorWithTx(ctx, t)
		sequenceExec, ok := exec.(riverdriver.SequenceExecutor)
		if driver.DatabaseName() != riverdriver.DatabaseNamePostgres {
			require.False(t, ok)
			t.Skip("sequence storage is Postgres-only")
		}
		require.True(t, ok)
		return exec, sequenceExec
	}

	attach := func(t *testing.T, exec riverdriver.Executor, sequenceExec riverdriver.SequenceExecutor, key string, position int64, scheduledAt time.Time) *rivertype.JobRow {
		t.Helper()
		job := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{ScheduledAt: &scheduledAt, State: new(rivertype.JobStatePending)})
		member, err := sequenceExec.SequenceJobAttach(ctx, &riverdriver.SequenceJobAttachParams{ID: job.ID, Key: key, Position: position, ContinueOnCancelled: true})
		require.NoError(t, err)
		return member
	}

	t.Run("SequenceStorage", func(t *testing.T) {
		t.Parallel()

		t.Run("BypassPreservesWorkAndSchedule", func(t *testing.T) {
			t.Parallel()

			exec, sequenceExec := setup(t)
			now := time.Now().UTC()
			future := now.Add(time.Hour)
			first := attach(t, exec, sequenceExec, "bypass", 1, now)
			second := attach(t, exec, sequenceExec, "bypass", 2, future)
			ordinary := testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: new(rivertype.JobStatePending)})
			count, err := sequenceExec.SequenceJobBypass(ctx, &riverdriver.SequenceJobBypassParams{Key: "bypass", Limit: 1, Now: now})
			require.NoError(t, err)
			require.Equal(t, 1, count)
			count, err = sequenceExec.SequenceJobBypass(ctx, &riverdriver.SequenceJobBypassParams{Key: "bypass", Limit: 10, Now: now})
			require.NoError(t, err)
			require.Equal(t, 1, count)
			count, err = sequenceExec.SequenceJobBypass(ctx, &riverdriver.SequenceJobBypassParams{Key: "bypass", Limit: 10, Now: now})
			require.NoError(t, err)
			require.Zero(t, count)
			for _, member := range []*rivertype.JobRow{first, second} {
				row, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: member.ID})
				require.NoError(t, err)
				require.NotNil(t, row.SequenceResolvedAt)
				require.Equal(t, member.ScheduledAt, row.ScheduledAt)
				require.Equal(t, member.EncodedArgs, row.EncodedArgs)
				require.Equal(t, member.Metadata, row.Metadata)
				require.Equal(t, member.Attempt, row.Attempt)
				require.Nil(t, row.FinalizedAt)
				if row.ID == first.ID {
					require.Equal(t, rivertype.JobStateAvailable, row.State)
				} else {
					require.Equal(t, rivertype.JobStateScheduled, row.State)
				}
			}
			stats, err := sequenceExec.SequenceJobStats(ctx, &riverdriver.SequenceGroupKeyParams{Key: "bypass"})
			require.NoError(t, err)
			require.Equal(t, &riverdriver.SequenceJobStats{Active: 2, MaxPosition: 2, Nonterminal: 2, ResolvedNonterminal: 2}, stats)
			row, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: ordinary.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStatePending, row.State)
			require.Nil(t, row.SequenceResolvedAt)
		})

		t.Run("CleanerPreservesUnresolvedOutcomes", func(t *testing.T) {
			t.Parallel()

			exec, sequenceExec := setup(t)
			now := time.Now().UTC().Add(-time.Hour)
			member := attach(t, exec, sequenceExec, "clean", 1, now)
			_, err := exec.JobCancel(ctx, &riverdriver.JobCancelParams{ID: member.ID, CancelAttemptedAt: now, Now: &now})
			require.NoError(t, err)
			testfactory.Job(ctx, t, exec, &testfactory.JobOpts{State: new(rivertype.JobStateCancelled), FinalizedAt: &now})
			params := &riverdriver.JobDeleteBeforeParams{CancelledFinalizedAtHorizon: time.Now(), CancelledDoDelete: true, Max: 100}
			count, err := exec.JobDeleteBefore(ctx, params)
			require.NoError(t, err)
			require.Equal(t, 1, count)
			_, err = exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: member.ID})
			require.NoError(t, err)
			_, err = sequenceExec.SequenceJobResolve(ctx, &riverdriver.SequenceJobMutationParams{ID: member.ID, Key: "clean", Now: time.Now()})
			require.NoError(t, err)
			count, err = exec.JobDeleteBefore(ctx, params)
			require.NoError(t, err)
			require.Equal(t, 1, count)
		})

		t.Run("GroupPersistenceAndOrphanDiscovery", func(t *testing.T) {
			t.Parallel()

			exec, sequenceExec := setup(t)
			attach(t, exec, sequenceExec, "orphan", 3, time.Now())
			keys, err := sequenceExec.SequenceOrphanList(ctx, &riverdriver.SequenceOrphanListParams{Limit: 10})
			require.NoError(t, err)
			require.Equal(t, []string{"orphan"}, keys)
			require.NoError(t, sequenceExec.SequenceGroupEnsure(ctx, &riverdriver.SequenceGroupKeyParams{Key: "orphan"}))
			group, err := sequenceExec.SequenceGroupGet(ctx, &riverdriver.SequenceGroupGetParams{Key: "orphan", Lock: true})
			require.NoError(t, err)
			group.NextPosition = 4
			group, err = sequenceExec.SequenceGroupUpdate(ctx, &riverdriver.SequenceGroupUpdateParams{Group: group})
			require.NoError(t, err)
			require.Equal(t, int64(4), group.NextPosition)
			require.NoError(t, sequenceExec.SequenceGroupEnsure(ctx, &riverdriver.SequenceGroupKeyParams{Key: "orphan"}))
			groups, err := sequenceExec.SequenceGroupList(ctx, &riverdriver.SequenceGroupListParams{Limit: 10, DueBefore: new(time.Now().Add(time.Hour))})
			require.NoError(t, err)
			require.Len(t, groups, 1)
			require.Equal(t, int64(4), groups[0].NextPosition)
			for _, key := range []string{"a-idle", "z-future"} {
				require.NoError(t, sequenceExec.SequenceGroupEnsure(ctx, &riverdriver.SequenceGroupKeyParams{Key: key}))
				other, err := sequenceExec.SequenceGroupGet(ctx, &riverdriver.SequenceGroupGetParams{Key: key, Lock: true})
				require.NoError(t, err)
				other.NextCheckAt = nil
				if key == "z-future" {
					other.NextCheckAt = new(time.Now().Add(2 * time.Hour))
				}
				_, err = sequenceExec.SequenceGroupUpdate(ctx, &riverdriver.SequenceGroupUpdateParams{Group: other})
				require.NoError(t, err)
			}
			groups, err = sequenceExec.SequenceGroupList(ctx, &riverdriver.SequenceGroupListParams{Limit: 10, DueBefore: new(time.Now().Add(time.Hour))})
			require.NoError(t, err)
			require.Len(t, groups, 1)
			require.Equal(t, "orphan", groups[0].Key)
			groups, err = sequenceExec.SequenceGroupList(ctx, &riverdriver.SequenceGroupListParams{AfterKey: "a-idle", Limit: 10})
			require.NoError(t, err)
			require.Len(t, groups, 2)
			require.Equal(t, "orphan", groups[0].Key)
			require.Equal(t, "z-future", groups[1].Key)
			keys, err = sequenceExec.SequenceOrphanList(ctx, &riverdriver.SequenceOrphanListParams{Limit: 10})
			require.NoError(t, err)
			require.Empty(t, keys)
		})

		t.Run("RetryMatchesCoreStateTransitions", func(t *testing.T) {
			t.Parallel()

			for _, state := range []rivertype.JobState{
				rivertype.JobStateAvailable, rivertype.JobStateCancelled, rivertype.JobStateCompleted,
				rivertype.JobStateDiscarded, rivertype.JobStatePending, rivertype.JobStateRetryable,
				rivertype.JobStateRunning, rivertype.JobStateScheduled,
			} {
				t.Run(string(state), func(t *testing.T) {
					t.Parallel()

					for _, clock := range []string{"DatabaseDefault", "ExplicitOverride"} {
						t.Run(clock, func(t *testing.T) {
							t.Parallel()

							for _, offset := range []time.Duration{-time.Hour, 0, time.Hour} {
								t.Run(offset.String(), func(t *testing.T) {
									t.Parallel()

									exec, sequenceExec := setup(t)
									var databaseNow time.Time
									require.NoError(t, exec.QueryRow(ctx, "SELECT now()").Scan(&databaseNow))
									now := databaseNow.UTC()
									var nowOverride *time.Time
									if clock == "ExplicitOverride" {
										now = now.Add(2 * time.Hour)
										nowOverride = &now
									}
									past, scheduledAt := now.Add(-time.Hour), now.Add(offset)
									var finalizedAt *time.Time
									if state == rivertype.JobStateCancelled || state == rivertype.JobStateCompleted || state == rivertype.JobStateDiscarded {
										finalizedAt = &past
									}
									opts := &testfactory.JobOpts{
										Attempt: new(25), AttemptedAt: &past, AttemptedBy: []string{"earlier-worker"},
										EncodedArgs: []byte(`{"kept":"args"}`), Errors: [][]byte{[]byte(`{"attempt":25,"error":"earlier failure"}`)},
										MaxAttempts: new(25), Metadata: []byte(`{"kept":"metadata"}`),
										ScheduledAt: &scheduledAt, State: new(rivertype.JobStatePending),
									}
									ordinary := testfactory.Job(ctx, t, exec, opts)
									member := testfactory.Job(ctx, t, exec, opts)
									member, err := sequenceExec.SequenceJobAttach(ctx, &riverdriver.SequenceJobAttachParams{ID: member.ID, Key: "retry", Position: 1, ContinueOnCancelled: true})
									require.NoError(t, err)
									// Set both fixture states without invoking the retry behavior under test.
									require.NoError(t, exec.Exec(ctx, "UPDATE river_job SET state = $1, finalized_at = $2 WHERE id IN ($3, $4)", state, finalizedAt, ordinary.ID, member.ID))
									ordinaryBefore, memberBefore := ordinary, member
									ordinary, err = exec.JobRetry(ctx, &riverdriver.JobRetryParams{ID: ordinary.ID, Now: nowOverride})
									require.NoError(t, err)
									member, err = sequenceExec.SequenceJobRetry(ctx, &riverdriver.SequenceJobRetryParams{ID: member.ID, Key: "retry", Now: nowOverride})
									require.NoError(t, err)
									require.Equal(t, ordinary.State, member.State)
									require.Equal(t, ordinary.Attempt, member.Attempt)
									require.Equal(t, ordinary.MaxAttempts, member.MaxAttempts)
									require.Equal(t, ordinary.FinalizedAt, member.FinalizedAt)
									require.Equal(t, ordinary.ScheduledAt, member.ScheduledAt)
									if state == rivertype.JobStateRunning || (state == rivertype.JobStateAvailable && offset < 0) {
										require.Equal(t, scheduledAt, member.ScheduledAt)
									} else {
										require.Equal(t, now, member.ScheduledAt)
									}
									for _, pair := range [][2]*rivertype.JobRow{{ordinaryBefore, ordinary}, {memberBefore, member}} {
										require.Equal(t, pair[0].AttemptedAt, pair[1].AttemptedAt)
										require.Equal(t, pair[0].AttemptedBy, pair[1].AttemptedBy)
										require.Equal(t, pair[0].Errors, pair[1].Errors)
										require.Equal(t, pair[0].EncodedArgs, pair[1].EncodedArgs)
										require.Equal(t, pair[0].Metadata, pair[1].Metadata)
									}
									require.Equal(t, memberBefore.SequenceKey, member.SequenceKey)
									require.Equal(t, memberBefore.SequencePosition, member.SequencePosition)
									require.Nil(t, member.SequenceResolvedAt)
									if nowOverride == nil && state != rivertype.JobStateRunning {
										// The default retry clock must also match claims in this same transaction.
										claimed, err := exec.JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{ClientID: testClientID, MaxAttemptedBy: 5, MaxToLock: 10, Queue: "default"})
										require.NoError(t, err)
										require.Len(t, claimed.Jobs, 2)
										require.ElementsMatch(t, []int64{ordinary.ID, member.ID}, []int64{claimed.Jobs[0].ID, claimed.Jobs[1].ID})
									}
								})
							}
						})
					}
				})
			}
		})

		t.Run("MembershipAndRawRetryProtection", func(t *testing.T) {
			t.Parallel()

			exec, sequenceExec := setup(t)
			later := attach(t, exec, sequenceExec, "ordered", 5, time.Now().Add(time.Hour))
			first := attach(t, exec, sequenceExec, "ordered", 2, time.Now())
			require.Equal(t, "ordered", *first.SequenceKey)
			require.True(t, *first.SequenceContinueOnCancelled)
			require.False(t, *first.SequenceContinueOnDiscarded)
			head, err := sequenceExec.SequenceJobHead(ctx, &riverdriver.SequenceGroupKeyParams{Key: "ordered", Lock: true})
			require.NoError(t, err)
			require.Equal(t, first.ID, head.ID)
			_, err = exec.JobRetry(ctx, &riverdriver.JobRetryParams{ID: later.ID})
			require.ErrorIs(t, err, riverdriver.ErrSequenceRetry)
			unchanged, err := exec.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: later.ID})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStatePending, unchanged.State)
			released, err := sequenceExec.SequenceJobRelease(ctx, &riverdriver.SequenceJobMutationParams{ID: first.ID, Key: "ordered"})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateAvailable, released.State)
			stats, err := sequenceExec.SequenceJobStats(ctx, &riverdriver.SequenceGroupKeyParams{Key: "ordered"})
			require.NoError(t, err)
			require.Equal(t, &riverdriver.SequenceJobStats{Active: 1, MaxPosition: 5, Nonterminal: 2, Unresolved: 2}, stats)
			_, err = sequenceExec.SequenceJobResolve(ctx, &riverdriver.SequenceJobMutationParams{ID: first.ID, Key: "ordered", Now: time.Now()})
			require.ErrorIs(t, err, rivertype.ErrNotFound)
		})
	})
}
