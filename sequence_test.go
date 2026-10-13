package river

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivertype"
)

// sequenceKeyTestArgs fixes the v1 grouping contract independently of database storage.
type sequenceKeyTestArgs struct {
	AccountID int64  `json:"account_id" river:"sequence"`
	Action    string `json:"action"`
}

// Kind identifies the key test worker.
func (sequenceKeyTestArgs) Kind() string { return "account_task" }

// SequenceOpts uses the tagged account identifier for grouping.
func (sequenceKeyTestArgs) SequenceOpts() SequenceOpts { return SequenceOpts{ByArgs: true} }

// sequencePendingDefaultArgs exercises job-level insert defaults.
type sequencePendingDefaultArgs struct{ sequenceKeyTestArgs }

// InsertOpts requests an incompatible manual pending state.
func (sequencePendingDefaultArgs) InsertOpts() InsertOpts { return InsertOpts{Pending: true} }

// sequenceTestExecutor advertises sequence support without permitting unexpected storage calls.
type sequenceTestExecutor struct {
	riverdriver.ExecutorTx
	riverdriver.SequenceExecutor
}

// rivertype.JobRow's existing untagged fields are a compatibility constraint under test.
//
//nolint:musttag
func TestJobRowSequenceJSON(t *testing.T) {
	t.Parallel()

	t.Run("OrdinaryJobsOmitSequenceFields", func(t *testing.T) {
		t.Parallel()

		encoded, err := json.Marshal(&rivertype.JobRow{ID: 1})
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "sequence_")
	})

	t.Run("SequenceFieldsRoundTrip", func(t *testing.T) {
		t.Parallel()

		resolvedAt := time.Date(2026, time.October, 12, 0, 0, 0, 0, time.UTC)
		job := &rivertype.JobRow{
			ID:                          2,
			SequenceContinueOnCancelled: new(false),
			SequenceContinueOnDiscarded: new(true),
			SequenceKey:                 new("account:42"),
			SequencePosition:            new(int64(3)),
			SequenceResolvedAt:          &resolvedAt,
		}
		encoded, err := json.Marshal(job)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"sequence_continue_on_cancelled":false`)
		require.Contains(t, string(encoded), `"sequence_key":"account:42"`)
		var decoded rivertype.JobRow
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		require.Equal(t, job, &decoded)
	})
}

func TestSequenceCompleteTxIsolation(t *testing.T) {
	t.Parallel()

	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("Commit=%t", commit), func(t *testing.T) {
			t.Parallel()

			bundle := setupSequenceIntegration(t)
			ctx := context.WithValue(context.Background(), rivercommon.ContextKeyClient{}, bundle.client)
			rows, err := bundle.client.InsertMany(ctx, []InsertManyParams{
				{Args: sequenceKeyTestArgs{AccountID: 100, Action: "head"}},
				{Args: sequenceKeyTestArgs{AccountID: 100, Action: "tail"}},
			})
			require.NoError(t, err)
			_, err = bundle.client.SequenceReconcile(ctx, *rows[0].Job.SequenceKey)
			require.NoError(t, err)
			_, err = bundle.pool.Exec(ctx, "UPDATE "+pgx.Identifier{bundle.schema, "river_job"}.Sanitize()+" SET state='running', attempted_at=now(), attempt=1 WHERE id=$1", rows[0].Job.ID)
			require.NoError(t, err)
			head, err := bundle.client.JobGet(ctx, rows[0].Job.ID)
			require.NoError(t, err)

			_, err = bundle.pool.Exec(ctx, "CREATE TABLE "+pgx.Identifier{bundle.schema, "sequence_business"}.Sanitize()+" (value integer NOT NULL)")
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := bundle.pool.Exec(context.Background(), "DROP TABLE "+pgx.Identifier{bundle.schema, "sequence_business"}.Sanitize())
				require.NoError(t, err)
			})
			_, err = bundle.pool.Exec(ctx, "ALTER TABLE "+pgx.Identifier{bundle.schema, "river_sequence_group"}.Sanitize()+" RENAME TO river_sequence_group_unavailable")
			require.NoError(t, err)
			t.Cleanup(func() {
				_, err := bundle.pool.Exec(context.Background(), "ALTER TABLE "+pgx.Identifier{bundle.schema, "river_sequence_group_unavailable"}.Sanitize()+" RENAME TO river_sequence_group")
				require.NoError(t, err)
			})

			tx, err := bundle.pool.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
			_, err = tx.Exec(ctx, "INSERT INTO "+pgx.Identifier{bundle.schema, "sequence_business"}.Sanitize()+" (value) VALUES (1)")
			require.NoError(t, err)
			completed, err := JobCompleteTx[*riverpgxv5.Driver](ctx, tx, &Job[sequenceKeyTestArgs]{JobRow: head})
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStateCompleted, completed.State)
			_, err = tx.Exec(ctx, "SELECT 1")
			require.NoError(t, err, "sequence failure must not poison the business transaction")
			if commit {
				require.NoError(t, tx.Commit(ctx))
			} else {
				require.NoError(t, tx.Rollback(ctx))
			}

			var count int
			require.NoError(t, bundle.pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{bundle.schema, "sequence_business"}.Sanitize()).Scan(&count))
			head, err = bundle.client.JobGet(ctx, head.ID)
			require.NoError(t, err)
			if commit {
				require.Equal(t, 1, count)
				require.Equal(t, rivertype.JobStateCompleted, head.State)
			} else {
				require.Zero(t, count)
				require.Equal(t, rivertype.JobStateRunning, head.State)
			}
			tail, err := bundle.client.JobGet(ctx, rows[1].Job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStatePending, tail.State)
			require.Nil(t, head.SequenceResolvedAt)
		})
	}
}

func TestSequenceConcurrentInsertTransactions(t *testing.T) {
	t.Parallel()

	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("FirstCommit=%t", commit), func(t *testing.T) {
			t.Parallel()

			bundle := setupSequenceIntegration(t)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			seed, err := bundle.client.Insert(ctx, sequenceKeyTestArgs{AccountID: 100}, nil)
			require.NoError(t, err)
			tx1, err := bundle.pool.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx1.Rollback(context.Background()) })
			first, err := bundle.client.InsertTx(ctx, tx1, sequenceKeyTestArgs{AccountID: 100, Action: "first"}, nil)
			require.NoError(t, err)
			tx2, err := bundle.pool.Begin(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx2.Rollback(context.Background()) })
			var pid int
			require.NoError(t, tx2.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid))

			type insertResult struct {
				result *rivertype.JobInsertResult
				err    error
			}
			var finished testsignal.TestSignal[insertResult]
			finished.Init(t)
			go func() {
				result, err := bundle.client.InsertTx(ctx, tx2, sequenceKeyTestArgs{AccountID: 100, Action: "second"}, nil)
				finished.Signal(insertResult{result: result, err: err})
			}()

			// Observe the server-side wait rather than relying on scheduler sleeps.
			waiting := false
			require.Eventually(t, func() bool {
				err := bundle.pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')", pid).Scan(&waiting)
				return err == nil && waiting
			}, 3*time.Second, 5*time.Millisecond)
			if commit {
				require.NoError(t, tx1.Commit(ctx))
			} else {
				require.NoError(t, tx1.Rollback(ctx))
			}
			inserted := finished.WaitOrTimeout()
			require.NotNil(t, inserted.result)
			require.NoError(t, inserted.err)
			require.NoError(t, tx2.Commit(ctx))
			if commit {
				require.Equal(t, int64(2), *first.Job.SequencePosition)
				require.Equal(t, int64(3), *inserted.result.Job.SequencePosition)
			} else {
				_, err := bundle.client.JobGet(ctx, first.Job.ID)
				require.ErrorIs(t, err, ErrNotFound)
				require.Equal(t, int64(2), *inserted.result.Job.SequencePosition)
			}
			_, err = bundle.client.SequenceReconcile(ctx, *seed.Job.SequenceKey)
			require.NoError(t, err)
			var active int
			require.NoError(t, bundle.pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{bundle.schema, "river_job"}.Sanitize()+" WHERE state IN ('available','running','scheduled','retryable')").Scan(&active))
			require.Equal(t, 1, active)
		})
	}
}

func TestSequenceConfig(t *testing.T) {
	t.Parallel()

	t.Run("DefaultsPreserveAcceptanceSeparation", func(t *testing.T) {
		t.Parallel()

		original := SequenceConfig{DisableReconciler: true}
		config := (&Config{Sequences: original}).WithDefaults()
		require.Equal(t, SequenceConfig{DisableReconciler: true}, original)
		require.False(t, config.Sequences.AcceptNewJobs)
		require.True(t, config.Sequences.DisableReconciler)
		require.Equal(t, 32, config.Sequences.BatchSize)
		require.Equal(t, time.Second, config.Sequences.PollInterval)
		require.Equal(t, 2*time.Second, config.Sequences.TransactionTimeout)
		require.NoError(t, config.Sequences.validate())
	})

	t.Run("InvalidValues", func(t *testing.T) {
		t.Parallel()

		for _, config := range []SequenceConfig{
			{BatchSize: -1},
			{BatchSize: 257},
			{PollInterval: time.Millisecond},
			{PollInterval: 2 * time.Minute},
			{TransactionTimeout: time.Millisecond},
			{TransactionTimeout: time.Minute},
		} {
			require.Error(t, config.withDefaults().validate())
		}
	})
}

func TestSequenceFinalHookPayload(t *testing.T) {
	t.Parallel()

	for _, middleware := range []bool{false, true} {
		t.Run(fmt.Sprintf("Middleware=%t", middleware), func(t *testing.T) {
			t.Parallel()

			bundle := setupSequenceIntegration(t)
			ctx := context.Background()
			config := &Config{Logger: riversharedtest.Logger(t), Schema: bundle.schema, Sequences: SequenceConfig{AcceptNewJobs: true}}
			if middleware {
				config.Middleware = []rivertype.Middleware{&sequenceMutationMiddleware{}}
			} else {
				config.Hooks = []rivertype.Hook{&sequenceMutationHook{}}
			}
			client, err := NewClient(bundle.driver, config)
			require.NoError(t, err)
			changed, err := client.Insert(ctx, sequenceKeyTestArgs{AccountID: 100}, nil)
			require.NoError(t, err)
			expected, err := bundle.client.Insert(ctx, sequenceKeyTestArgs{AccountID: 200}, nil)
			require.NoError(t, err)
			require.Equal(t, expected.Job.SequenceKey, changed.Job.SequenceKey)
			require.Equal(t, int64(1), *changed.Job.SequencePosition)
			require.Equal(t, int64(2), *expected.Job.SequencePosition)
			var stored sequenceKeyTestArgs
			require.NoError(t, json.Unmarshal(changed.Job.EncodedArgs, &stored))
			require.Equal(t, int64(200), stored.AccountID)
		})
	}
}

func TestSequenceInsertTxErrorAtomicity(t *testing.T) {
	t.Parallel()

	t.Run("PositionExhaustion", func(t *testing.T) {
		t.Parallel()

		bundle := setupSequenceIntegration(t)
		ctx := context.Background()
		seed, err := bundle.client.Insert(ctx, sequenceKeyTestArgs{AccountID: 100}, nil)
		require.NoError(t, err)
		_, err = bundle.pool.Exec(ctx, "UPDATE "+pgx.Identifier{bundle.schema, "river_sequence_group"}.Sanitize()+" SET next_position=$1 WHERE key=$2", int64(math.MaxInt64-1), *seed.Job.SequenceKey)
		require.NoError(t, err)
		tx, err := bundle.pool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
		_, err = bundle.client.InsertManyTx(ctx, tx, []InsertManyParams{
			{Args: sequenceKeyTestArgs{AccountID: 100, Action: "one"}},
			{Args: sequenceKeyTestArgs{AccountID: 100, Action: "two"}},
		})
		require.ErrorIs(t, err, ErrSequencePositionExhausted)
		// Explicitly commit after the handled error to expose orphan pending rows.
		require.NoError(t, tx.Commit(ctx))
		var count int
		require.NoError(t, bundle.pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{bundle.schema, "river_job"}.Sanitize()).Scan(&count))
		require.Equal(t, 1, count)
	})

	t.Run("PostInsertPilotError", func(t *testing.T) {
		t.Parallel()

		bundle := setupSequenceIntegration(t)
		ctx := context.Background()
		bundle.client.pilot = &sequenceShortResultPilot{Pilot: bundle.client.pilot}
		tx, err := bundle.pool.Begin(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
		_, err = bundle.client.InsertManyTx(ctx, tx, []InsertManyParams{
			{Args: sequenceOrdinaryTestArgs{}},
			{Args: sequenceKeyTestArgs{AccountID: 100}},
		})
		require.ErrorContains(t, err, "sequence insert result count differs")
		require.NoError(t, tx.Commit(ctx))
		var count int
		require.NoError(t, bundle.pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{bundle.schema, "river_job"}.Sanitize()).Scan(&count))
		require.Zero(t, count)
		require.NoError(t, bundle.pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{bundle.schema, "river_sequence_group"}.Sanitize()).Scan(&count))
		require.Zero(t, count)
	})
}

func TestSequenceInsertValidation(t *testing.T) {
	t.Parallel()

	t.Run("Disabled", func(t *testing.T) {
		t.Parallel()

		client := &Client[struct{}]{config: (&Config{}).WithDefaults()}
		_, err := client.prepareSequenceInsert(context.Background(), &sequenceTestExecutor{}, []*riverdriver.JobInsertFastParams{{Args: sequenceKeyTestArgs{}}})
		require.ErrorIs(t, err, ErrSequenceDisabled)
	})

	t.Run("ManualPending", func(t *testing.T) {
		t.Parallel()

		config := (&Config{Sequences: SequenceConfig{AcceptNewJobs: true}}).WithDefaults()
		archetype := baseservice.NewArchetype(config.Logger)
		_, err := insertParamsFromConfigArgsAndOptions(archetype, config, sequenceKeyTestArgs{}, &InsertOpts{Pending: true})
		require.ErrorIs(t, err, ErrSequencePending)
		_, err = insertParamsFromConfigArgsAndOptions(archetype, config, sequencePendingDefaultArgs{}, nil)
		require.ErrorIs(t, err, ErrSequencePending)
		client := &Client[struct{}]{config: config}
		_, err = client.prepareSequenceInsert(context.Background(), &sequenceTestExecutor{}, []*riverdriver.JobInsertFastParams{{Args: sequenceKeyTestArgs{}, State: rivertype.JobStatePending}})
		require.ErrorIs(t, err, ErrSequencePending)
	})

	t.Run("OrdinaryJobsNeverInspectSequenceStorage", func(t *testing.T) {
		t.Parallel()

		client := &Client[struct{}]{config: (&Config{}).WithDefaults()}
		entries, err := client.prepareSequenceInsert(context.Background(), nil, []*riverdriver.JobInsertFastParams{{Kind: "ordinary"}})
		require.NoError(t, err)
		require.Nil(t, entries)
		require.NoError(t, client.attachSequenceInsert(context.Background(), nil, nil, nil))
	})

	t.Run("UnsupportedExecutor", func(t *testing.T) {
		t.Parallel()

		client := &Client[struct{}]{config: (&Config{Sequences: SequenceConfig{AcceptNewJobs: true}}).WithDefaults()}
		_, err := client.prepareSequenceInsert(context.Background(), nil, []*riverdriver.JobInsertFastParams{{Args: sequenceKeyTestArgs{}}})
		require.ErrorIs(t, err, ErrSequenceNotSupported)
	})
}

func TestSequenceKey(t *testing.T) {
	t.Parallel()

	args := sequenceKeyTestArgs{AccountID: 100, Action: "create"}
	encoded, err := json.Marshal(args)
	require.NoError(t, err)

	// These vectors were calculated from the reference's null-delimited SHA-256 protocol.
	for _, test := range []struct {
		name string
		opts SequenceOpts
		want string
	}{
		{"AllDimensions", SequenceOpts{ByArgs: true, ByQueue: true}, "e92a16105b6d70dddb3f411a429d52c2b2ac44f799f54b7c2fc886e740b7368a"},
		{"ArgsOnly", SequenceOpts{ByArgs: true, ExcludeKind: true}, "3430cc2f082bfd69a03dd4c0f14b28dc6d967130f913658478b533308b84c9eb"},
		{"DefaultKind", SequenceOpts{}, "b268340ecdcdd1655315a164802aea82a286a903e770de85a87fe607f06c1b88"},
		{"EmptyDimensionsFallback", SequenceOpts{ExcludeKind: true}, "b268340ecdcdd1655315a164802aea82a286a903e770de85a87fe607f06c1b88"},
		{"QueueOnly", SequenceOpts{ByQueue: true, ExcludeKind: true}, "5cf55e90d96abd028eec3c5af3bafe53f3b93c743cab85399627ef25716da01f"},
		{"SelectedArgs", SequenceOpts{ByArgs: true}, "a73c9e742cdb08cf08fdc65f7d24d28f9484c008db037c972bd3636fbbc839f7"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			key, err := sequenceKey("payments", args.Kind(), encoded, test.opts, args)
			require.NoError(t, err)
			require.Equal(t, test.want, key)
			pointerKey, err := sequenceKey("payments", args.Kind(), encoded, test.opts, &args)
			require.NoError(t, err)
			require.Equal(t, key, pointerKey)
		})
	}

	t.Run("ContinuationPolicyDoesNotChangeIdentity", func(t *testing.T) {
		t.Parallel()

		base, err := sequenceKey("payments", args.Kind(), encoded, SequenceOpts{ByArgs: true}, args)
		require.NoError(t, err)
		continued, err := sequenceKey("payments", args.Kind(), encoded, SequenceOpts{ByArgs: true, ContinueOnCancelled: true, ContinueOnDiscarded: true}, args)
		require.NoError(t, err)
		require.Equal(t, base, continued)
	})

	t.Run("FullEncodedArgsFallback", func(t *testing.T) {
		t.Parallel()

		key, err := sequenceKey("payments", "account_task", []byte(`{"b":2,"a":1}`), SequenceOpts{ByArgs: true}, struct{}{})
		require.NoError(t, err)
		require.Equal(t, "5ca885159adfa827e732daef5ffd02318ea919d21a7f8c84cfe8df24b874ac4b", key)
	})

	t.Run("SelectedFieldMarshalErrorsAreNotIgnored", func(t *testing.T) {
		t.Parallel()

		_, err := sequenceKey("payments", "account_task", []byte(`{}`), SequenceOpts{ByArgs: true}, struct {
			Unsupported chan int `river:"sequence"`
		}{})
		require.ErrorContains(t, err, "marshaling sequence fields")
	})

	t.Run("UnselectedFieldsDoNotChangeIdentity", func(t *testing.T) {
		t.Parallel()

		other := sequenceKeyTestArgs{AccountID: 100, Action: "archive"}
		otherEncoded, err := json.Marshal(other)
		require.NoError(t, err)
		key, err := sequenceKey("payments", args.Kind(), encoded, SequenceOpts{ByArgs: true}, args)
		require.NoError(t, err)
		otherKey, err := sequenceKey("other_queue", other.Kind(), otherEncoded, SequenceOpts{ByArgs: true}, other)
		require.NoError(t, err)
		require.Equal(t, key, otherKey)
	})
}

func TestSequencePeriodicIsolation(t *testing.T) {
	t.Parallel()

	for _, fault := range []string{"disabled", "locked", "missing_table", "none"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()

			bundle := setupSequenceIntegration(t)
			ctx := context.Background()
			config := newTestConfig(t, bundle.schema)
			config.Sequences = SequenceConfig{
				AcceptNewJobs: fault != "disabled", DisableReconciler: true,
				TransactionTimeout: time.Second,
			}
			config.PeriodicJobs = []*PeriodicJob{
				NewPeriodicJob(NeverSchedule(), func() (JobArgs, *InsertOpts) { return sequenceKeyTestArgs{AccountID: 100}, nil }, &PeriodicJobOpts{RunOnStart: true}),
				NewPeriodicJob(NeverSchedule(), func() (JobArgs, *InsertOpts) { return sequenceOrdinaryTestArgs{}, nil }, &PeriodicJobOpts{RunOnStart: true}),
				NewPeriodicJob(NeverSchedule(), func() (JobArgs, *InsertOpts) { return sequenceKeyTestArgs{AccountID: 200}, nil }, &PeriodicJobOpts{RunOnStart: true}),
			}
			client, err := NewClient(bundle.driver, config)
			require.NoError(t, err)
			if fault == "missing_table" {
				_, err := bundle.pool.Exec(ctx, "ALTER TABLE "+pgx.Identifier{bundle.schema, "river_sequence_group"}.Sanitize()+" RENAME TO river_sequence_group_unavailable")
				require.NoError(t, err)
				t.Cleanup(func() {
					_, err := bundle.pool.Exec(context.Background(), "ALTER TABLE "+pgx.Identifier{bundle.schema, "river_sequence_group_unavailable"}.Sanitize()+" RENAME TO river_sequence_group")
					require.NoError(t, err)
				})
			}
			if fault == "locked" {
				seed, err := bundle.client.Insert(ctx, sequenceKeyTestArgs{AccountID: 100}, nil)
				require.NoError(t, err)
				tx, err := bundle.pool.Begin(ctx)
				require.NoError(t, err)
				t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
				_, err = tx.Exec(ctx, "SELECT key FROM "+pgx.Identifier{bundle.schema, "river_sequence_group"}.Sanitize()+" WHERE key=$1 FOR UPDATE", *seed.Job.SequenceKey)
				require.NoError(t, err)
			}

			// Start only the real periodic service so inserted rows remain observable.
			enqueuer := client.periodicJobs.periodicJobEnqueuer
			enqueuer.TestSignals.Init(t)
			require.NoError(t, enqueuer.Start(ctx))
			t.Cleanup(enqueuer.Stop)
			enqueuer.TestSignals.EnteredLoop.WaitOrTimeout()

			var ordinary int
			require.NoError(t, bundle.pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{bundle.schema, "river_job"}.Sanitize()+" WHERE kind=$1", (sequenceOrdinaryTestArgs{}).Kind()).Scan(&ordinary))
			require.Equal(t, 1, ordinary, "ordinary periodic occurrence must commit independently")
			countAccount := func(account int64) int {
				var count int
				require.NoError(t, bundle.pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{bundle.schema, "river_job"}.Sanitize()+" WHERE (args->>'account_id')::bigint=$1", account).Scan(&count))
				return count
			}
			switch fault {
			case "disabled", "missing_table":
				require.Zero(t, countAccount(100))
				require.Zero(t, countAccount(200))
			case "locked":
				require.Eventually(t, func() bool { return countAccount(200) == 1 }, 5*time.Second, 10*time.Millisecond,
					"a blocked group must not lose another group's occurrence")
				require.Equal(t, 1, countAccount(100), "only the existing seed remains")
			case "none":
				require.Eventually(t, func() bool { return countAccount(100) == 1 && countAccount(200) == 1 }, 5*time.Second, 10*time.Millisecond)
			}
		})
	}
}

// TestSequencePeriodicLockDoesNotDelayOrdinaryTicks covers later occurrences,
// including recovery of the original sequence occurrences after a group unlock.
func TestSequencePeriodicLockDoesNotDelayOrdinaryTicks(t *testing.T) {
	t.Parallel()

	bundle := setupSequenceIntegration(t)
	ctx := context.Background()
	for _, account := range []int64{100, 200} {
		_, err := bundle.client.Insert(ctx, sequenceKeyTestArgs{AccountID: account, Action: "seed"}, nil)
		require.NoError(t, err)
	}
	lockTx, err := bundle.pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lockTx.Rollback(context.Background()) })
	_, err = lockTx.Exec(ctx, "SELECT key FROM "+pgx.Identifier{bundle.schema, "river_sequence_group"}.Sanitize()+" FOR UPDATE")
	require.NoError(t, err)

	var firstConstructed, secondConstructed atomic.Int32
	config := newTestConfig(t, bundle.schema)
	config.Sequences = SequenceConfig{AcceptNewJobs: true, DisableReconciler: true, TransactionTimeout: 2 * time.Second}
	config.PeriodicJobs = []*PeriodicJob{
		NewPeriodicJob(NeverSchedule(), func() (JobArgs, *InsertOpts) {
			firstConstructed.Add(1)
			return sequenceKeyTestArgs{AccountID: 100, Action: "periodic"}, nil
		}, &PeriodicJobOpts{RunOnStart: true}),
		NewPeriodicJob(PeriodicInterval(200*time.Millisecond), func() (JobArgs, *InsertOpts) {
			return sequenceOrdinaryTestArgs{}, nil
		}, nil),
		NewPeriodicJob(NeverSchedule(), func() (JobArgs, *InsertOpts) {
			secondConstructed.Add(1)
			return sequenceKeyTestArgs{AccountID: 200, Action: "periodic"}, nil
		}, &PeriodicJobOpts{RunOnStart: true}),
	}
	client, err := NewClient(bundle.driver, config)
	require.NoError(t, err)
	enqueuer := client.periodicJobs.periodicJobEnqueuer
	var failedSequenceAttempts atomic.Int32
	insert := enqueuer.Config.Insert
	enqueuer.Config.Insert = func(ctx context.Context, tx riverdriver.ExecutorTx, params []*rivertype.JobInsertParams) ([]*rivertype.JobInsertResult, error) {
		results, err := insert(ctx, tx, params)
		if err != nil && len(params) == 1 && params[0].Kind == (sequenceKeyTestArgs{}).Kind() {
			failedSequenceAttempts.Add(1)
		}
		return results, err
	}
	enqueuer.TestSignals.Init(t)
	require.NoError(t, enqueuer.Start(ctx))
	t.Cleanup(enqueuer.Stop)
	enqueuer.TestSignals.EnteredLoop.WaitOrTimeout()

	// Observe a real database lock wait instead of assuming the worker is active.
	require.Eventually(t, func() bool {
		var waiting bool
		require.NoError(t, bundle.pool.QueryRow(ctx,
			"SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%' || $1 || '%')", bundle.schema).Scan(&waiting))
		return waiting
	}, time.Second, 10*time.Millisecond)
	countJobs := func(kind string) int {
		var count int
		require.NoError(t, bundle.pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{bundle.schema, "river_job"}.Sanitize()+" WHERE kind=$1", kind).Scan(&count))
		return count
	}
	// All three ticks must commit before even one isolated lock timeout expires.
	// The former synchronous loop could not insert any until both waits ended.
	require.Eventually(t, func() bool { return countJobs((sequenceOrdinaryTestArgs{}).Kind()) >= 3 },
		1500*time.Millisecond, 10*time.Millisecond)
	require.Equal(t, 2, countJobs((sequenceKeyTestArgs{}).Kind()), "only the committed seeds exist while the groups remain locked")
	require.Equal(t, int32(1), firstConstructed.Load())
	require.Equal(t, int32(1), secondConstructed.Load())

	// Keep the lock until a real pgx timeout closes the failed attempt's
	// connection. Cleanup errors before Commit must not freeze the occurrence.
	require.Eventually(t, func() bool { return failedSequenceAttempts.Load() > 0 },
		3*time.Second, 10*time.Millisecond)
	require.NoError(t, lockTx.Rollback(ctx))
	require.Eventually(t, func() bool { return countJobs((sequenceKeyTestArgs{}).Kind()) == 4 },
		5*time.Second, 10*time.Millisecond)
	enqueuer.Stop()
	require.Equal(t, 4, countJobs((sequenceKeyTestArgs{}).Kind()), "each retained occurrence commits once")
	require.Equal(t, int32(1), firstConstructed.Load(), "recovery must reuse the original occurrence")
	require.Equal(t, int32(1), secondConstructed.Load())
}

func TestSequenceRejectedBatchIsAtomic(t *testing.T) {
	t.Parallel()

	for _, fast := range []bool{false, true} {
		t.Run(fmt.Sprintf("Fast=%t", fast), func(t *testing.T) {
			t.Parallel()

			bundle := setupSequenceIntegration(t)
			ctx := context.Background()
			params := []InsertManyParams{
				{Args: sequenceOrdinaryTestArgs{}},
				{Args: sequenceKeyTestArgs{AccountID: 100}},
			}
			if fast {
				_, err := bundle.client.InsertManyFast(ctx, params)
				require.ErrorIs(t, err, ErrSequenceFastInsert)
			} else {
				params[1].InsertOpts = &InsertOpts{Pending: true}
				_, err := bundle.client.InsertMany(ctx, params)
				require.ErrorIs(t, err, ErrSequencePending)
			}
			var count int
			require.NoError(t, bundle.pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{bundle.schema, "river_job"}.Sanitize()).Scan(&count))
			require.Zero(t, count)
			require.NoError(t, bundle.pool.QueryRow(ctx, "SELECT count(*) FROM "+pgx.Identifier{bundle.schema, "river_sequence_group"}.Sanitize()).Scan(&count))
			require.Zero(t, count)
		})
	}
}

// TestSequenceRetryTxUsesDatabaseClock verifies public retry and fetch behavior
// inside a caller-owned transaction without a stubbed application clock.
func TestSequenceRetryTxUsesDatabaseClock(t *testing.T) {
	t.Parallel()

	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("Commit=%t", commit), func(t *testing.T) {
			t.Parallel()

			bundle := setupSequenceIntegration(t)
			ctx := context.Background()
			ordinary, err := bundle.client.Insert(ctx, sequenceOrdinaryTestArgs{}, &InsertOpts{Pending: true})
			require.NoError(t, err)
			head, err := bundle.client.Insert(ctx, sequenceKeyTestArgs{AccountID: 1, Action: "head"}, nil)
			require.NoError(t, err)
			follower, err := bundle.client.Insert(ctx, sequenceKeyTestArgs{AccountID: 1, Action: "follower"}, nil)
			require.NoError(t, err)

			tx, err := bundle.pool.Begin(ctx)
			require.NoError(t, err)
			defer tx.Rollback(ctx)
			var databaseNow time.Time
			require.NoError(t, tx.QueryRow(ctx, "SELECT CURRENT_TIMESTAMP").Scan(&databaseNow))
			ordinaryRetry, err := bundle.client.JobRetryTx(ctx, tx, ordinary.Job.ID)
			require.NoError(t, err)
			sequenceRetry, err := bundle.client.JobRetryTx(ctx, tx, head.Job.ID)
			require.NoError(t, err)
			require.Equal(t, databaseNow.UTC(), ordinaryRetry.ScheduledAt)
			require.Equal(t, ordinaryRetry.ScheduledAt, sequenceRetry.ScheduledAt,
				"both retry paths must use the database transaction timestamp by default")

			claimed, err := bundle.driver.UnwrapExecutor(tx).JobGetAvailable(ctx, &riverdriver.JobGetAvailableParams{
				ClientID: "retry_clock", MaxAttemptedBy: 5, MaxToLock: 10, Queue: QueueDefault, Schema: bundle.schema,
			})
			require.NoError(t, err)
			claimedIDs := make([]int64, len(claimed.Jobs))
			for i, job := range claimed.Jobs {
				claimedIDs[i] = job.ID
			}
			require.ElementsMatch(t, []int64{ordinary.Job.ID, head.Job.ID}, claimedIDs,
				"both retries must be immediately eligible while the follower remains pending")

			// Uncommitted retry and claim changes must remain invisible to others.
			for _, id := range []int64{ordinary.Job.ID, head.Job.ID, follower.Job.ID} {
				job, err := bundle.client.JobGet(ctx, id)
				require.NoError(t, err)
				require.Equal(t, rivertype.JobStatePending, job.State)
			}
			if commit {
				require.NoError(t, tx.Commit(ctx))
			} else {
				require.NoError(t, tx.Rollback(ctx))
			}
			for _, original := range []*rivertype.JobRow{ordinary.Job, head.Job} {
				job, err := bundle.client.JobGet(ctx, original.ID)
				require.NoError(t, err)
				if commit {
					require.Equal(t, rivertype.JobStateRunning, job.State)
					require.Equal(t, databaseNow.UTC(), job.ScheduledAt)
				} else {
					require.Equal(t, rivertype.JobStatePending, job.State)
					require.Equal(t, original.ScheduledAt, job.ScheduledAt)
				}
			}
			unchangedFollower, err := bundle.client.JobGet(ctx, follower.Job.ID)
			require.NoError(t, err)
			require.Equal(t, rivertype.JobStatePending, unchangedFollower.State)
		})
	}
}

func TestSequenceTaggedFields(t *testing.T) {
	t.Parallel()

	t.Run("ReferenceTags", func(t *testing.T) {
		t.Parallel()

		args := struct {
			First    int    `json:"first,omitempty" river:"unique,sequence"`
			Named    string `river:"sequence"`
			Ignored  string `json:"-"               river:"sequence"`
			Ordinary string `json:"ordinary"`
			private  string `river:"sequence"`
		}{First: 3, Named: "value", Ignored: "ignored", Ordinary: "ordinary", private: "hidden"}
		require.Equal(t, map[string]any{"first": 3, "Named": "value"}, sequenceTaggedFields(args))
		require.Equal(t, sequenceTaggedFields(args), sequenceTaggedFields(&args))
	})

	t.Run("UnsupportedValuesFallBack", func(t *testing.T) {
		t.Parallel()

		var pointer *sequenceKeyTestArgs
		for _, args := range []any{nil, pointer, map[string]any{"account": 1}, "value"} {
			require.Empty(t, sequenceTaggedFields(args))
		}
	})
}

func TestValidateSequenceKey(t *testing.T) {
	t.Parallel()

	require.Error(t, validateSequenceKey(""))
	require.Error(t, validateSequenceKey(strings.Repeat("a", 512)))
	require.NoError(t, validateSequenceKey(strings.Repeat("a", 64)))
}

// sequenceIntegrationBundle isolates transaction and hook integration tests by schema.
type sequenceIntegrationBundle struct {
	client *Client[pgx.Tx]
	driver *riverpgxv5.Driver
	pool   *pgxpool.Pool
	schema string
}

// setupSequenceIntegration creates a migrated schema without starting background coordination.
func setupSequenceIntegration(t *testing.T) *sequenceIntegrationBundle {
	t.Helper()

	ctx := context.Background()
	pool := riversharedtest.DBPool(ctx, t)
	driver := riverpgxv5.New(pool)
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)
	client, err := NewClient(driver, &Config{
		Logger: riversharedtest.Logger(t), Schema: schema,
		Sequences: SequenceConfig{AcceptNewJobs: true},
	})
	require.NoError(t, err)
	return &sequenceIntegrationBundle{client: client, driver: driver, pool: pool, schema: schema}
}

// sequenceMutationHook tests grouping against the payload that is actually persisted.
type sequenceMutationHook struct{ HookDefaults }

// InsertBegin changes the account identifier after ordinary insert preparation.
func (sequenceMutationHook) InsertBegin(_ context.Context, params *rivertype.JobInsertParams) error {
	params.EncodedArgs = []byte(`{"account_id":200,"action":"hook"}`)
	return nil
}

// sequenceMutationMiddleware exercises the outer insert middleware path.
type sequenceMutationMiddleware struct{ MiddlewareDefaults }

// InsertMany replaces the persisted account before invoking the insert operation.
func (sequenceMutationMiddleware) InsertMany(ctx context.Context, params []*rivertype.JobInsertParams, next func(context.Context) ([]*rivertype.JobInsertResult, error)) ([]*rivertype.JobInsertResult, error) {
	for _, param := range params {
		param.EncodedArgs = []byte(`{"account_id":200,"action":"middleware"}`)
	}
	return next(ctx)
}

// sequenceOrdinaryTestArgs verifies that a rejected mixed insert stays atomic.
type sequenceOrdinaryTestArgs struct{}

// Kind identifies an ordinary job with no ordering options.
func (sequenceOrdinaryTestArgs) Kind() string { return "sequence_ordinary_test" }

// sequenceShortResultPilot simulates a post-write plugin contract failure.
type sequenceShortResultPilot struct{ riverpilot.Pilot }

// JobInsertMany performs real inserts, then returns an invalid result count.
func (p *sequenceShortResultPilot) JobInsertMany(ctx context.Context, tx riverdriver.ExecutorTx, params *riverdriver.JobInsertFastManyParams) ([]*riverdriver.JobInsertFastResult, error) {
	results, err := p.Pilot.JobInsertMany(ctx, tx, params)
	if err != nil {
		return nil, err
	}
	return results[:0], nil
}
