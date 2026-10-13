package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdbtest"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

// recoveryArgs supplies one deliberately small recovery group.
type recoveryArgs struct{}

// Kind identifies test recovery jobs.
func (recoveryArgs) Kind() string { return "recovery" }

// SequenceOpts enables ordering for every recovery job.
func (recoveryArgs) SequenceOpts() river.SequenceOpts { return river.SequenceOpts{} }

func TestOptionsValidate(t *testing.T) {
	t.Parallel()

	opts := options{batchSize: 32, databaseURL: "postgres://localhost/test", key: "key", schema: "public", timeout: time.Second}
	require.NoError(t, opts.validate())
	opts.apply = true
	require.Error(t, opts.validate())
	opts.writersStopped = true
	require.Error(t, opts.validate())
	opts.reason = "recovery drill"
	require.NoError(t, opts.validate())
	opts.schema = ""
	require.Error(t, opts.validate())
}

func TestRecoverSequenceWithoutGroupTable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	driver := riverpgxv5.New(riversharedtest.DBPool(ctx, t))
	schema := riverdbtest.TestSchema(ctx, t, driver, nil)
	client, err := river.NewClient(driver, &river.Config{Schema: schema, Sequences: river.SequenceConfig{AcceptNewJobs: true}})
	require.NoError(t, err)
	first, err := client.Insert(ctx, recoveryArgs{}, nil)
	require.NoError(t, err)
	future, err := client.Insert(ctx, recoveryArgs{}, &river.InsertOpts{ScheduledAt: time.Now().Add(time.Hour)})
	require.NoError(t, err)
	ordinary, err := driver.GetExecutor().JobInsertFastMany(ctx, &riverdriver.JobInsertFastManyParams{
		Schema: schema,
		Jobs: []*riverdriver.JobInsertFastParams{{
			EncodedArgs: []byte(`{}`), Kind: "manual_pending", MaxAttempts: 3,
			Metadata: []byte(`{}`), Priority: 1, Queue: river.QueueDefault, State: rivertype.JobStatePending, Tags: []string{},
		}},
	})
	require.NoError(t, err)
	exec := driver.GetExecutor()
	require.NoError(t, exec.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.river_sequence_group RENAME TO hidden_sequence_group", schema)))
	defer func() {
		require.NoError(t, exec.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.hidden_sequence_group RENAME TO river_sequence_group", schema)))
	}()
	opts := options{batchSize: 1, databaseURL: "test", key: *first.Job.SequenceKey, schema: schema, timeout: time.Second}
	var output bytes.Buffer
	require.NoError(t, recoverSequence(ctx, exec, &output, opts))
	before, err := client.JobGet(ctx, first.Job.ID)
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStatePending, before.State)
	opts.apply, opts.writersStopped, opts.reason = true, true, "test recovery"
	require.NoError(t, recoverSequence(ctx, exec, &output, opts))
	for _, test := range []struct {
		id    int64
		state rivertype.JobState
	}{{first.Job.ID, rivertype.JobStateAvailable}, {future.Job.ID, rivertype.JobStateScheduled}} {
		job, err := client.JobGet(ctx, test.id)
		require.NoError(t, err)
		require.Equal(t, test.state, job.State)
		require.NotNil(t, job.SequenceResolvedAt)
		require.Zero(t, job.Attempt)
	}
	pending, err := client.JobGet(ctx, ordinary[0].Job.ID)
	require.NoError(t, err)
	require.Equal(t, rivertype.JobStatePending, pending.State)
	require.Nil(t, pending.SequenceKey)
	require.NoError(t, recoverSequence(ctx, exec, &output, opts))
}
