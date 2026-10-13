// Command sequencebypass previews or releases a single sequence without its
// group table. Stop all sequence writers and reconcilers before applying it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivershared/util/dbutil"
)

// options bounds a deliberately scoped offline recovery operation.
type options struct {
	apply          bool
	batchSize      int
	databaseURL    string
	key            string
	reason         string
	schema         string
	timeout        time.Duration
	writersStopped bool
}

// main exits unsuccessfully if recovery is incomplete or preconditions fail.
func main() {
	opts := options{}
	flag.BoolVar(&opts.apply, "apply", false, "Apply the previewed changes; defaults to a read-only preview")
	flag.IntVar(&opts.batchSize, "batch-size", 32, "Maximum members per short transaction (1-256)")
	flag.StringVar(&opts.databaseURL, "database-url", os.Getenv("DATABASE_URL"), "Postgres connection URL (defaults to DATABASE_URL)")
	flag.StringVar(&opts.key, "key", "", "Exact sequence key; wildcards are not supported")
	flag.StringVar(&opts.reason, "reason", "", "Reason for explicitly waiving order (required with --apply)")
	flag.StringVar(&opts.schema, "schema", "", "Explicit schema containing river_job")
	flag.DurationVar(&opts.timeout, "timeout", 30*time.Second, "Total time budget; partial progress is retained on failure")
	flag.BoolVar(&opts.writersStopped, "writers-stopped", false, "Confirm all sequence writers and reconcilers have been stopped")
	flag.Parse()
	if err := run(opts, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run owns and closes the database pool before main exits.
func run(opts options, output io.Writer) error {
	if err := opts.validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, opts.databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	return recoverSequence(ctx, riverpgxv5.New(pool).GetExecutor(), output, opts)
}

// validate rejects unscoped recovery and unacknowledged concurrent writers.
func (opts options) validate() error {
	if opts.databaseURL == "" || opts.schema == "" || opts.key == "" || len(opts.key) >= 512 {
		return errors.New("database-url, explicit schema, and an exact sequence key are required")
	}
	if opts.batchSize < 1 || opts.batchSize > 256 || opts.timeout <= 0 {
		return errors.New("batch-size must be 1-256 and timeout must be positive")
	}
	if opts.apply && (!opts.writersStopped || opts.reason == "") {
		return errors.New("--apply requires --writers-stopped and --reason; ordinary workers may keep running")
	}
	return nil
}

// recoverSequence only changes marked members and never consults a group table.
func recoverSequence(ctx context.Context, exec riverdriver.Executor, output io.Writer, opts options) error {
	if err := opts.validate(); err != nil {
		return err
	}
	storage, ok := exec.(riverdriver.SequenceExecutor)
	if !ok {
		return errors.New("driver does not support sequence recovery")
	}
	params := &riverdriver.SequenceGroupKeyParams{Key: opts.key, Schema: opts.schema}
	stats, err := storage.SequenceJobStats(ctx, params)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(output, "schema=%q key=%q unresolved=%d nonterminal=%d apply=%t reason=%q\n",
		opts.schema, opts.key, stats.Unresolved, stats.Nonterminal, opts.apply, opts.reason); err != nil {
		return err
	}
	if !opts.apply {
		return nil
	}
	for {
		batchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		count, batchErr := recoverBatch(batchCtx, exec, opts)
		cancel()
		if batchErr != nil {
			return fmt.Errorf("recovery incomplete; already committed batches are retained: %w", batchErr)
		}
		stats, err = storage.SequenceJobStats(ctx, params)
		if err != nil {
			return err
		}
		if count == 0 {
			if stats.Unresolved > 0 {
				return fmt.Errorf("recovery incomplete: %d locked members remain; retry after stopping sequence transactions", stats.Unresolved)
			}
			break
		}
	}
	_, err = fmt.Fprintln(output, "Ordering waived. Keep sequence writers disabled; restore the group in bypass mode and drain all nonterminal members before restoring ordered mode.")
	return err
}

// recoverBatch atomically waives a bounded batch while retaining execution state.
func recoverBatch(ctx context.Context, exec riverdriver.Executor, opts options) (int, error) {
	tx, err := exec.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer dbutil.RollbackWithoutCancel(ctx, tx)
	storage, ok := tx.(riverdriver.SequenceExecutor)
	if !ok {
		return 0, errors.New("transaction does not support sequence recovery")
	}
	if err := storage.SequenceSetLocalTimeouts(ctx); err != nil {
		return 0, err
	}
	count, err := storage.SequenceJobBypass(ctx, &riverdriver.SequenceJobBypassParams{
		Key: opts.key, Limit: opts.batchSize, Now: time.Now().UTC(), Schema: opts.schema,
	})
	if err != nil {
		return 0, err
	}
	return count, tx.Commit(ctx)
}
