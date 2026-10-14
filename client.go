package river

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/riverqueue/river/internal/dblist"
	"github.com/riverqueue/river/internal/dbunique"
	"github.com/riverqueue/river/internal/jobcompleter"
	"github.com/riverqueue/river/internal/jobexecutor"
	"github.com/riverqueue/river/internal/joblease"
	"github.com/riverqueue/river/internal/leadership"
	"github.com/riverqueue/river/internal/maintenance"
	"github.com/riverqueue/river/internal/notifier"
	"github.com/riverqueue/river/internal/notifylimiter"
	"github.com/riverqueue/river/internal/pluginconfig"
	"github.com/riverqueue/river/internal/pluginlookup"
	"github.com/riverqueue/river/internal/retrypolicy"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/internal/riverplugin"
	sequenceruntime "github.com/riverqueue/river/internal/sequence"
	"github.com/riverqueue/river/internal/workunit"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivershared/util/maputil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/valutil"
	"github.com/riverqueue/river/rivertype"
)

const (
	FetchCooldownDefault = 100 * time.Millisecond
	FetchCooldownMin     = 1 * time.Millisecond

	FetchPollIntervalDefault = 1 * time.Second
	FetchPollIntervalMin     = 1 * time.Millisecond

	JobStuckThresholdDefault = 10 * time.Second
	JobTimeoutDefault        = 1 * time.Minute
	MaxAttemptsDefault       = rivercommon.MaxAttemptsDefault
	PriorityDefault          = rivercommon.PriorityDefault
	QueueDefault             = rivercommon.QueueDefault
	QueueNumWorkersMax       = 10_000
)

var (
	postgresSchemaNameRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

	reindexerIndexNamesDefault = []string{ //nolint:gochecknoglobals
		"river_job_args_index",
		"river_job_kind",
		"river_job_metadata_index",
		"river_job_pkey",
		"river_job_prioritized_fetching_index",
		"river_job_state_and_finalized_at_index",
		"river_job_unique_idx",
	}
)

// TestConfig contains configuration specific to test environments.
type TestConfig struct {
	// DisableUniqueEnforcement disables the application of unique job
	// constraints. This is useful for testing scenarios when testing a worker
	// that typically uses uniqueness, but where enforcing uniqueness would cause
	// conflicts with parallel test execution.
	//
	// The [rivertest.Worker] type automatically disables uniqueness enforcement
	// when creating jobs.
	DisableUniqueEnforcement bool

	// Time is a time generator to make time stubbable in tests.
	Time rivertype.TimeGenerator
}

// Config is the configuration for a Client.
//
// Both Queues and Workers are required for a client to work jobs, but an
// insert-only client can be initialized by omitting Queues, and not calling
// Start for the client. Workers can also be omitted, but it's better to include
// it so River can check that inserted job kinds have a worker that can run
// them.
type Config struct {
	// AdvisoryLockPrefix is a configurable 32-bit prefix that River will use
	// when generating any key to acquire a Postgres advisory lock. All advisory
	// locks share the same 64-bit number space, so this allows a calling
	// application to guarantee that a River advisory lock will never conflict
	// with one of its own by cordoning each type to its own prefix.
	//
	// If this value isn't set, River defaults to generating key hashes across
	// the entire 64-bit advisory lock number space, which is large enough that
	// conflicts are exceedingly unlikely. If callers don't strictly need this
	// option then it's recommended to leave it unset because the prefix leaves
	// only 32 bits of number space for advisory lock hashes, so it makes
	// internally conflicting River-generated keys more likely.
	//
	// Advisory locks are currently only used for the deprecated fallback/slow
	// path of unique job insertion when pending, scheduled, available, or running
	// are omitted from a customized ByState configuration.
	AdvisoryLockPrefix int32

	// CancelledJobRetentionPeriod is the amount of time to keep cancelled jobs
	// around before they're removed permanently.
	//
	// The special value -1 disables deletion of cancelled jobs.
	//
	// Defaults to 24 hours.
	CancelledJobRetentionPeriod time.Duration

	// CompletedJobRetentionPeriod is the amount of time to keep completed jobs
	// around before they're removed permanently.
	//
	// The special value -1 disables deletion of completed jobs.
	//
	// Defaults to 24 hours.
	CompletedJobRetentionPeriod time.Duration

	// DiscardedJobRetentionPeriod is the amount of time to keep discarded jobs
	// around before they're removed permanently.
	//
	// The special value -1 disables deletion of discarded jobs.
	//
	// Defaults to 7 days.
	DiscardedJobRetentionPeriod time.Duration

	// ErrorHandler can be configured to be invoked in case of an error or panic
	// occurring in a job. This is often useful for logging and exception
	// tracking, but can also be used to customize retry behavior.
	ErrorHandler ErrorHandler

	// FetchCooldown is the minimum amount of time to wait between fetches of new
	// jobs. Jobs will only be fetched *at most* this often, but if no new jobs
	// are coming in via LISTEN/NOTIFY then fetches may be delayed as long as
	// FetchPollInterval.
	//
	// Throughput is limited by this value.
	//
	// Individual QueueConfig structs may override this for a specific queue.
	//
	// Defaults to 100 ms.
	FetchCooldown time.Duration

	// FetchOnlyKnownKinds restricts job fetching to kinds registered in Workers,
	// including kind aliases. Jobs of other kinds remain available without
	// consuming attempts, allowing clients with different workers to share a queue.
	//
	// This option only affects fetching. A leader's rescuer still handles stuck
	// jobs across all queues and discards those whose kinds it doesn't know.
	// Clients with partial worker registries should use LeaderElectionDisabled,
	// with another eligible client that has all job kinds registered.
	//
	// Defaults to false, so unknown kinds are fetched and fail with an
	// UnknownJobKindError as usual.
	FetchOnlyKnownKinds bool

	// FetchPollInterval is the amount of time between periodic fetches for new
	// jobs. Typically new jobs will be picked up ~immediately after insert via
	// LISTEN/NOTIFY, but this provides a fallback.
	//
	// Individual QueueConfig structs may override this for a specific queue.
	//
	// Defaults to 1 second.
	FetchPollInterval time.Duration

	// ID is the unique identifier for this client. If not set, a random
	// identifier will be generated.
	//
	// This is used to identify the client in job attempts and for leader election.
	// This value must be unique across all clients in the same database and
	// schema and there must not be more than one process running with the same
	// ID at the same time.
	//
	// A client ID should differ between different programs and must be unique
	// across all clients in the same database and schema. There must not be
	// more than one process running with the same ID at the same time.
	// Duplicate IDs between processes will lead to facilities like leader
	// election or client statistics to fail in novel ways. However, the client
	// ID is shared by all executors within any given client. (i.e.  different
	// Go processes have different IDs, but IDs are shared within any given
	// process.)
	//
	// If in doubt, leave this property empty.
	ID string

	// JobCleanerTimeout is the timeout of the individual queries within the job
	// cleaner.
	//
	// Defaults to 30 seconds, which should be more than enough time for most
	// deployments.
	JobCleanerTimeout time.Duration

	// JobInsertMiddleware are optional functions that can be called around job
	// insertion.
	//
	// Deprecated: Prefer the use of Plugins instead (which may contain
	// instances of rivertype.JobInsertMiddleware).
	JobInsertMiddleware []rivertype.JobInsertMiddleware

	// JobStuckHandler is invoked when a producer detects that a job exceeded
	// its timeout and did not return from context cancellation within the
	// allotted JobStuckThreshold (and if it didn't, we usually assume it won't
	// return at all). The handler receives minimal information about the stuck
	// job and the total number of jobs currently considered stuck across the
	// client.
	//
	// JobStuckHandler lets an implementation indicate that a new worker slot
	// should be opened to replace the one now occupied by a stuck job. It can
	// also be used (for example) to stop and exit the program if too many jobs
	// have been reported stuck.
	JobStuckHandler JobStuckHandler

	// JobLease enables renewable leases for jobs acquired by this client. Nil
	// preserves legacy timeout-based rescue. Enable only after upgrading all
	// runtimes sharing the schema; leases do not replace the business JobTimeout.
	JobLease *JobLeaseConfig

	// JobStuckThreshold is the amount of time after JobTimeout elapses to
	// wait before a still-running job is considered stuck.
	//
	// Defaults to 10 seconds.
	JobStuckThreshold time.Duration

	// JobTimeout is the maximum amount of time a job is allowed to run before its
	// context is cancelled. A timeout of zero means JobTimeoutDefault will be
	// used, whereas a value of -1 means the job's context will not be cancelled
	// unless the Client is shutting down.
	//
	// Defaults to 1 minute.
	JobTimeout time.Duration

	// Hooks are functions that may activate at certain points during a job's
	// lifecycle (see rivertype.Hook), installed globally.
	//
	// The effect of hooks in this list will depend on the specific hook
	// interfaces they implement. rivertype.HookInsertBegin will cause the hook
	// to be invoked before a job is inserted. rivertype.HookMetricEmit will
	// cause the hook to be invoked when River emits a metric. Implementing
	// rivertype.HookWorkBegin will cause it to be invoked before a job is
	// worked. Hook structs may implement multiple hook interfaces.
	//
	// Order in this list is significant. A hook that appears first will be
	// entered before a hook that appears later. For any particular phase, order
	// is relevant only for hooks that will run for that phase. For example, if
	// two rivertype.HookInsertBegin are separated by a rivertype.HookWorkBegin,
	// during job insertion those two outer hooks will run one after another,
	// and the work hook between them will not run. When a job is worked, the
	// work hook runs and the insertion hooks on either side of it are skipped.
	//
	// Jobs may have their own specific hooks by implementing JobArgsWithHooks or
	// JobArgsWithPlugins.
	//
	// Entries in Hooks are installed only as hooks, even if they also implement
	// rivertype.Middleware. Use Plugins for an extension that should act as
	// both a hook and middleware.
	Hooks []rivertype.Hook

	// LeaderElectionDisabled prevents this client from participating in leader
	// election and running maintenance services. It still fetches and executes
	// jobs from its configured queues normally.
	//
	// At least one other started client in the same database and schema must
	// remain eligible to lead for scheduled jobs, retries, periodic enqueueing,
	// stuck job rescue, and cleanup to progress. This client will never become
	// leader, even if no other eligible client is running.
	//
	// PeriodicJobs must be empty when this is true. Periodic jobs cannot be
	// configured on this client, though it can still execute periodic jobs
	// enqueued by another client.
	//
	// Defaults to false.
	LeaderElectionDisabled bool

	// Logger is the structured logger to use for logging purposes. If none is
	// specified, logs will be emitted to STDOUT with messages at warn level
	// or higher.
	Logger *slog.Logger

	// MaxAttempts is the default number of times a job will be retried before
	// being discarded. This value is applied to all jobs by default, and can be
	// overridden on individual job types on the JobArgs or on a per-job basis at
	// insertion time.
	//
	// If not specified, defaults to 25 (MaxAttemptsDefault).
	MaxAttempts int

	// Middleware contains middleware that may activate at certain points during
	// a job's lifecycle (see rivertype.Middleware), installed globally.
	//
	// The effect of middleware in this list will depend on the specific
	// middleware interfaces they implement, so for example implementing
	// rivertype.JobInsertMiddleware will cause the middleware to be invoked
	// when jobs are inserted, and implementing rivertype.WorkerMiddleware will
	// cause it to be invoked when a job is worked. Middleware structs may
	// implement multiple middleware interfaces.
	//
	// Order in this list is significant. Middleware that appears first will be
	// entered before middleware that appears later. For any particular phase,
	// order is relevant only for middlewares that will run for that phase. For
	// example, if two rivertype.JobInsertMiddleware are separated by a
	// rivertype.WorkerMiddleware, during job insertion those two outer
	// middlewares will run one after another, and the work middleware between
	// them will not run. When a job is worked, the work middleware runs and the
	// insertion middlewares on either side of it are skipped.
	//
	// Entries in Middleware are installed only as middleware, even if they also
	// implement rivertype.Hook. Use Plugins for an extension that should act as
	// both middleware and a hook.
	Middleware []rivertype.Middleware

	// Plugins contains extensions installed globally as hooks, middleware, or
	// both.
	//
	// A type qualifies as a plugin by implementing [rivertype.Plugin]. Most
	// existing hook and middleware implementations already do this by embedding
	// HookDefaults or MiddlewareDefaults. If a type participates on both sides,
	// it may embed PluginDefaults, or embed both HookDefaults and
	// MiddlewareDefaults directly and define its own IsPlugin method, then
	// implement any operation-specific hook or middleware interfaces it needs.
	//
	// Use Hooks or Middleware when an extension should be installed only as the
	// corresponding kind. Use Plugins when it should be eligible as both.
	// Jobs may have their own specific plugins by implementing
	// JobArgsWithPlugins.
	Plugins []rivertype.Plugin

	// PeriodicJobs are a set of periodic jobs to run at the specified intervals
	// in the client.
	//
	// Must be empty when LeaderElectionDisabled is true.
	PeriodicJobs []*PeriodicJob

	// PollOnly starts the client in "poll only" mode, which avoids issuing
	// `LISTEN` statements to wait for events like a leadership resignation or
	// new job available. The program instead polls periodically to look for
	// changes (checking for new jobs on the period in FetchPollInterval).
	//
	// The downside of this mode of operation is that events will usually be
	// noticed less quickly. A new job in the queue may have to wait up to
	// FetchPollInterval to be locked for work. When a leader resigns, it will
	// be up to five seconds before a new one elects itself.
	//
	// The upside is that it makes River compatible with systems where
	// listen/notify isn't available. For example, PgBouncer in transaction
	// pooling mode.
	PollOnly bool

	// Queues is a list of queue names for this client to operate on along with
	// configuration for the queue like the maximum number of workers to run for
	// each queue.
	//
	// This field may be omitted for a program that's only queueing jobs rather
	// than working them. If it's specified, then Workers must also be given.
	Queues map[string]QueueConfig

	// ReindexerSchedule is the schedule for running the reindexer. If nil, the
	// reindexer will run at midnight UTC every day.
	ReindexerSchedule PeriodicSchedule

	// ReindexerIndexNames customizes which indexes River periodically reindexes.
	// If nil, River uses [ReindexerIndexNamesDefault]. If non-nil, the provided
	// slice is used as the exact list.
	ReindexerIndexNames []string

	// ReindexerTimeout is the amount of time to wait for the reindexer to run a
	// single reindex operation before cancelling it via context. Set to -1 to
	// disable the timeout.
	//
	// Defaults to 1 minute.
	ReindexerTimeout time.Duration

	// RescueStuckJobsAfter is the amount of time a job can be running before it
	// is considered stuck. A stuck job which has not yet reached its max attempts
	// will be scheduled for a retry, while one which has exhausted its attempts
	// will be discarded.  This prevents jobs from being stuck forever if a worker
	// crashes or is killed.
	//
	// Note that this can result in repeat or duplicate execution of a job that is
	// not actually stuck but is still working. The value should be set higher
	// than the maximum duration you expect your jobs to run. Setting a value too
	// low will result in more duplicate executions, whereas too high of a value
	// will result in jobs being stuck for longer than necessary before they are
	// retried.
	//
	// For jobs without leases, RescueStuckJobsAfter must be greater than
	// JobTimeout. Otherwise, jobs would become eligible for rescue while still
	// running. Leased jobs use JobLease.RescueGracePeriod instead.
	//
	// Defaults to 1 hour, or in cases where JobTimeout has been configured and
	// is greater than 1 hour, JobTimeout + 1 hour.
	RescueStuckJobsAfter time.Duration

	// RetryPolicy is a configurable retry policy for the client.
	//
	// Defaults to DefaultRetryPolicy.
	RetryPolicy ClientRetryPolicy

	// Schema is a non-standard Schema where River tables are located. All table
	// references in database queries will use this value as a prefix.
	//
	// Defaults to empty, which causes the client to look for tables using the
	// setting of Postgres `search_path`.
	Schema string

	// SoftStopTimeout is the maximum amount of time that the client will wait
	// for running jobs to finish during a stop before their contexts are
	// cancelled. After the timeout elapses, the client escalates to a hard stop
	// by cancelling the context of all running jobs. This applies regardless of
	// how stop is initiated — whether by calling Stop, StopAndCancel, or by
	// cancelling the context passed to Start.
	//
	// In combination with signal.NotifyContext on the context passed to Start,
	// this can simplify graceful stop to:
	//
	//	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	//	defer stop()
	//
	//	if err := client.Start(ctx); err != nil { ... }
	//	<-client.Stopped()
	//
	// The signal cancels the Start context, which initiates a soft stop. If
	// running jobs haven't finished after SoftStopTimeout, their contexts are
	// automatically cancelled to trigger a hard stop.
	//
	// StopAndCancel bypasses the timeout entirely and cancels job contexts
	// immediately.
	//
	// Defaults to no timeout (wait indefinitely for jobs to finish).
	SoftStopTimeout time.Duration

	// Sequences configures opt-in ordered inserts and independent reconciliation.
	// Closing acceptance does not stop already committed sequences from draining.
	Sequences SequenceConfig

	// SkipJobKindValidation causes the job kind format validation check to be
	// skipped. This is available as an interim stopgap for users that have
	// invalid job kind names, but would rather disable the check rather than
	// fix them immediately.
	//
	// Deprecated: This option will be removed in a future versions so that job
	// kinds will always have to have a valid format.
	SkipJobKindValidation bool

	// SkipUnknownJobCheck is a flag to control whether the client should skip
	// checking to see if a registered worker exists in the client's worker bundle
	// for a job arg prior to insertion.
	//
	// This can be set to true to allow a client to insert jobs which are
	// intended to be worked by a different client which effectively makes
	// the client's insertion behavior mimic that of an insert-only client.
	//
	// Defaults to false.
	SkipUnknownJobCheck bool

	// Test holds configuration specific to test environments.
	Test TestConfig

	// TestOnly can be set to true to disable certain features that are useful
	// in production, but which may be harmful to tests, in ways like having the
	// effect of making them slower. It should not be used outside of test
	// suites.
	//
	// For example, queue maintenance services normally stagger their startup
	// with a random jittered sleep so they don't all try to work at the same
	// time. This is nice in production, but makes starting and stopping the
	// client in a test case slower.
	TestOnly bool

	// Workers is a bundle of registered job workers.
	//
	// This field may be omitted for a program that's only enqueueing jobs
	// rather than working them, but if it is configured the client can validate
	// ahead of time that a worker is properly registered for an inserted job.
	// (i.e.  That it wasn't forgotten by accident.)
	Workers *Workers

	// WorkerMiddleware are optional functions that can be called around
	// all job executions.
	//
	// Deprecated: Prefer the use of Plugins instead (which may contain
	// instances of rivertype.WorkerMiddleware).
	WorkerMiddleware []rivertype.WorkerMiddleware

	// queuePollInterval is the amount of time between periodic checks for queue
	// setting changes. This is only used in poll-only mode (when no notifier is
	// provided).
	//
	// This is internal for the time being as it hasn't had any major demand to
	// be exposed, but it's needed to make sure that our poll-only tests can
	// finish in a timely manner.
	queuePollInterval time.Duration

	// Scheduler run interval. Shared between the scheduler and producer/job
	// executors, but not currently exposed for configuration.
	schedulerInterval time.Duration
}

// ReindexerIndexNamesDefault returns the default set of indexes reindexed by River.
func ReindexerIndexNamesDefault() []string {
	indexNames := make([]string, len(reindexerIndexNamesDefault))
	copy(indexNames, reindexerIndexNamesDefault)

	return indexNames
}

// WithDefaults returns a copy of the Config with all default values applied.
func (c *Config) WithDefaults() *Config {
	if c == nil {
		c = &Config{}
	}

	reindexerIndexNames := ReindexerIndexNamesDefault()
	if c.ReindexerIndexNames != nil {
		reindexerIndexNames = make([]string, len(c.ReindexerIndexNames))
		copy(reindexerIndexNames, c.ReindexerIndexNames)
	}

	// Use the existing logger if set, otherwise create a default one.
	logger := c.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		}))
	}

	// Compute the default rescue value. For convenience, if JobTimeout is specified
	// but RescueStuckJobsAfter is not set (or less than 1) and JobTimeout is large, use
	// JobTimeout + maintenance.JobRescuerRescueAfterDefault as the default.
	rescueAfter := maintenance.JobRescuerRescueAfterDefault
	if c.JobTimeout > 0 && c.RescueStuckJobsAfter < 1 && c.JobTimeout > c.RescueStuckJobsAfter {
		rescueAfter = c.JobTimeout + maintenance.JobRescuerRescueAfterDefault
	}

	// Set default retry policy if none is provided.
	retryPolicy := c.RetryPolicy
	if retryPolicy == nil {
		retryPolicy = &DefaultClientRetryPolicy{}
	}

	return &Config{
		AdvisoryLockPrefix:          c.AdvisoryLockPrefix,
		CancelledJobRetentionPeriod: cmp.Or(c.CancelledJobRetentionPeriod, riversharedmaintenance.CancelledJobRetentionPeriodDefault),
		CompletedJobRetentionPeriod: cmp.Or(c.CompletedJobRetentionPeriod, riversharedmaintenance.CompletedJobRetentionPeriodDefault),
		DiscardedJobRetentionPeriod: cmp.Or(c.DiscardedJobRetentionPeriod, riversharedmaintenance.DiscardedJobRetentionPeriodDefault),
		ErrorHandler:                c.ErrorHandler,
		FetchCooldown:               cmp.Or(c.FetchCooldown, FetchCooldownDefault),
		FetchOnlyKnownKinds:         c.FetchOnlyKnownKinds,
		FetchPollInterval:           cmp.Or(c.FetchPollInterval, FetchPollIntervalDefault),
		ID:                          valutil.ValOrDefaultFunc(c.ID, func() string { return defaultClientID(time.Now().UTC()) }),
		Hooks:                       c.Hooks,
		JobInsertMiddleware:         c.JobInsertMiddleware,
		JobLease:                    c.JobLease.WithDefaults(),
		JobStuckHandler:             c.JobStuckHandler,
		JobStuckThreshold:           cmp.Or(c.JobStuckThreshold, JobStuckThresholdDefault),
		JobTimeout:                  cmp.Or(c.JobTimeout, JobTimeoutDefault),
		LeaderElectionDisabled:      c.LeaderElectionDisabled,
		Logger:                      logger,
		MaxAttempts:                 cmp.Or(c.MaxAttempts, MaxAttemptsDefault),
		Middleware:                  c.Middleware,
		PeriodicJobs:                c.PeriodicJobs,
		Plugins:                     c.Plugins,
		PollOnly:                    c.PollOnly,
		Queues:                      c.Queues,
		ReindexerIndexNames:         reindexerIndexNames,
		ReindexerSchedule:           c.ReindexerSchedule,
		ReindexerTimeout:            cmp.Or(c.ReindexerTimeout, maintenance.ReindexerTimeoutDefault),
		RescueStuckJobsAfter:        cmp.Or(c.RescueStuckJobsAfter, rescueAfter),
		RetryPolicy:                 retryPolicy,
		Schema:                      c.Schema,
		Sequences:                   c.Sequences.withDefaults(),
		SoftStopTimeout:             c.SoftStopTimeout,
		SkipJobKindValidation:       c.SkipJobKindValidation,
		SkipUnknownJobCheck:         c.SkipUnknownJobCheck,
		Test:                        c.Test,
		TestOnly:                    c.TestOnly,
		WorkerMiddleware:            c.WorkerMiddleware,
		Workers:                     c.Workers,
		queuePollInterval:           c.queuePollInterval,
		schedulerInterval:           cmp.Or(c.schedulerInterval, maintenance.JobSchedulerIntervalDefault),
	}
}

func (c *Config) validate() error {
	if err := c.JobLease.Validate(); err != nil {
		return err
	}
	if err := c.Sequences.validate(); err != nil {
		return err
	}
	if c.CancelledJobRetentionPeriod < -1 {
		return errors.New("CancelledJobRetentionPeriod time cannot be less than zero, except for -1 (infinite)")
	}
	if c.CompletedJobRetentionPeriod < -1 {
		return errors.New("CompletedJobRetentionPeriod cannot be less than zero, except for -1 (infinite)")
	}
	if c.DiscardedJobRetentionPeriod < -1 {
		return errors.New("DiscardedJobRetentionPeriod cannot be less than zero, except for -1 (infinite)")
	}
	if c.FetchCooldown < FetchCooldownMin {
		return fmt.Errorf("FetchCooldown must be at least %s", FetchCooldownMin)
	}
	if c.FetchPollInterval < FetchPollIntervalMin {
		return fmt.Errorf("FetchPollInterval must be at least %s", FetchPollIntervalMin)
	}
	if c.FetchPollInterval < c.FetchCooldown {
		return fmt.Errorf("FetchPollInterval cannot be shorter than FetchCooldown (%s)", c.FetchCooldown)
	}
	if len(c.ID) > 100 {
		return errors.New("ID cannot be longer than 100 characters")
	}
	if c.JobTimeout < -1 {
		return errors.New("JobTimeout cannot be negative, except for -1 (infinite)")
	}
	if c.JobStuckThreshold < 0 {
		return errors.New("JobStuckThreshold cannot be less than zero")
	}
	if c.LeaderElectionDisabled && len(c.PeriodicJobs) > 0 {
		return errors.New("PeriodicJobs must be empty when LeaderElectionDisabled is true")
	}
	if c.MaxAttempts < 0 {
		return errors.New("MaxAttempts cannot be less than zero")
	}
	if len(c.Middleware) > 0 && (len(c.JobInsertMiddleware) > 0 || len(c.WorkerMiddleware) > 0) {
		return errors.New("only one of the pair JobInsertMiddleware/WorkerMiddleware or Middleware may be provided (Middleware is recommended, and may contain both job insert and worker middleware)")
	}
	if c.ReindexerTimeout < -1 {
		return errors.New("ReindexerTimeout cannot be negative, except for -1 (infinite)")
	}
	if c.RescueStuckJobsAfter < 0 {
		return errors.New("RescueStuckJobsAfter cannot be less than zero")
	}
	if c.JobLease == nil && c.RescueStuckJobsAfter < c.JobTimeout {
		return errors.New("RescueStuckJobsAfter cannot be less than JobTimeout")
	}

	// Max Postgres notification topic length is 63 and we prefix schema to
	// notification topic, so whatever schema the user specifies must fit inside
	// this convention.
	maxSchemaLength := 63 - 1 - len(string(notifier.NotificationTopicLongest)) // -1 for the dot in `<schema>.<topic>`
	if len(c.Schema) > maxSchemaLength {
		return fmt.Errorf("Schema length must be less than or equal to %d characters", maxSchemaLength)
	}
	if c.Schema != "" && !postgresSchemaNameRE.MatchString(c.Schema) {
		return errors.New("Schema name can only contain letters, numbers, and underscores, and must start with a letter or underscore")
	}

	for queue, queueConfig := range c.Queues {
		if err := queueConfig.validate(queue, c.FetchCooldown, c.FetchPollInterval); err != nil {
			return err
		}
	}

	if c.Workers == nil && c.Queues != nil {
		return errors.New("Workers must be set if Queues is set")
	}

	if c.Workers != nil {
		for _, workerInfo := range c.Workers.workersMap {
			kind := workerInfo.jobArgs.Kind()
			if !rivercommon.UserSpecifiedIDOrKindRE.MatchString(kind) {
				if c.SkipJobKindValidation {
					c.Logger.Warn("job kind should match regex; this will be an error in future versions",
						slog.String("kind", kind),
						slog.String("regex", rivercommon.UserSpecifiedIDOrKindRE.String()),
					)
				} else {
					return fmt.Errorf("job kind %q should match regex %s", kind, rivercommon.UserSpecifiedIDOrKindRE.String())
				}
			}
		}
	}

	return nil
}

// Indicates whether with the given configuration, this client will be expected
// to execute jobs (rather than just being used to enqueue them). Executing jobs
// requires a set of configured queues.
func (c *Config) willExecuteJobs() bool {
	return len(c.Queues) > 0
}

// QueueConfig contains queue-specific configuration.
type QueueConfig struct {
	// FetchCooldown is the minimum amount of time to wait between fetches of new
	// jobs. Jobs will only be fetched *at most* this often, but if no new jobs
	// are coming in via LISTEN/NOTIFY then fetches may be delayed as long as
	// FetchPollInterval.
	//
	// Throughput is limited by this value.
	//
	// If non-zero, this overrides the FetchCooldown setting in the Client's
	// Config.
	FetchCooldown time.Duration

	// FetchPollInterval is the amount of time between periodic fetches for new
	// jobs. Typically new jobs will be picked up ~immediately after insert via
	// LISTEN/NOTIFY, but this provides a fallback.
	//
	// If non-zero, this overrides the FetchCooldown setting in the Client's
	// Config.
	FetchPollInterval time.Duration

	// MaxWorkers is the maximum number of workers to run for the queue, or put
	// otherwise, the maximum parallelism to run.
	//
	// This is the maximum number of workers within this particular client
	// instance, but note that it doesn't control the total number of workers
	// across parallel processes. Installations will want to calculate their
	// total number by multiplying this number by the number of parallel nodes
	// running River clients configured to the same database and queue.
	//
	// Requires a minimum of 1, and a maximum of 10,000.
	MaxWorkers int
}

func (c QueueConfig) validate(queueName string, clientFetchCooldown time.Duration, clientFetchPollInterval time.Duration) error {
	if c.FetchCooldown < 0 {
		return errors.New("FetchCooldown cannot be less than zero")
	}
	if c.FetchPollInterval < 0 {
		return errors.New("FetchPollInterval cannot be less than zero")
	}

	resolvedFetchCooldown := cmp.Or(c.FetchCooldown, clientFetchCooldown)
	resolvedFetchPollInterval := cmp.Or(c.FetchPollInterval, clientFetchPollInterval)
	if resolvedFetchPollInterval < resolvedFetchCooldown {
		return errors.New("FetchPollInterval cannot be less than FetchCooldown")
	}

	if c.MaxWorkers < 1 || c.MaxWorkers > QueueNumWorkersMax {
		return fmt.Errorf("invalid number of workers for queue %q: %d", queueName, c.MaxWorkers)
	}
	if err := validateQueueName(queueName); err != nil {
		return err
	}

	return nil
}

// Client is a single isolated instance of River. Your application may use
// multiple instances operating on different databases or Postgres schemas
// within a single database.
//
// Methods ending in Tx reuse the caller's transaction. The caller remains
// responsible for committing or rolling it back, and should roll back on error
// because an operation may have made partial changes. If the application needs
// to recover from an error and continue the transaction, it should create its
// own savepoint before calling River.
//
// As a temporary compatibility fallback, set RIVER_USE_LEGACY_SUBTRANSACTIONS=1
// (or true) before starting the application to restore savepoints in internal
// transaction helpers, including those used by River Pro. This applies only to
// calls using those helpers, not every transactional API. The fallback is
// planned for removal in a future release; prefer explicit application-owned
// savepoints.
type Client[TTx any] struct {
	// BaseService and BaseStartStop can't be embedded like on other services
	// because their properties would leak to the external API.
	baseService   baseservice.BaseService
	baseStartStop startstop.BaseStartStop

	clientNotifyBundle    *ClientNotifyBundle[TTx]
	completer             jobcompleter.JobCompleter
	config                *Config
	driver                riverdriver.Driver[TTx]
	elector               *leadership.Elector
	pluginLookupByJob     *pluginlookup.JobPluginLookup
	pluginLookupGlobal    *pluginlookup.PluginLookup
	insertNotifyLimiter   *notifylimiter.Limiter
	jobLeaseManager       *joblease.Manager
	notifier              *notifier.Notifier // may be nil in poll-only mode
	periodicJobs          *PeriodicJobBundle
	pilot                 riverpilot.Pilot
	producersByQueueName  map[string]*producer
	producersMu           sync.RWMutex
	queueMaintainer       *maintenance.QueueMaintainer
	queueMaintainerLeader *maintenance.QueueMaintainerLeader
	queues                *QueueBundle
	sequenceService       *sequenceruntime.Service
	services              []startstop.Service
	stopped               <-chan struct{}
	stuckJobCount         atomic.Int32
	subscriptionManager   *subscriptionManager
	testSignals           clientTestSignals

	// workCancel cancels the context used for all work goroutines. Normal Stop
	// does not cancel that context.
	workCancel context.CancelCauseFunc
}

// Test-only signals.
type clientTestSignals struct {
	jobCleaner            *maintenance.JobCleanerTestSignals
	jobRescuer            *maintenance.JobRescuerTestSignals
	jobScheduler          *maintenance.JobSchedulerTestSignals
	periodicJobEnqueuer   *maintenance.PeriodicJobEnqueuerTestSignals
	queueCleaner          *maintenance.QueueCleanerTestSignals
	queueMaintainerLeader *maintenance.QueueMaintainerLeaderTestSignals
	reindexer             *maintenance.ReindexerTestSignals
}

func (ts *clientTestSignals) Init(tb testutil.TestingTB) {
	if ts.jobCleaner != nil {
		ts.jobCleaner.Init(tb)
	}
	if ts.jobRescuer != nil {
		ts.jobRescuer.Init(tb)
	}
	if ts.jobScheduler != nil {
		ts.jobScheduler.Init(tb)
	}
	if ts.periodicJobEnqueuer != nil {
		ts.periodicJobEnqueuer.Init(tb)
	}
	if ts.queueCleaner != nil {
		ts.queueCleaner.Init(tb)
	}
	if ts.queueMaintainerLeader != nil {
		ts.queueMaintainerLeader.Init(tb)
	}
	if ts.reindexer != nil {
		ts.reindexer.Init(tb)
	}
}

var (
	// ErrNotFound is returned when a query by ID does not match any existing
	// rows. For example, attempting to cancel a job that doesn't exist will
	// return this error.
	ErrNotFound = rivertype.ErrNotFound

	errMissingConfig                 = errors.New("missing config")
	errMissingDatabasePoolWithQueues = errors.New("must have a non-nil database pool to execute jobs (either use a driver with database pool or don't configure Queues)")
	errMissingDriver                 = errors.New("missing database driver (try wrapping a Pgx pool with river/riverdriver/riverpgxv5.New)")
)

// NewClient creates a new Client with the given database driver and
// configuration.
//
// Currently only one driver is supported, which is Pgx v5. See package
// riverpgxv5.
//
// The function takes a generic parameter TTx representing a transaction type,
// but it can be omitted because it'll generally always be inferred from the
// driver. For example:
//
//	import "github.com/riverqueue/river"
//	import "github.com/riverqueue/river/riverdriver/riverpgxv5"
//
//	...
//
//	dbPool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
//	if err != nil {
//		// handle error
//	}
//	defer dbPool.Close()
//
//	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
//		...
//	})
//	if err != nil {
//		// handle error
//	}
func NewClient[TTx any](driver riverdriver.Driver[TTx], config *Config) (*Client[TTx], error) {
	if driver == nil {
		return nil, errMissingDriver
	}
	if config == nil {
		return nil, errMissingConfig
	}

	config = config.WithDefaults()

	if err := config.validate(); err != nil {
		return nil, err
	}

	archetype := baseservice.NewArchetype(config.Logger)
	if config.Test.Time != nil {
		if withStub, ok := config.Test.Time.(baseservice.TimeGeneratorWithStub); ok {
			archetype.Time = withStub
		} else {
			archetype.Time = &baseservice.TimeGeneratorWithStubWrapper{TimeGenerator: config.Test.Time}
		}
	}
	if _, ok := config.RetryPolicy.(*DefaultClientRetryPolicy); ok {
		config.RetryPolicy = retrypolicy.NewDefault(archetype.Time)
	}

	var (
		middleware = pluginconfig.CombinedMiddleware(config.Middleware, config.JobInsertMiddleware, config.WorkerMiddleware)
		plugins    = append(riverplugin.DefaultPlugins(), config.Plugins...)
	)
	pluginLookupByJob := pluginlookup.NewJobPluginLookup(archetype)
	pluginLookupGlobal := pluginlookup.NewPluginLookupFromConfig(archetype, config.Hooks, middleware, plugins)

	client := &Client[TTx]{
		clientNotifyBundle: &ClientNotifyBundle[TTx]{
			config: config,
			driver: driver,
		},
		config:               config,
		driver:               driver,
		pluginLookupByJob:    pluginLookupByJob,
		pluginLookupGlobal:   pluginLookupGlobal,
		producersByQueueName: make(map[string]*producer),
		testSignals:          clientTestSignals{},
		workCancel:           func(cause error) {}, // replaced on start, but here in case StopAndCancel is called before start up
	}

	client.queues = &QueueBundle{
		clientFetchCooldown:     config.FetchCooldown,
		clientFetchPollInterval: config.FetchPollInterval,
		clientWillExecuteJobs:   config.willExecuteJobs(),
		producerAdd:             client.producerAdd,
		producerRemove:          client.producerRemove,
	}

	baseservice.Init(archetype, &client.baseService)
	client.baseService.Name = "Client" // Have to correct the name because base service isn't embedded like it usually is
	client.insertNotifyLimiter = notifylimiter.NewLimiter(archetype, config.FetchCooldown)

	pluginDriver, _ := driver.(driverPlugin[TTx])
	if pluginDriver != nil {
		pluginDriver.PluginInit(archetype)
		client.pilot = pluginDriver.PluginPilot()
	}

	var workerMetadata []*rivertype.WorkerMetadata
	if config.Workers != nil {
		workerMetadata = make([]*rivertype.WorkerMetadata, 0, len(config.Workers.workersMap))
		for kind, workerInfo := range config.Workers.workersMap {
			workerMetadata = append(workerMetadata, &rivertype.WorkerMetadata{
				JobArgHooks: pluginLookupByJob.ByJobArgs(workerInfo.jobArgs).Hooks(),
				Kind:        kind,
			})
		}
	}

	if client.pilot == nil {
		client.pilot = &riverpilot.StandardPilot{}
	}
	client.pilot.PilotInit(archetype, (&riverpilot.PilotInitParams{
		Insert:                 client.insertMany,
		NotifyNonTxJobInsert:   client.notifyProducerWithoutListenerJobFetch,
		ProducerReportInterval: producerReportIntervalDefault,
		WorkerMetadata:         workerMetadata,
	}).Validate())
	pluginPilot, _ := client.pilot.(pilotPlugin)

	if withBaseService, ok := config.RetryPolicy.(baseservice.WithBaseService); ok {
		baseservice.Init(archetype, withBaseService)
	}

	// There are a number of internal components that are only needed/desired if
	// we're actually going to be working jobs (as opposed to just enqueueing
	// them):
	if config.willExecuteJobs() {
		if !driver.PoolIsSet() {
			return nil, errMissingDatabasePoolWithQueues
		}

		// Sequence coordination is independent of leader election and completion.
		// Its operational failures are contained inside its own service loop.
		if _, ok := driver.GetExecutor().(riverdriver.SequenceExecutor); ok && !config.Sequences.DisableReconciler {
			client.sequenceService = sequenceruntime.New(archetype, &sequenceruntime.Config{
				BatchSize:          config.Sequences.BatchSize,
				PollInterval:       config.Sequences.PollInterval,
				Schema:             config.Schema,
				TransactionTimeout: config.Sequences.TransactionTimeout,
			}, driver.GetExecutor())
			client.services = append(client.services, client.sequenceService)
		}

		client.completer = jobcompleter.NewBatchCompleter(archetype, config.Schema, driver.GetExecutor(), client.pilot, nil)
		if config.JobLease != nil {
			leasePilot, ok := client.pilot.(riverpilot.PilotJobLease)
			if !ok {
				return nil, errors.New("JobLease requires a Pilot that supports job leases")
			}
			client.jobLeaseManager = joblease.NewManager(archetype, config.JobLease, config.Schema, func(ctx context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				return leasePilot.JobRenewLeaseMany(ctx, driver.GetExecutor(), params)
			})
			client.services = append(client.services, client.jobLeaseManager)
		}
		client.subscriptionManager = newSubscriptionManager(archetype, nil)
		client.services = append(client.services, client.completer, client.subscriptionManager)

		if driver.SupportsListener() {
			// In poll only mode, we don't try to initialize a notifier that
			// uses listen/notify. Instead, each service polls for changes it's
			// interested in. e.g. Elector polls to see if leader has expired.
			if !config.PollOnly {
				client.notifier = notifier.New(archetype, driver.GetListener(&riverdriver.GetListenenerParams{Schema: config.Schema}))
				client.services = append(client.services, client.notifier)
			}
		} else {
			config.Logger.Info("Driver does not support listener; entering poll only mode")
		}

		if !config.LeaderElectionDisabled {
			client.elector = leadership.NewElector(archetype, driver.GetExecutor(), client.notifier, &leadership.Config{
				ClientID: config.ID,
				Schema:   config.Schema,
			})
			client.services = append(client.services, client.elector)
		}

		for queue, queueConfig := range config.Queues {
			if _, err := client.producerAdd(queue, queueConfig); err != nil {
				return nil, err
			}
		}

		client.services = append(client.services,
			startstop.StartStopFunc(client.logStatsLoop))

		if pluginPilot != nil {
			client.services = append(client.services, pluginPilot.PluginServices()...)
		}
	}

	if config.willExecuteJobs() && !config.LeaderElectionDisabled {
		//
		// Maintenance services
		//

		maintenanceServices := []startstop.Service{}

		{
			jobCleaner := maintenance.NewJobCleaner(archetype, &maintenance.JobCleanerConfig{
				CancelledJobRetentionPeriod: config.CancelledJobRetentionPeriod,
				CompletedJobRetentionPeriod: config.CompletedJobRetentionPeriod,
				DiscardedJobRetentionPeriod: config.DiscardedJobRetentionPeriod,
				Pilot:                       client.pilot,
				QueuesExcluded:              client.pilot.JobCleanerQueuesExcluded(),
				Schema:                      config.Schema,
				Timeout:                     config.JobCleanerTimeout,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, jobCleaner)
			client.testSignals.jobCleaner = &jobCleaner.TestSignals
		}

		{
			var leaseGracePeriod, rescueInterval time.Duration
			if config.JobLease != nil {
				leaseGracePeriod = config.JobLease.RescueGracePeriod
				rescueInterval = config.JobLease.RescueScanInterval
			}
			jobRescuer := maintenance.NewRescuer(archetype, &maintenance.JobRescuerConfig{
				ClientJobTimeout:  config.JobTimeout,
				ClientRetryPolicy: config.RetryPolicy,
				Interval:          rescueInterval,
				LeaseGracePeriod:  leaseGracePeriod,
				Pilot:             client.pilot,
				RescueAfter:       config.RescueStuckJobsAfter,
				Schema:            config.Schema,
				WorkUnitFactoryFunc: func(kind string) workunit.WorkUnitFactory {
					if workerInfo, ok := config.Workers.workersMap[kind]; ok {
						return workerInfo.workUnitFactory
					}
					return nil
				},
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, jobRescuer)
			client.testSignals.jobRescuer = &jobRescuer.TestSignals
		}

		{
			jobScheduler := maintenance.NewJobScheduler(archetype, &maintenance.JobSchedulerConfig{
				Interval:     config.schedulerInterval,
				NotifyInsert: client.maybeNotifyInsertForQueues,
				Schema:       config.Schema,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, jobScheduler)
			client.testSignals.jobScheduler = &jobScheduler.TestSignals
		}

		{
			periodicJobEnqueuer, err := maintenance.NewPeriodicJobEnqueuer(archetype, &maintenance.PeriodicJobEnqueuerConfig{
				AdvisoryLockPrefix: config.AdvisoryLockPrefix,
				PluginLookupGlobal: client.pluginLookupGlobal,
				Insert:             client.insertMany,
				IsolateJob: func(params *rivertype.JobInsertParams) bool {
					_, ordered := params.Args.(JobArgsWithSequenceOpts)
					return ordered
				},
				IsolatedInsertTimeout: config.Sequences.TransactionTimeout,
				Pilot:                 client.pilot,
				Schema:                config.Schema,
			}, driver.GetExecutor())
			if err != nil {
				return nil, err
			}
			maintenanceServices = append(maintenanceServices, periodicJobEnqueuer)
			client.testSignals.periodicJobEnqueuer = &periodicJobEnqueuer.TestSignals

			client.periodicJobs = newPeriodicJobBundle(client.config, periodicJobEnqueuer)
			client.periodicJobs.AddMany(config.PeriodicJobs)
		}

		{
			queueCleaner := maintenance.NewQueueCleaner(archetype, &maintenance.QueueCleanerConfig{
				RetentionPeriod: maintenance.QueueRetentionPeriodDefault,
				Schema:          config.Schema,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, queueCleaner)
			client.testSignals.queueCleaner = &queueCleaner.TestSignals
		}

		if driver.DatabaseName() == riverdriver.DatabaseNameSQLite {
			sqliteNotificationCleaner := maintenance.NewSQLiteNotificationCleaner(archetype, &maintenance.SQLiteNotificationCleanerConfig{
				Schema: config.Schema,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, sqliteNotificationCleaner)
		}

		{
			var scheduleFunc func(time.Time) time.Time
			if config.ReindexerSchedule != nil {
				scheduleFunc = config.ReindexerSchedule.Next
			}

			reindexer := maintenance.NewReindexer(archetype, &maintenance.ReindexerConfig{
				IndexNames:   config.ReindexerIndexNames,
				ScheduleFunc: scheduleFunc,
				Schema:       config.Schema,
				Timeout:      config.ReindexerTimeout,
			}, driver.GetExecutor())
			maintenanceServices = append(maintenanceServices, reindexer)
			client.testSignals.reindexer = &reindexer.TestSignals
		}

		if pluginPilot != nil {
			maintenanceServices = append(maintenanceServices, pluginPilot.PluginMaintenanceServices()...)
		}

		// Not added to the main services list because the queue maintainer is
		// started conditionally based on whether the client is the leader.
		client.queueMaintainer = maintenance.NewQueueMaintainer(archetype, maintenanceServices)

		if config.TestOnly {
			client.queueMaintainer.StaggerStartupDisable(true)
		}

		client.queueMaintainerLeader = maintenance.NewQueueMaintainerLeader(archetype, &maintenance.QueueMaintainerLeaderConfig{
			ClientID:        config.ID,
			Elector:         client.elector,
			QueueMaintainer: client.queueMaintainer,
		})
		client.services = append(client.services, client.queueMaintainerLeader)
		client.testSignals.queueMaintainerLeader = &client.queueMaintainerLeader.TestSignals
	}

	return client, nil
}

// Start starts the client's job fetching and working loops. Once this is called,
// the client will run in a background goroutine until stopped. All jobs are
// run with a context inheriting from the provided context, but with a timeout
// deadline applied based on the job's settings.
//
// A graceful shutdown stops fetching new jobs but allows any previously fetched
// jobs to complete. This can be initiated with the Stop method.
//
// A more abrupt shutdown can be achieved by either cancelling the provided
// context or by calling StopAndCancel. This will not only stop fetching new
// jobs, but will also cancel the context for any currently-running jobs. If
// using StopAndCancel, there's no need to also call Stop.
func (c *Client[TTx]) Start(ctx context.Context) error {
	fetchCtx, shouldStart, started, stopped := c.baseStartStop.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	c.queues.startStopMu.Lock()
	defer c.queues.startStopMu.Unlock()

	// BaseStartStop will set its stopped channel to nil after it stops, so make
	// sure to take a channel reference before finishing stopped.
	c.stopped = c.baseStartStop.StoppedUnsafe()

	producersAsServices := func() []startstop.Service {
		return sliceutil.Map(
			maputil.Values(c.producersByQueueName),
			func(p *producer) startstop.Service { return p },
		)
	}

	// Startup code. Wrapped in a closure so it doesn't have to remember to
	// close the stopped channel if returning with an error.
	if err := func() error {
		if !c.config.willExecuteJobs() {
			return errors.New("client Queues and Workers must be configured for a client to start working")
		}
		if c.config.Workers != nil && len(c.config.Workers.workersMap) < 1 {
			return errors.New("at least one Worker must be added to the Workers bundle")
		}

		// Before doing anything else, make an initial connection to the database to
		// verify that it appears healthy. Many of the subcomponents below start up
		// in a goroutine and in case of initial failure, only produce a log line,
		// so even in the case of a fundamental failure like the database not being
		// available, the client appears to have started even though it's completely
		// non-functional. Here we try to make an initial assessment of health and
		// return quickly in case of an apparent problem.
		executor := c.driver.GetExecutor()
		if err := executor.Ping(fetchCtx); err != nil {
			return fmt.Errorf("error making initial connection to database: %w", err)
		}
		if err := executor.InitDriver(fetchCtx); err != nil {
			return fmt.Errorf("error initializing driver: %w", err)
		}

		// Database capabilities are only known after initialization. A notifier
		// created by NewClient must be removed before any services start if the
		// server can't deliver notifications (for example, older Yugabyte).
		if c.notifier != nil && !c.driver.SupportsListener() {
			c.config.Logger.InfoContext(fetchCtx, "Database does not support listener; entering poll only mode")
			c.services = slices.DeleteFunc(c.services, func(service startstop.Service) bool {
				return service == c.notifier
			})
			c.notifier = nil
			if c.elector != nil {
				c.elector.SetNotifier(nil)
			}
			for _, producer := range c.producersByQueueName {
				producer.config.Notifier = nil
			}
		}

		// Each time we start, we need a fresh completer subscribe channel to
		// send job completion events on, because the completer will close it
		// each time it shuts down.
		completerSubscribeCh := make(chan []jobcompleter.CompleterJobUpdated, 10)
		c.completer.ResetSubscribeChan(completerSubscribeCh)
		c.subscriptionManager.ResetSubscribeChan(completerSubscribeCh)

		// In case of error, stop any services that might have started. This
		// is safe because even services that were never started will still
		// tolerate being stopped.
		stopServicesOnError := func() {
			startstop.StopAllParallel(c.services...)
		}

		// The completer is part of the services list below, but although it can
		// stop gracefully along with all the other services, it needs to be
		// started with a context that's _not_ cancelled if the user-provided
		// context is cancelled.  This ensures that even when fetch is cancelled on
		// shutdown, the completer is still given a separate opportunity to start
		// stopping only after the producers have finished up and returned.
		if err := c.completer.Start(context.WithoutCancel(ctx)); err != nil {
			stopServicesOnError()
			return err
		}
		// Renewal must outlive fetch cancellation and queued completion writes.
		if c.jobLeaseManager != nil {
			if err := c.jobLeaseManager.Start(context.WithoutCancel(ctx)); err != nil {
				stopServicesOnError()
				return err
			}
		}

		// We use separate contexts for fetching and working to allow for a
		// graceful stop. When SoftStopTimeout is configured, the work context
		// is detached from the start context so that cancelling the start
		// context initiates a soft stop (with timeout escalation) rather than
		// an immediate hard stop. When SoftStopTimeout is not configured, the
		// work context inherits from the start context to preserve the
		// existing behavior where cancelling the start context is equivalent
		// to StopAndCancel.
		workParentCtx := ctx
		if c.config.SoftStopTimeout > 0 {
			workParentCtx = context.WithoutCancel(ctx)
		}
		workCtx, workCancel := context.WithCancelCause(workParentCtx)

		// Client available to executors and to various service hooks.
		fetchCtx := withClient(fetchCtx, c)
		workCtx = withClient(workCtx, c)

		if err := startstop.StartAll(fetchCtx, c.services...); err != nil {
			workCancel(err)
			stopServicesOnError()
			return err
		}

		for _, producer := range c.producersByQueueName {
			if err := producer.StartWorkContext(fetchCtx, workCtx); err != nil {
				workCancel(err)
				startstop.StopAllParallel(producersAsServices()...)
				stopServicesOnError()
				return err
			}
		}

		c.queues.fetchCtx = fetchCtx
		c.queues.workCtx = workCtx
		c.workCancel = workCancel

		return nil
	}(); err != nil {
		defer stopped()
		if errors.Is(context.Cause(fetchCtx), startstop.ErrStop) {
			return nil
		}
		return err
	}

	// Generate producer services while c.queues.startStopMu.Lock() is still
	// held. This is used for WaitAllStarted below, but don't use it elsewhere
	// because new producers may have been added while the client is running.
	producerServices := producersAsServices()

	go func() {
		// Wait for all subservices to start up before signaling our own start.
		// This isn't strictly needed, but gives tests a way to fully confirm
		// that all goroutines for subservices are spun up before continuing.
		//
		// Stop also cancels the "started" channel, so in case of a context
		// cancellation, this statement will fall through. The client will
		// briefly start, but then immediately stop again.
		startstop.WaitAllStarted(append(
			c.services,
			producerServices..., // see comment on this variable
		)...)

		started()
		defer stopped()

		c.baseService.Logger.InfoContext(ctx, "River client started", slog.String("client_id", c.ID()))
		defer c.baseService.Logger.InfoContext(ctx, "River client stopped", slog.String("client_id", c.ID()))

		// The call to Stop cancels this context. Block here until shutdown.
		<-fetchCtx.Done()

		c.queues.startStopMu.Lock()
		defer c.queues.startStopMu.Unlock()

		// If SoftStopTimeout is configured, start a timer that will cancel
		// the work context (escalating to a hard stop) if producers don't
		// finish in time. StopAndCancel also calls workCancel, in which case
		// this timer is a harmless no-op because the context is already done.
		if c.config.SoftStopTimeout > 0 {
			softStopTimer := time.AfterFunc(c.config.SoftStopTimeout, func() {
				c.baseService.Logger.WarnContext(ctx, c.baseService.Name+": Soft stop timeout; cancelling remaining job contexts", slog.Duration("soft_stop_timeout", c.config.SoftStopTimeout))
				c.workCancel(rivercommon.ErrStop)
			})
			defer softStopTimer.Stop()
		}

		// On stop, have the producers stop fetching first of all.
		c.baseService.Logger.DebugContext(ctx, c.baseService.Name+": Stopping producers")
		startstop.StopAllParallel(producersAsServices()...)
		c.baseService.Logger.DebugContext(ctx, c.baseService.Name+": All producers stopped")

		c.workCancel(rivercommon.ErrStop)
		if c.jobLeaseManager != nil {
			c.completer.Stop()
			c.jobLeaseManager.Stop()
		}

		// Stop all mainline services where stop order isn't important.
		// This list of services contains the completer, which should always
		// stop after the producers so that any remaining work that was enqueued
		// will have a chance to have its state completed as it finishes.
		//
		// TODO: there's a risk here that the completer is stuck on a job that
		// won't complete. We probably need a timeout or way to move on in those
		// cases.
		servicesToStop := c.services

		if c.queueMaintainer != nil {
			// Will only be started if this client was leader, but can tolerate a
			// stop without having been started.
			servicesToStop = append(servicesToStop, c.queueMaintainer)
		}

		startstop.StopAllParallel(servicesToStop...)
	}()

	return nil
}

// Stop performs a graceful shutdown of the Client. It signals all producers
// to stop fetching new jobs and waits for any fetched or in-progress jobs to
// complete before exiting. If the provided context is done before shutdown has
// completed, Stop will return immediately with the context's error.
//
// If SoftStopTimeout is configured, running job contexts will be automatically
// cancelled after the timeout elapses, escalating to a hard stop. This also
// applies when stop is initiated by cancelling the context passed to Start.
//
// There's no need to call this method if a hard stop has already been initiated
// by cancelling the context passed to Start or by calling StopAndCancel.
func (c *Client[TTx]) Stop(ctx context.Context) error {
	shouldStop, stopped, finalizeStop := c.baseStartStop.StopInit()
	if !shouldStop {
		return nil
	}

	select {
	case <-ctx.Done(): // stop context cancelled
		finalizeStop(false) // not stopped; allow Stop to be called again
		return ctx.Err()
	case <-stopped:
		finalizeStop(true)
		return nil
	}
}

// StopAndCancel shuts down the client and cancels all work in progress. It is a
// more aggressive stop than Stop because the contexts for any in-progress jobs
// are cancelled. However, it still waits for jobs to complete before returning,
// even though their contexts are cancelled. If the provided context is done
// before shutdown has completed, StopAndCancel will return immediately with the
// context's error.
//
// This can also be initiated by cancelling the context passed to Start. There is
// no need to call this method if the context passed to Start is cancelled
// instead.
//
// In most cases, using Stop with SoftStopTimeout configured is preferable to
// calling StopAndCancel directly. SoftStopTimeout gives running jobs a chance
// to finish before automatically escalating to context cancellation, providing
// graceful stop semantics without requiring manual orchestration of Stop and
// StopAndCancel.
func (c *Client[TTx]) StopAndCancel(ctx context.Context) error {
	c.baseService.Logger.InfoContext(ctx, c.baseService.Name+": Hard stop started; cancelling all work")
	c.workCancel(rivercommon.ErrStop)

	shouldStop, stopped, finalizeStop := c.baseStartStop.StopInit()
	if !shouldStop {
		return nil
	}

	select {
	case <-ctx.Done(): // stop context cancelled
		finalizeStop(false) // not stopped; allow Stop to be called again
		return ctx.Err()
	case <-stopped:
		finalizeStop(true)
		return nil
	}
}

// Stopped returns a channel that will be closed when the Client has stopped.
// It can be used to wait for a graceful shutdown to complete.
//
// It is not affected by any contexts passed to Stop or StopAndCancel.
func (c *Client[TTx]) Stopped() <-chan struct{} {
	return c.stopped
}

// Subscribe subscribes to the provided kinds of events that occur within the
// client, like EventKindJobCompleted for when a job completes.
//
// Returns a channel over which to receive events along with a cancel function
// that can be used to cancel and tear down resources associated with the
// subscription. It's recommended but not necessary to invoke the cancel
// function. Resources will be freed when the client stops in case it's not.
//
// The event channel is buffered and sends on it are non-blocking. Consumers
// must process events in a timely manner or it's possible for events to be
// dropped. Any slow operations performed in a response to a receipt (e.g.
// persisting to a database) should be made asynchronous to avoid event loss.
//
// Callers must specify the kinds of events they're interested in. This allows
// for forward compatibility in case new kinds of events are added in future
// versions. If new event kinds are added, callers will have to explicitly add
// them to their requested list and ensure they can be handled correctly.
func (c *Client[TTx]) Subscribe(kinds ...EventKind) (<-chan *Event, func()) {
	return c.SubscribeConfig(&SubscribeConfig{Kinds: kinds})
}

// The default maximum size of the subscribe channel. Events that would overflow
// it will be dropped.
const subscribeChanSizeDefault = 1_000

// SubscribeConfig is more thorough subscription configuration used for
// Client.SubscribeConfig.
type SubscribeConfig struct {
	// ChanSize is the size of the buffered channel that will be created for the
	// subscription. Incoming events that would overflow this buffer because a
	// listener isn't reading from the channel in a timely manner will be dropped.
	//
	// Defaults to 1000.
	ChanSize int

	// Kinds are the kinds of events that the subscription will receive.
	// Requiring that kinds are specified explicitly allows for forward
	// compatibility in case new kinds of events are added in future versions.
	// If new event kinds are added, callers will have to explicitly add them to
	// their requested list and ensure they can be handled correctly.
	Kinds []EventKind
}

// SubscribeConfig is a special internal variant of Subscribe that lets us
// inject an overridden channel size.
func (c *Client[TTx]) SubscribeConfig(config *SubscribeConfig) (<-chan *Event, func()) {
	if c.subscriptionManager == nil {
		panic("created a subscription on a client that will never work jobs (Queues not configured)")
	}

	return c.subscriptionManager.SubscribeConfig(config)
}

// Dump aggregate stats from job completions to logs periodically.  These
// numbers don't mean much in themselves, but can give a rough idea of the
// proportions of each compared to each other, and may help flag outlying values
// indicative of a problem.
func (c *Client[TTx]) logStatsLoop(ctx context.Context, shouldStart bool, started, stopped func()) error {
	if !shouldStart {
		return nil
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				c.subscriptionManager.logStats(ctx, c.baseService.Name)
			}
		}
	}()

	return nil
}

// Driver exposes the underlying driver used by the client.
//
// API is not stable. DO NOT USE.
func (c *Client[TTx]) Driver() riverdriver.Driver[TTx] {
	return c.driver
}

// JobCancel cancels the job with the given ID. If possible, the job is
// cancelled immediately and will not be retried. The provided context is used
// for the underlying Postgres update and can be used to cancel the operation or
// apply a timeout.
//
// If the job is still in the queue (available, scheduled, or retryable), it is
// immediately marked as cancelled and will not be retried.
//
// If the job is already finalized (cancelled, completed, or discarded), no
// changes are made.
//
// If the job is currently running, it is not immediately cancelled, but is
// instead marked for cancellation. The client running the job will also be
// notified to cancel the running job's context. When running without a notifier,
// clients poll for cancellation requests every two seconds. Although the job's
// context will be cancelled, since Go does not provide a mechanism to
// interrupt a running goroutine the job will continue running until it returns.
// As always, it is important for workers to respect context cancellation and
// return promptly when the job context is done.
//
// Once the cancellation signal is received by the client running the job, any
// error returned by that job will result in it being cancelled permanently and
// not retried. However if the job returns no error, it will be completed as
// usual.
//
// In the event the running job finishes executing _before_ the cancellation
// signal is received but _after_ this update was made, the behavior depends on
// which state the job is being transitioned into (based on its return error):
//
//   - If the job completed successfully, was cancelled from within, or was
//     discarded due to exceeding its max attempts, the job will be updated as
//     usual.
//   - If the job was snoozed to run again later or encountered a retryable error,
//     the job will be marked as cancelled and will not be attempted again.
//
// Returns the up-to-date JobRow for the specified jobID if it exists. Returns
// ErrNotFound if the job doesn't exist.
func (c *Client[TTx]) JobCancel(ctx context.Context, jobID int64) (*rivertype.JobRow, error) {
	job, err := c.jobCancel(ctx, c.driver.GetExecutor(), jobID)
	if err != nil {
		return nil, err
	}

	c.notifyProducerWithoutListenerQueueControlEvent(job.Queue, &controlEventPayload{
		Action: controlActionCancel,
		JobID:  job.ID,
		Queue:  job.Queue,
	})

	return job, nil
}

// JobCancelTx cancels the job with the given ID within the specified
// transaction. This variant lets a caller cancel a job atomically alongside
// other database changes. A cancelled job doesn't take effect until the
// transaction commits, and if the transaction rolls back, so too is the
// cancelled job.
//
// If possible, the job is cancelled immediately and will not be retried. The
// provided context is used for the underlying Postgres update and can be used
// to cancel the operation or apply a timeout.
//
// If the job is still in the queue (available, scheduled, or retryable), it is
// immediately marked as cancelled and will not be retried.
//
// If the job is already finalized (cancelled, completed, or discarded), no
// changes are made.
//
// If the job is currently running, it is not immediately cancelled, but is
// instead marked for cancellation. The client running the job will also be
// notified to cancel the running job's context. When running without a notifier,
// clients poll for cancellation requests every two seconds. Although the job's
// context will be cancelled, since Go does not provide a mechanism to
// interrupt a running goroutine the job will continue running until it returns.
// As always, it is important for workers to respect context cancellation and
// return promptly when the job context is done.
//
// Once the cancellation signal is received by the client running the job, any
// error returned by that job will result in it being cancelled permanently and
// not retried. However if the job returns no error, it will be completed as
// usual.
//
// In the event the running job finishes executing _before_ the cancellation
// signal is received but _after_ this update was made, the behavior depends on
// which state the job is being transitioned into (based on its return error):
//
//   - If the job completed successfully, was cancelled from within, or was
//     discarded due to exceeding its max attempts, the job will be updated as
//     usual.
//   - If the job was snoozed to run again later or encountered a retryable error,
//     the job will be marked as cancelled and will not be attempted again.
//
// Returns the up-to-date JobRow for the specified jobID if it exists. Returns
// ErrNotFound if the job doesn't exist.
func (c *Client[TTx]) JobCancelTx(ctx context.Context, tx TTx, jobID int64) (*rivertype.JobRow, error) {
	return c.jobCancel(ctx, c.driver.UnwrapExecutor(tx), jobID)
}

func (c *Client[TTx]) jobCancel(ctx context.Context, exec riverdriver.Executor, jobID int64) (*rivertype.JobRow, error) {
	return c.pilot.JobCancel(ctx, exec, &riverdriver.JobCancelParams{
		ID:                jobID,
		CancelAttemptedAt: c.baseService.Time.Now(),
		ControlTopic:      string(notifier.NotificationTopicControl),
		Now:               c.baseService.Time.NowOrNil(),
		Schema:            c.config.Schema,
	})
}

// JobDelete deletes the job with the given ID from the database, returning the
// deleted row if it was deleted. Jobs in the running state are not deleted,
// instead returning rivertype.ErrJobRunning.
func (c *Client[TTx]) JobDelete(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	return c.jobDelete(ctx, c.driver.GetExecutor(), id)
}

// JobDeleteTx deletes the job with the given ID from the database, returning the
// deleted row if it was deleted. Jobs in the running state are not deleted,
// instead returning rivertype.ErrJobRunning. This variant lets a caller retry a
// job atomically alongside other database changes. A deleted job isn't deleted
// until the transaction commits, and if the transaction rolls back, so too is
// the deleted job.
func (c *Client[TTx]) JobDeleteTx(ctx context.Context, tx TTx, id int64) (*rivertype.JobRow, error) {
	return c.jobDelete(ctx, c.driver.UnwrapExecutor(tx), id)
}

func (c *Client[TTx]) jobDelete(ctx context.Context, exec riverdriver.Executor, id int64) (*rivertype.JobRow, error) {
	params := &riverdriver.JobDeleteParams{
		ID:     id,
		Schema: c.config.Schema,
	}

	if pilot, ok := c.pilot.(riverpilot.PilotJobDeleter); ok {
		return pilot.JobDelete(ctx, exec, params)
	}

	// Temporary compatibility fallback for Pilot implementations from before
	// PilotJobDeleter. Once Pilot embeds PilotJobDeleter, replace the assertion
	// above and this fallback with a direct call to c.pilot.JobDelete.
	return exec.JobDelete(ctx, params)
}

// JobGet fetches a single job by its ID. Returns the up-to-date JobRow for the
// specified jobID if it exists. Returns ErrNotFound if the job doesn't exist.
func (c *Client[TTx]) JobGet(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	return c.driver.GetExecutor().JobGetByID(ctx, &riverdriver.JobGetByIDParams{
		ID:     id,
		Schema: c.config.Schema,
	})
}

// JobGetTx fetches a single job by its ID, within a transaction. Returns the
// up-to-date JobRow for the specified jobID if it exists. Returns ErrNotFound
// if the job doesn't exist.
func (c *Client[TTx]) JobGetTx(ctx context.Context, tx TTx, id int64) (*rivertype.JobRow, error) {
	return c.driver.UnwrapExecutor(tx).JobGetByID(ctx, &riverdriver.JobGetByIDParams{
		ID:     id,
		Schema: c.config.Schema,
	})
}

// JobRetry updates the job with the given ID to make it immediately available
// to be retried. Jobs in the running state are not touched, while jobs in any
// other state are made available. To prevent jobs already waiting in the queue
// from being set back in line, the job's scheduled_at field is set to the
// current time only if it's not already in the past.
//
// MaxAttempts is also incremented by one if the job has already exhausted its
// max attempts. Sequence members can only be retried while they are the current
// unresolved head; retrying a successor or a resolved member returns an error.
func (c *Client[TTx]) JobRetry(ctx context.Context, id int64) (*rivertype.JobRow, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}
	job, err := c.JobGet(ctx, id)
	if err != nil {
		return nil, err
	}
	if job.SequenceKey == nil {
		return c.jobRetry(ctx, c.driver.GetExecutor(), id)
	}
	var retried *rivertype.JobRow
	err = c.sequenceWithTx(ctx, func(ctx context.Context, tx riverdriver.ExecutorTx) error {
		var err error
		retried, err = sequenceruntime.Retry(ctx, tx, &riverdriver.JobRetryParams{
			ID: id, Now: c.baseService.Time.NowOrNil(), Schema: c.config.Schema,
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return retried, nil
}

// JobRetryTx updates the job with the given ID to make it immediately available
// to be retried, within the specified transaction. This variant lets a caller
// retry a job atomically alongside other database changes. A retried job isn't
// visible to be worked until the transaction commits, and if the transaction
// rolls back, so too is the retried job.
//
// Jobs in the running state are not touched, while jobs in any other state are
// made available. To prevent jobs already waiting in the queue from being set
// back in line, the job's scheduled_at field is set to the current time only if
// it's not already in the past.
//
// MaxAttempts is also incremented by one if the job has already exhausted its
// max attempts. Sequence members can only be retried while they are the current
// unresolved head; retrying a successor or a resolved member returns an error.
func (c *Client[TTx]) JobRetryTx(ctx context.Context, tx TTx, id int64) (*rivertype.JobRow, error) {
	execTx := c.driver.UnwrapExecutor(tx)
	job, err := execTx.JobGetByID(ctx, &riverdriver.JobGetByIDParams{ID: id, Schema: c.config.Schema})
	if err != nil {
		return nil, err
	}
	if job.SequenceKey == nil {
		return c.jobRetry(ctx, execTx, id)
	}
	return sequenceruntime.Retry(ctx, execTx, &riverdriver.JobRetryParams{
		ID: id, Now: c.baseService.Time.NowOrNil(), Schema: c.config.Schema,
	})
}

func (c *Client[TTx]) jobRetry(ctx context.Context, exec riverdriver.Executor, id int64) (*rivertype.JobRow, error) {
	return c.pilot.JobRetry(ctx, exec, &riverdriver.JobRetryParams{
		ID:     id,
		Now:    c.baseService.Time.NowOrNil(),
		Schema: c.config.Schema,
	})
}

// JobUpdateParams contains parameters for Client.JobUpdate and Client.JobUpdateTx.
type JobUpdateParams struct {
	// Output is a new output value for a job.
	//
	// If not set, and a job is updated from inside a work function, the job's
	// output is set based on output recorded so far using RecordOutput.
	Output any
}

// JobUpdate updates the job with the given ID.
//
// If JobUpdateParams.Output is not set, this function may be used inside a job
// work function to set a job's output based on output recorded so far using
// RecordOutput.
func (c *Client[TTx]) JobUpdate(ctx context.Context, id int64, params *JobUpdateParams) (*rivertype.JobRow, error) {
	return c.jobUpdate(ctx, c.driver.GetExecutor(), id, params)
}

// JobUpdateTx updates the job with the given ID.
//
// If JobUpdateParams.Output is not set, this function may be used inside a job
// work function to set a job's output based on output recorded so far using
// RecordOutput.
//
// This variant updates the job inside of a transaction.
func (c *Client[TTx]) JobUpdateTx(ctx context.Context, tx TTx, id int64, params *JobUpdateParams) (*rivertype.JobRow, error) {
	return c.jobUpdate(ctx, c.driver.UnwrapExecutor(tx), id, params)
}

func (c *Client[TTx]) jobUpdate(ctx context.Context, exec riverdriver.Executor, id int64, params *JobUpdateParams) (*rivertype.JobRow, error) {
	ctx, release, err := joblease.FromContext(ctx).Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if params == nil {
		params = &JobUpdateParams{}
	}

	outputFromWorkContext := func() json.RawMessage {
		metadataUpdates, hasMetadataUpdates := jobexecutor.MetadataUpdatesFromWorkContext(ctx)
		if !hasMetadataUpdates {
			return nil
		}

		if val, ok := metadataUpdates[rivertype.MetadataKeyOutput]; ok {
			return val.(json.RawMessage) //nolint:forcetypeassert
		}

		return nil
	}()

	var (
		metadataDoMerge      bool
		metadataUpdatesBytes = []byte("{}") // even in the event of no update, still valid jsonb
	)
	if outputFromWorkContext != nil || params.Output != nil {
		metadataDoMerge = true

		var outputBytes json.RawMessage

		switch {
		// comes first because params takes precedence over context output
		case params.Output != nil:
			var err error
			outputBytes, err = json.Marshal(params.Output)
			if err != nil {
				return nil, err
			}

			if err := checkOutputSize(outputBytes); err != nil {
				return nil, err
			}

		case outputFromWorkContext != nil:
			// no size check necessary here because it's already been checked in RecordOutput
			outputBytes = outputFromWorkContext
		}

		var err error
		metadataUpdatesBytes, err = json.Marshal(map[string]json.RawMessage{
			rivertype.MetadataKeyOutput: outputBytes,
		})
		if err != nil {
			return nil, fmt.Errorf("error marshaling metadata updates to JSON: %w", err)
		}
	}

	return exec.JobUpdate(ctx, &riverdriver.JobUpdateParams{
		ID:              id,
		MetadataDoMerge: metadataDoMerge,
		Metadata:        metadataUpdatesBytes,
		Schema:          c.config.Schema,
	})
}

// ID returns the unique ID of this client as set in its config or
// auto-generated if not specified.
func (c *Client[TTx]) ID() string {
	return c.config.ID
}

func insertParamsFromConfigArgsAndOptions(archetype *baseservice.Archetype, config *Config, args JobArgs, insertOpts *InsertOpts) (*rivertype.JobInsertParams, error) {
	encodedArgs, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("error marshaling args to JSON: %w", err)
	}

	if insertOpts == nil {
		insertOpts = &InsertOpts{}
	}

	var jobInsertOpts InsertOpts
	if argsWithOpts, ok := args.(JobArgsWithInsertOpts); ok {
		jobInsertOpts = argsWithOpts.InsertOpts()
	}

	if _, ok := args.(JobArgsWithSequenceOpts); ok && (insertOpts.Pending || jobInsertOpts.Pending) {
		return nil, ErrSequencePending
	}

	// If the time is stubbed (in a test), use that for `created_at`. Otherwise,
	// leave an empty value which will either use the database's `now()` or be defaulted
	// by drivers as necessary.
	createdAt := archetype.Time.NowOrNil()

	maxAttempts := cmp.Or(insertOpts.MaxAttempts, jobInsertOpts.MaxAttempts, config.MaxAttempts)
	priority := cmp.Or(insertOpts.Priority, jobInsertOpts.Priority, rivercommon.PriorityDefault)
	queue := cmp.Or(insertOpts.Queue, jobInsertOpts.Queue, rivercommon.QueueDefault)

	if err := validateQueueName(queue); err != nil {
		return nil, err
	}

	tags := insertOpts.Tags
	if insertOpts.Tags == nil {
		tags = jobInsertOpts.Tags
	}
	if tags == nil {
		tags = []string{}
	} else {
		for _, tag := range tags {
			if len(tag) > 255 {
				return nil, errors.New("tags should be a maximum of 255 characters long")
			}
			if !tagRE.MatchString(tag) {
				return nil, errors.New("tags should match regex " + tagRE.String())
			}
		}
	}

	if priority < 1 || priority > 4 {
		return nil, errors.New("priority must be between 1 and 4")
	}

	var uniqueOpts UniqueOpts
	if !config.Test.DisableUniqueEnforcement {
		uniqueOpts = insertOpts.UniqueOpts
		if uniqueOpts.isEmpty() {
			uniqueOpts = jobInsertOpts.UniqueOpts
		}
	}
	if err := uniqueOpts.validate(); err != nil {
		return nil, err
	}

	metadata := insertOpts.Metadata
	if len(metadata) == 0 {
		metadata = []byte("{}")
	}

	insertParams := &rivertype.JobInsertParams{
		Args:        args,
		CreatedAt:   createdAt,
		EncodedArgs: encodedArgs,
		Kind:        args.Kind(),
		MaxAttempts: maxAttempts,
		Metadata:    metadata,
		Priority:    priority,
		Queue:       queue,
		State:       rivertype.JobStateAvailable,
		Tags:        tags,
	}
	switch {
	case !insertOpts.ScheduledAt.IsZero():
		insertParams.ScheduledAt = &insertOpts.ScheduledAt
		insertParams.State = rivertype.JobStateScheduled
	case !jobInsertOpts.ScheduledAt.IsZero():
		insertParams.ScheduledAt = &jobInsertOpts.ScheduledAt
		insertParams.State = rivertype.JobStateScheduled
	default:
		// Use a stubbed time if there was one, but otherwise prefer the value
		// generated by the database. createdAt is nil unless time is stubbed.
		insertParams.ScheduledAt = createdAt
	}

	if insertOpts.Pending {
		insertParams.State = rivertype.JobStatePending
	}

	// Compute the unique key only after the effective scheduled time is known
	// so that a ByPeriod key describes the period in which the job is
	// scheduled to run rather than the period in which it was inserted.
	if !uniqueOpts.isEmpty() {
		internalUniqueOpts := (*dbunique.UniqueOpts)(&uniqueOpts)
		insertParams.UniqueKey, err = dbunique.UniqueKey(archetype.Time, internalUniqueOpts, insertParams)
		if err != nil {
			return nil, err
		}
		insertParams.UniqueStates = internalUniqueOpts.StateBitmask()
	}

	return insertParams, nil
}

var errNoDriverDBPool = errors.New("driver must have non-nil database pool to use non-transactional methods like Insert and InsertMany (try InsertTx or InsertManyTx instead")

// Insert inserts a new job with the provided args. Job opts can be used to
// override any defaults that may have been provided by an implementation of
// JobArgsWithInsertOpts.InsertOpts, as well as any global defaults. The
// provided context is used for the underlying Postgres insert and can be used
// to cancel the operation or apply a timeout.
//
//	jobRow, err := client.Insert(insertCtx, MyArgs{}, nil)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) Insert(ctx context.Context, args JobArgs, opts *InsertOpts) (*rivertype.JobInsertResult, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}

	res, err := dbutil.WithTxV(ctx, c.driver.GetExecutor(), func(ctx context.Context, execTx riverdriver.ExecutorTx) ([]*rivertype.JobInsertResult, error) {
		return c.validateParamsAndInsertMany(ctx, execTx, []InsertManyParams{{Args: args, InsertOpts: opts}})
	})
	if err != nil {
		return nil, err
	}

	c.notifyProducerWithoutListenerJobFetch(ctx, res)

	return res[0], nil
}

// InsertTx inserts a new job with the provided args on the given transaction.
// Job opts can be used to override any defaults that may have been provided by
// an implementation of JobArgsWithInsertOpts.InsertOpts, as well as any global
// defaults. The provided context is used for the underlying Postgres insert and
// can be used to cancel the operation or apply a timeout.
//
//	jobRow, err := client.InsertTx(insertCtx, tx, MyArgs{}, nil)
//	if err != nil {
//		// handle error
//	}
//
// This variant lets a caller insert jobs atomically alongside other database
// changes. It's also possible to insert a job outside a transaction, but this
// usage is recommended to ensure that all data a job needs to run is available
// by the time it starts. Because of snapshot visibility guarantees across
// transactions, the job will not be worked until the transaction has committed,
// and if the transaction rolls back, so too is the inserted job.
func (c *Client[TTx]) InsertTx(ctx context.Context, tx TTx, args JobArgs, opts *InsertOpts) (*rivertype.JobInsertResult, error) {
	res, err := c.validateParamsAndInsertMany(ctx, c.driver.UnwrapExecutor(tx), []InsertManyParams{{Args: args, InsertOpts: opts}})
	if err != nil {
		return nil, err
	}
	return res[0], nil
}

// InsertManyParams encapsulates a single job combined with insert options for
// use with batch insertion.
type InsertManyParams struct {
	// Args are the arguments of the job to insert.
	Args JobArgs

	// InsertOpts are insertion options for this job.
	InsertOpts *InsertOpts
}

// InsertMany inserts many jobs at once. Each job is inserted as an
// InsertManyParams tuple, which takes job args along with an optional set of
// insert options, which override insert options provided by an
// JobArgsWithInsertOpts.InsertOpts implementation or any client-level defaults.
// The provided context is used for the underlying Postgres inserts and can be
// used to cancel the operation or apply a timeout.
//
//	count, err := client.InsertMany(ctx, []river.InsertManyParams{
//		{Args: BatchInsertArgs{}},
//		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 3}},
//	})
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) InsertMany(ctx context.Context, params []InsertManyParams) ([]*rivertype.JobInsertResult, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}

	res, err := dbutil.WithTxV(ctx, c.driver.GetExecutor(), func(ctx context.Context, execTx riverdriver.ExecutorTx) ([]*rivertype.JobInsertResult, error) {
		return c.validateParamsAndInsertMany(ctx, execTx, params)
	})
	if err != nil {
		return nil, err
	}

	c.notifyProducerWithoutListenerJobFetch(ctx, res)

	return res, nil
}

// InsertManyTx inserts many jobs at once. Each job is inserted as an
// InsertManyParams tuple, which takes job args along with an optional set of
// insert options, which override insert options provided by an
// JobArgsWithInsertOpts.InsertOpts implementation or any client-level defaults.
// The provided context is used for the underlying Postgres inserts and can be
// used to cancel the operation or apply a timeout.
//
//	count, err := client.InsertManyTx(ctx, tx, []river.InsertManyParams{
//		{Args: BatchInsertArgs{}},
//		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 3}},
//	})
//	if err != nil {
//		// handle error
//	}
//
// This variant lets a caller insert jobs atomically alongside other database
// changes. An inserted job isn't visible to be worked until the transaction
// commits, and if the transaction rolls back, so too is the inserted job.
func (c *Client[TTx]) InsertManyTx(ctx context.Context, tx TTx, params []InsertManyParams) ([]*rivertype.JobInsertResult, error) {
	res, err := c.validateParamsAndInsertMany(ctx, c.driver.UnwrapExecutor(tx), params)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// validateParamsAndInsertMany is a helper method that wraps the insertMany
// method to provide param validation and conversion prior to calling the actual
// insertMany method. This allows insertMany to be reused by the
// PeriodicJobEnqueuer which cannot reference top-level river package types.
func (c *Client[TTx]) validateParamsAndInsertMany(ctx context.Context, execTx riverdriver.ExecutorTx, params []InsertManyParams) ([]*rivertype.JobInsertResult, error) {
	insertParams, err := c.insertManyParams(params)
	if err != nil {
		return nil, err
	}

	return c.insertMany(ctx, execTx, insertParams)
}

// insertMany is a shared code path for InsertMany and InsertManyTx, also used
// by the PeriodicJobEnqueuer.
func (c *Client[TTx]) insertMany(ctx context.Context, execTx riverdriver.ExecutorTx, insertParams []*rivertype.JobInsertParams) ([]*rivertype.JobInsertResult, error) {
	return c.insertManyShared(ctx, execTx, insertParams, func(ctx context.Context, insertParams []*riverdriver.JobInsertFastParams) ([]*rivertype.JobInsertResult, error) {
		results, err := c.insertManyWithSequences(ctx, execTx, insertParams)
		if err != nil {
			return nil, err
		}

		return sliceutil.Map(results,
			func(result *riverdriver.JobInsertFastResult) *rivertype.JobInsertResult {
				return (*rivertype.JobInsertResult)(result)
			},
		), nil
	})
}

// The shared code path for all Insert and InsertMany methods. It takes a
// function that executes the actual insert operation and allows for different
// implementations of the insert query to be passed in, each mapping their
// results back to a common result type.
func (c *Client[TTx]) insertManyShared(
	ctx context.Context,
	tx riverdriver.ExecutorTx,
	insertParams []*rivertype.JobInsertParams,
	execute func(context.Context, []*riverdriver.JobInsertFastParams) ([]*rivertype.JobInsertResult, error),
) ([]*rivertype.JobInsertResult, error) {
	doInner := func(ctx context.Context) ([]*rivertype.JobInsertResult, error) {
		for _, params := range insertParams {
			for _, hook := range append(
				c.pluginLookupGlobal.ByKind(pluginlookup.PluginKindHookInsertBegin),
				c.pluginLookupByJob.ByJobArgs(params.Args).ByKind(pluginlookup.PluginKindHookInsertBegin)...,
			) {
				if err := hook.(rivertype.HookInsertBegin).InsertBegin(ctx, params); err != nil { //nolint:forcetypeassert
					return nil, err
				}
			}
		}

		finalInsertParams := sliceutil.Map(insertParams, func(params *rivertype.JobInsertParams) *riverdriver.JobInsertFastParams {
			return (*riverdriver.JobInsertFastParams)(params)
		})

		insertResults, err := execute(ctx, finalInsertParams)
		if err != nil {
			return insertResults, err
		}

		queues := make([]string, 0, 10)
		for _, params := range insertParams {
			if params.State == rivertype.JobStateAvailable {
				queues = append(queues, params.Queue)
			}
		}

		if err = c.maybeNotifyInsertForQueues(ctx, tx, queues); err != nil {
			return nil, err
		}

		return insertResults, nil
	}

	jobInsertMiddleware := append([]any(nil), c.pluginLookupGlobal.ByKind(pluginlookup.PluginKindMiddlewareJobInsert)...)
	jobKindsSeen := make(map[string]struct{}, len(insertParams))
	for _, params := range insertParams {
		kind := params.Args.Kind()
		if _, ok := jobKindsSeen[kind]; ok {
			continue
		}
		jobKindsSeen[kind] = struct{}{}

		jobInsertMiddleware = append(
			jobInsertMiddleware,
			c.pluginLookupByJob.ByJobArgs(params.Args).ByKind(pluginlookup.PluginKindMiddlewareJobInsert)...,
		)
	}
	if len(jobInsertMiddleware) > 0 {
		// Wrap middlewares in reverse order so the one defined first is wrapped
		// as the outermost function and is first to receive the operation.
		for _, v := range slices.Backward(jobInsertMiddleware) {
			middlewareItem := v.(rivertype.JobInsertMiddleware) //nolint:forcetypeassert // capture the current middleware item
			previousDoInner := doInner                          // Capture the current doInner function
			doInner = func(ctx context.Context) ([]*rivertype.JobInsertResult, error) {
				return middlewareItem.InsertMany(ctx, insertParams, previousDoInner)
			}
		}
	}

	return doInner(ctx)
}

// Validates input parameters for a batch insert operation and generates a set
// of batch insert parameters.
func (c *Client[TTx]) insertManyParams(params []InsertManyParams) ([]*rivertype.JobInsertParams, error) {
	if len(params) < 1 {
		return nil, errors.New("no jobs to insert")
	}

	insertParams := make([]*rivertype.JobInsertParams, len(params))
	for i, param := range params {
		if err := c.validateJobArgs(param.Args); err != nil {
			return nil, err
		}

		insertParamsItem, err := insertParamsFromConfigArgsAndOptions(&c.baseService.Archetype, c.config, param.Args, param.InsertOpts)
		if err != nil {
			return nil, err
		}

		insertParams[i] = insertParamsItem
	}

	return insertParams, nil
}

// Notifies an internal producer of new jobs being queued for work.  Only
// invoked if the client's driver doesn't support a listener. If a listener is
// supported, job notifications go out via listen/notify instead.
//
// Should only ever be invoked *outside* a transaction. If invoked within a
// transaction, the producer wouldn't yet be able to access the new jobs that
// triggered the notification because they're not committed yet.
func (c *Client[TTx]) notifyProducerWithoutListenerJobFetch(_ context.Context, res []*rivertype.JobInsertResult) {
	if c.driver.SupportsListener() {
		return
	}

	c.producersMu.RLock()
	defer c.producersMu.RUnlock()

	if len(c.producersByQueueName) < 1 {
		return
	}

	// Special case for when we were handling exactly one job, which is a very
	// common case. Acts as a minor optimization by avoiding the map allocation.
	if len(res) == 1 {
		if producer, ok := c.producersByQueueName[res[0].Job.Queue]; ok {
			producer.TriggerJobFetch()
		}

		return
	}

	queuesTriggered := make(map[string]struct{})

	for _, insertRes := range res {
		if _, ok := queuesTriggered[insertRes.Job.Queue]; ok {
			continue
		}
		queuesTriggered[insertRes.Job.Queue] = struct{}{}

		if producer, ok := c.producersByQueueName[insertRes.Job.Queue]; ok {
			producer.TriggerJobFetch()
		}
	}
}

// InsertManyFast inserts many jobs at once using Postgres' `COPY FROM` mechanism,
// making the operation quite fast and memory efficient. Each job is inserted as
// an InsertManyParams tuple, which takes job args along with an optional set of
// insert options, which override insert options provided by an
// JobArgsWithInsertOpts.InsertOpts implementation or any client-level defaults.
// The provided context is used for the underlying Postgres inserts and can be
// used to cancel the operation or apply a timeout.
//
//	count, err := client.InsertMany(ctx, []river.InsertManyParams{
//		{Args: BatchInsertArgs{}},
//		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 3}},
//	})
//	if err != nil {
//		// handle error
//	}
//
// Unlike with `InsertMany`, unique conflicts cannot be handled gracefully. If a
// unique constraint is violated, the operation will fail and no jobs will be inserted.
func (c *Client[TTx]) InsertManyFast(ctx context.Context, params []InsertManyParams) (int, error) {
	if !c.driver.PoolIsSet() {
		return 0, errNoDriverDBPool
	}

	// Wrap in a transaction in case we need to notify about inserts.
	res, err := dbutil.WithTxV(ctx, c.driver.GetExecutor(), func(ctx context.Context, execTx riverdriver.ExecutorTx) ([]*rivertype.JobInsertResult, error) {
		return c.insertManyFast(ctx, execTx, params)
	})
	if err != nil {
		return 0, err
	}

	c.notifyProducerWithoutListenerJobFetch(ctx, res)

	return len(res), nil
}

// InsertManyFastTx inserts many jobs at once using Postgres' `COPY FROM`
// mechanism, making the operation quite fast and memory efficient. Each job is
// inserted as an InsertManyParams tuple, which takes job args along with an
// optional set of insert options, which override insert options provided by an
// JobArgsWithInsertOpts.InsertOpts implementation or any client-level defaults.
// The provided context is used for the underlying Postgres inserts and can be
// used to cancel the operation or apply a timeout.
//
//	count, err := client.InsertManyTx(ctx, tx, []river.InsertManyParams{
//		{Args: BatchInsertArgs{}},
//		{Args: BatchInsertArgs{}, InsertOpts: &river.InsertOpts{Priority: 3}},
//	})
//	if err != nil {
//		// handle error
//	}
//
// This variant lets a caller insert jobs atomically alongside other database
// changes. An inserted job isn't visible to be worked until the transaction
// commits, and if the transaction rolls back, so too is the inserted job.
//
// Unlike with `InsertManyTx`, unique conflicts cannot be handled gracefully. If
// a unique constraint is violated, the operation will fail and no jobs will be
// inserted.
func (c *Client[TTx]) InsertManyFastTx(ctx context.Context, tx TTx, params []InsertManyParams) (int, error) {
	res, err := c.insertManyFast(ctx, c.driver.UnwrapExecutor(tx), params)
	if err != nil {
		return 0, err
	}
	return len(res), nil
}

func (c *Client[TTx]) insertManyFast(ctx context.Context, execTx riverdriver.ExecutorTx, params []InsertManyParams) ([]*rivertype.JobInsertResult, error) {
	insertParams, err := c.insertManyParams(params)
	if err != nil {
		return nil, err
	}

	return c.insertManyShared(ctx, execTx, insertParams, func(ctx context.Context, insertParams []*riverdriver.JobInsertFastParams) ([]*rivertype.JobInsertResult, error) {
		for _, params := range insertParams {
			if _, ok := params.Args.(JobArgsWithSequenceOpts); ok {
				return nil, ErrSequenceFastInsert
			}
		}

		count, err := execTx.JobInsertFastManyNoReturning(ctx, &riverdriver.JobInsertFastManyParams{
			Jobs:   insertParams,
			Schema: c.config.Schema,
		})
		if err != nil {
			return nil, err
		}
		return make([]*rivertype.JobInsertResult, count), nil
	})
}

// Notify the given queues that new jobs are available. The queues list will be
// deduplicated and each will be checked to see if it is due for an insert
// notification from this client.
func (c *Client[TTx]) maybeNotifyInsertForQueues(ctx context.Context, tx riverdriver.ExecutorTx, queues []string) error {
	if len(queues) < 1 {
		return nil
	}

	var (
		queuesDeduped   = sliceutil.Uniq(queues)
		payloads        = make([]string, 0, len(queuesDeduped))
		queuesTriggered = make([]string, 0, len(queuesDeduped))
	)

	for _, queue := range queuesDeduped {
		if c.insertNotifyLimiter.ShouldTrigger(queue) {
			payloads = append(payloads, fmt.Sprintf("{\"queue\": %q}", queue))
			queuesTriggered = append(queuesTriggered, queue)
		}
	}

	if len(payloads) < 1 {
		return nil
	}

	if c.driver.SupportsListenNotify() {
		err := tx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
			Payload: payloads,
			Schema:  c.config.Schema,
			Topic:   string(notifier.NotificationTopicInsert),
		})
		if err != nil {
			c.baseService.Logger.ErrorContext(
				ctx,
				c.baseService.Name+": Failed to send job insert notification",
				slog.String("queues", strings.Join(queuesTriggered, ",")),
				slog.String("err", err.Error()),
			)
			return err
		}
	}

	return nil
}

// emit a notification about a queue being paused or resumed.
func (c *Client[TTx]) notifyQueuePauseOrResume(ctx context.Context, tx riverdriver.ExecutorTx, action controlAction, queue string, opts *QueuePauseOpts) (*controlEventPayload, error) {
	c.baseService.Logger.DebugContext(ctx,
		c.baseService.Name+": Notifying about queue state change",
		slog.String("action", string(action)),
		slog.String("queue", queue),
		slog.String("opts", fmt.Sprintf("%+v", opts)),
	)

	controlEvent := &controlEventPayload{Action: action, Queue: queue}

	payload, err := json.Marshal(controlEvent)
	if err != nil {
		return nil, err
	}

	if c.driver.SupportsListenNotify() {
		err = tx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
			Payload: []string{string(payload)},
			Schema:  c.config.Schema,
			Topic:   string(notifier.NotificationTopicControl),
		})
		if err != nil {
			c.baseService.Logger.ErrorContext(
				ctx,
				c.baseService.Name+": Failed to send queue state change notification",
				slog.String("err", err.Error()),
			)
			return nil, err
		}
	}

	return controlEvent, nil
}

// Validates job args prior to insertion. Currently, verifies that a worker to
// handle the kind is registered in the configured workers bundle.
// This validation is skipped if the client is configured as an insert-only (with no workers)
// or if the client is configured to skip unknown job kinds.
func (c *Client[TTx]) validateJobArgs(args JobArgs) error {
	if c.config.Workers == nil || c.config.SkipUnknownJobCheck {
		return nil
	}

	if _, ok := c.config.Workers.workersMap[args.Kind()]; !ok {
		return &UnknownJobKindError{Kind: args.Kind()}
	}

	return nil
}

func (c *Client[TTx]) producerAdd(queueName string, queueConfig QueueConfig) (*producer, error) {
	c.producersMu.Lock()
	defer c.producersMu.Unlock()

	if _, alreadyExists := c.producersByQueueName[queueName]; alreadyExists {
		return nil, &QueueAlreadyAddedError{Name: queueName}
	}

	producer := newProducer(&c.baseService.Archetype, c.driver.GetExecutor(), c.pilot, &producerConfig{
		ClientID:                     c.config.ID,
		Completer:                    c.completer,
		ErrorHandler:                 c.config.ErrorHandler,
		FetchCooldown:                cmp.Or(queueConfig.FetchCooldown, c.config.FetchCooldown),
		FetchOnlyKnownKinds:          c.config.FetchOnlyKnownKinds,
		FetchPollInterval:            cmp.Or(queueConfig.FetchPollInterval, c.config.FetchPollInterval),
		PluginLookupByJob:            c.pluginLookupByJob,
		PluginLookupGlobal:           c.pluginLookupGlobal,
		JobStuckHandler:              c.config.JobStuckHandler,
		JobLease:                     c.config.JobLease,
		JobLeaseManager:              c.jobLeaseManager,
		JobStuckCount:                &c.stuckJobCount,
		JobStuckThreshold:            c.config.JobStuckThreshold,
		JobTimeout:                   c.config.JobTimeout,
		MaxWorkers:                   queueConfig.MaxWorkers,
		Notifier:                     c.notifier,
		Queue:                        queueName,
		QueueEventCallback:           c.subscriptionManager.distributeQueueEvent,
		QueuePollInterval:            c.config.queuePollInterval,
		RetryPolicy:                  c.config.RetryPolicy,
		SchedulerInterval:            c.config.schedulerInterval,
		Schema:                       c.config.Schema,
		StaleProducerRetentionPeriod: 5 * time.Minute,
		Workers:                      c.config.Workers,
	})
	c.producersByQueueName[queueName] = producer
	return producer, nil
}

func (c *Client[TTx]) producerRemove(ctx context.Context, queueName string) error {
	c.producersMu.Lock()
	defer c.producersMu.Unlock()

	producer, ok := c.producersByQueueName[queueName]
	if !ok {
		return &QueueNotFoundError{Name: queueName}
	}

	shouldStop, stopped, finalizeStop := producer.StopInit()
	if shouldStop {
		select {
		case <-ctx.Done():
			finalizeStop(false)
			return ctx.Err()
		case <-stopped:
			finalizeStop(true)
		}
	}

	delete(c.producersByQueueName, queueName)

	return nil
}

var nameRegex = regexp.MustCompile(`^(?:[a-z0-9])+(?:[_|\-]?[a-z0-9]+)*$`)

func validateQueueName(queueName string) error {
	if queueName == "" {
		return errors.New("queue name cannot be empty")
	}
	if len(queueName) > 64 {
		return errors.New("queue name cannot be longer than 64 characters")
	}
	if !nameRegex.MatchString(queueName) {
		return fmt.Errorf("queue name is invalid, expected letters and numbers separated by underscores or hyphens: %q", queueName)
	}
	return nil
}

// JobDeleteManyResult is the result of a job list operation. It contains a list of
// jobs and a cursor for fetching the next page of results.
type JobDeleteManyResult struct {
	// Jobs is a slice of job returned as part of the list operation.
	Jobs []*rivertype.JobRow
}

// JobDeleteMany deletes many jobs at once based on the conditions defined by
// JobDeleteManyParams. Running jobs are always ignored.
//
//	params := river.NewJobDeleteManyParams().First(10).State(rivertype.JobStateCompleted)
//	jobRows, err := client.JobDeleteMany(ctx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) JobDeleteMany(ctx context.Context, params *JobDeleteManyParams) (*JobDeleteManyResult, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}

	return c.jobDeleteMany(ctx, c.driver.GetExecutor(), params)
}

// JobDeleteManyTx deletes many jobs at once based on the conditions defined by
// JobDeleteManyParams. Running jobs are always ignored.
//
//	params := river.NewJobDeleteManyParams().First(10).States(river.JobStateCompleted)
//	jobRows, err := client.JobDeleteManyTx(ctx, tx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) JobDeleteManyTx(ctx context.Context, tx TTx, params *JobDeleteManyParams) (*JobDeleteManyResult, error) {
	return c.jobDeleteMany(ctx, c.driver.UnwrapExecutor(tx), params)
}

func (c *Client[TTx]) jobDeleteMany(ctx context.Context, exec riverdriver.Executor, params *JobDeleteManyParams) (*JobDeleteManyResult, error) {
	if params == nil {
		params = NewJobDeleteManyParams()
	}
	params.schema = c.config.Schema

	if params.filtersEmpty() && !params.unsafeAll {
		return nil, errors.New("delete with no filters not allowed to prevent accidental deletion of all jobs; either specify a predicate (e.g. JobDeleteManyParams.IDs, JobDeleteManyParams.Kinds, ...) or call JobDeleteManyParams.All")
	}

	listParams, err := dblist.JobMakeDriverParams(ctx, params.toDBParams(), c.driver)
	if err != nil {
		return nil, err
	}

	driverParams := (*riverdriver.JobDeleteManyParams)(listParams)

	var jobs []*rivertype.JobRow
	if pilot, ok := c.pilot.(riverpilot.PilotJobDeleter); ok {
		jobs, err = pilot.JobDeleteMany(ctx, exec, driverParams)
	} else {
		// Temporary compatibility fallback for Pilot implementations from before
		// PilotJobDeleter. Once Pilot embeds PilotJobDeleter, replace the
		// assertion above and this fallback with a direct call to
		// c.pilot.JobDeleteMany.
		jobs, err = exec.JobDeleteMany(ctx, driverParams)
	}
	if err != nil {
		return nil, err
	}

	return &JobDeleteManyResult{Jobs: jobs}, nil
}

// JobListResult is the result of a job list operation. It contains a list of
// jobs and a cursor for fetching the next page of results.
type JobListResult struct {
	// Jobs is a slice of job returned as part of the list operation.
	Jobs []*rivertype.JobRow

	// LastCursor is a cursor that can be used to list the next page of jobs.
	LastCursor *JobListCursor
}

var errJobListParamsMetadataNotSupportedSQLite = errors.New("JobListParams.Metadata is not supported on SQLite")

// JobList returns a paginated list of jobs matching the provided filters. The
// provided context is used for the underlying Postgres query and can be used to
// cancel the operation or apply a timeout.
//
//	params := river.NewJobListParams().First(10).State(rivertype.JobStateCompleted)
//	jobRows, err := client.JobList(ctx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) JobList(ctx context.Context, params *JobListParams) (*JobListResult, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}

	if params == nil {
		params = NewJobListParams()
	}
	params.schema = c.config.Schema

	if c.driver.DatabaseName() == riverdriver.DatabaseNameSQLite && params.metadataCalled {
		return nil, errJobListParamsMetadataNotSupportedSQLite
	}

	dbParams, err := params.toDBParams()
	if err != nil {
		return nil, err
	}

	listParams, err := dblist.JobMakeDriverParams(ctx, dbParams, c.driver)
	if err != nil {
		return nil, err
	}

	jobs, err := c.driver.GetExecutor().JobList(ctx, listParams)
	if err != nil {
		return nil, err
	}

	res := &JobListResult{Jobs: jobs}
	if len(jobs) > 0 {
		res.LastCursor = jobListCursorFromJobAndParams(jobs[len(jobs)-1], params)
	}
	return res, nil
}

// JobListTx returns a paginated list of jobs matching the provided filters. The
// provided context is used for the underlying Postgres query and can be used to
// cancel the operation or apply a timeout.
//
//	params := river.NewJobListParams().First(10).States(river.JobStateCompleted)
//	jobRows, err := client.JobListTx(ctx, tx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) JobListTx(ctx context.Context, tx TTx, params *JobListParams) (*JobListResult, error) {
	if params == nil {
		params = NewJobListParams()
	}
	params.schema = c.config.Schema

	if c.driver.DatabaseName() == riverdriver.DatabaseNameSQLite && params.metadataCalled {
		return nil, errJobListParamsMetadataNotSupportedSQLite
	}

	dbParams, err := params.toDBParams()
	if err != nil {
		return nil, err
	}

	listParams, err := dblist.JobMakeDriverParams(ctx, dbParams, c.driver)
	if err != nil {
		return nil, err
	}

	jobs, err := c.driver.UnwrapExecutor(tx).JobList(ctx, listParams)
	if err != nil {
		return nil, err
	}

	res := &JobListResult{Jobs: jobs}
	if len(jobs) > 0 {
		res.LastCursor = jobListCursorFromJobAndParams(jobs[len(jobs)-1], params)
	}
	return res, nil
}

// Notify retrieves a notification bundle for the client (in the sense of
// Postgres listen/notify) used to send notifications of various kinds.
func (c *Client[TTx]) Notify() *ClientNotifyBundle[TTx] {
	return c.clientNotifyBundle
}

// ClientNotifyBundle sends various notifications for a client (in the sense of
// Postgres listen/notify). Functions are on this bundle struct instead of the
// top-level client to keep them grouped together and better organized.
type ClientNotifyBundle[TTx any] struct {
	config *Config
	driver riverdriver.Driver[TTx]
}

// RequestResign sends a notification requesting that the current leader resign.
// This usually causes the resignation of the current leader, but may have no
// effect if no leader is currently elected.
func (c *ClientNotifyBundle[TTx]) RequestResign(ctx context.Context) error {
	return dbutil.WithTx(ctx, c.driver.GetExecutor(), func(ctx context.Context, execTx riverdriver.ExecutorTx) error {
		return c.requestResignTx(ctx, execTx)
	})
}

// RequestResignTx sends a notification requesting that the current leader
// resign. This usually causes the resignation of the current leader, but may
// have no effect if no leader is currently elected.
//
// This variant sends a notification in a transaction, which means that no
// notification is sent until the transaction commits.
func (c *ClientNotifyBundle[TTx]) RequestResignTx(ctx context.Context, tx TTx) error {
	return c.requestResignTx(ctx, c.driver.UnwrapExecutor(tx))
}

// notifyExecTx is a shared helper between Notify and NotifyTx that sends a
// notification.
func (c *ClientNotifyBundle[TTx]) requestResignTx(ctx context.Context, execTx riverdriver.ExecutorTx) error {
	payloadStr, err := json.Marshal(&leadership.DBNotification{
		Action: leadership.DBNotificationKindRequestResign,
	})
	if err != nil {
		return err
	}

	return execTx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
		Payload: []string{string(payloadStr)},
		Schema:  c.config.Schema,
		Topic:   string(notifier.NotificationTopicLeadership),
	})
}

// PeriodicJobs returns the currently configured set of periodic jobs for the
// client, and can be used to add new or remove existing ones.
//
// This function should only be invoked on clients capable of enqueueing
// periodic jobs: Queues and Workers must be configured and
// LeaderElectionDisabled must be false. Otherwise, invoking this function
// will panic. The client must be started and elected leader for periodic jobs
// to be enqueued.
func (c *Client[TTx]) PeriodicJobs() *PeriodicJobBundle {
	if !c.config.willExecuteJobs() {
		panic("client Queues and Workers must be configured to modify periodic jobs (otherwise, they'll have no effect because a client not configured to work jobs can't be started)")
	}
	if c.config.LeaderElectionDisabled {
		panic("cannot modify periodic jobs when LeaderElectionDisabled is true")
	}

	return c.periodicJobs
}

// Pilot returns the pilot in use by the pilot. If not configured, this is often
// simply StandardPilot.
//
// API is not stable. DO NOT USE.
func (c *Client[TTx]) Pilot() riverpilot.Pilot {
	return c.pilot
}

// Queues returns the currently configured set of queues for the client, and can
// be used to add new ones.
func (c *Client[TTx]) Queues() *QueueBundle { return c.queues }

// QueueGet returns the queue with the given name. If the queue has not recently
// been active or does not exist, returns ErrNotFound.
//
// The provided context is used for the underlying Postgres query and can be
// used to cancel the operation or apply a timeout.
func (c *Client[TTx]) QueueGet(ctx context.Context, name string) (*rivertype.Queue, error) {
	return c.driver.GetExecutor().QueueGet(ctx, &riverdriver.QueueGetParams{
		Name:   name,
		Schema: c.config.Schema,
	})
}

// QueueGetTx returns the queue with the given name. If the queue has not recently
// been active or does not exist, returns ErrNotFound.
//
// The provided context is used for the underlying Postgres query and can be
// used to cancel the operation or apply a timeout.
func (c *Client[TTx]) QueueGetTx(ctx context.Context, tx TTx, name string) (*rivertype.Queue, error) {
	return c.driver.UnwrapExecutor(tx).QueueGet(ctx, &riverdriver.QueueGetParams{
		Name:   name,
		Schema: c.config.Schema,
	})
}

// QueueListResult is the result of a job list operation. It contains a list of
// jobs and leaves room for future cursor functionality.
type QueueListResult struct {
	// Queues is a slice of queues returned as part of the list operation.
	Queues []*rivertype.Queue
}

// QueueList returns a list of all queues that are currently active or were
// recently active. Limit and offset can be used to paginate the results.
//
// The provided context is used for the underlying Postgres query and can be
// used to cancel the operation or apply a timeout.
//
//	params := river.NewQueueListParams().First(10)
//	queueRows, err := client.QueueListTx(ctx, tx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) QueueList(ctx context.Context, params *QueueListParams) (*QueueListResult, error) {
	if params == nil {
		params = NewQueueListParams()
	}

	queues, err := c.driver.GetExecutor().QueueList(ctx, &riverdriver.QueueListParams{
		Max:    int(params.paginationCount),
		Schema: c.config.Schema,
	})
	if err != nil {
		return nil, err
	}

	return &QueueListResult{Queues: queues}, nil
}

// QueueListTx returns a list of all queues that are currently active or were
// recently active. Limit and offset can be used to paginate the results.
//
// The provided context is used for the underlying Postgres query and can be
// used to cancel the operation or apply a timeout.
//
//	params := river.NewQueueListParams().First(10)
//	queueRows, err := client.QueueListTx(ctx, tx, params)
//	if err != nil {
//		// handle error
//	}
func (c *Client[TTx]) QueueListTx(ctx context.Context, tx TTx, params *QueueListParams) (*QueueListResult, error) {
	if params == nil {
		params = NewQueueListParams()
	}

	queues, err := c.driver.UnwrapExecutor(tx).QueueList(ctx, &riverdriver.QueueListParams{
		Max:    int(params.paginationCount),
		Schema: c.config.Schema,
	})
	if err != nil {
		return nil, err
	}

	return &QueueListResult{Queues: queues}, nil
}

// QueuePause pauses the queue with the given name. When a queue is paused,
// clients will not fetch any more jobs for that particular queue. To pause all
// queues at once, use the special queue name "*".
//
// Clients with a configured notifier should receive a notification about the
// paused queue(s) within a few milliseconds of the transaction commit. Clients
// in poll-only mode will pause after their next poll for queue configuration.
//
// The provided context is used for the underlying Postgres update and can be
// used to cancel the operation or apply a timeout. The opts are reserved for
// future functionality.
func (c *Client[TTx]) QueuePause(ctx context.Context, name string, opts *QueuePauseOpts) error {
	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return err
	}
	defer dbutil.RollbackWithoutCancel(ctx, tx)

	if err := tx.QueuePause(ctx, &riverdriver.QueuePauseParams{
		Name:   name,
		Now:    c.baseService.Time.NowOrNil(),
		Schema: c.config.Schema,
	}); err != nil {
		return err
	}

	controlEvent, err := c.notifyQueuePauseOrResume(ctx, tx, controlActionPause, name, opts)
	if err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return err
	}

	c.notifyProducerWithoutListenerQueueControlEvent(name, controlEvent)

	return nil
}

// QueuePauseTx pauses the queue with the given name. When a queue is paused,
// clients will not fetch any more jobs for that particular queue. To pause all
// queues at once, use the special queue name "*".
//
// Clients with a configured notifier should receive a notification about the
// paused queue(s) within a few milliseconds of the transaction commit. Clients
// in poll-only mode will pause after their next poll for queue configuration.
//
// The provided context is used for the underlying Postgres update and can be
// used to cancel the operation or apply a timeout. The opts are reserved for
// future functionality.
func (c *Client[TTx]) QueuePauseTx(ctx context.Context, tx TTx, name string, opts *QueuePauseOpts) error {
	executorTx := c.driver.UnwrapExecutor(tx)

	if err := executorTx.QueuePause(ctx, &riverdriver.QueuePauseParams{
		Name:   name,
		Now:    c.baseService.Time.NowOrNil(),
		Schema: c.config.Schema,
	}); err != nil {
		return err
	}

	if _, err := c.notifyQueuePauseOrResume(ctx, executorTx, controlActionPause, name, opts); err != nil {
		return err
	}

	return nil
}

// QueueResume resumes the queue with the given name. If the queue was
// previously paused, any clients configured to work that queue will resume
// fetching additional jobs. To resume all queues at once, use the special queue
// name "*".
//
// Clients with a configured notifier should receive a notification about the
// resumed queue(s) within a few milliseconds of the transaction commit. Clients
// in poll-only mode will resume after their next poll for queue configuration.
//
// The provided context is used for the underlying Postgres update and can be
// used to cancel the operation or apply a timeout. The opts are reserved for
// future functionality.
func (c *Client[TTx]) QueueResume(ctx context.Context, name string, opts *QueuePauseOpts) error {
	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return err
	}
	defer dbutil.RollbackWithoutCancel(ctx, tx)

	if err := tx.QueueResume(ctx, &riverdriver.QueueResumeParams{
		Name:   name,
		Now:    c.baseService.Time.NowOrNil(),
		Schema: c.config.Schema,
	}); err != nil {
		return err
	}

	controlEvent, err := c.notifyQueuePauseOrResume(ctx, tx, controlActionResume, name, opts)
	if err != nil {
		return err
	}

	if err = tx.Commit(ctx); err != nil {
		return err
	}

	c.notifyProducerWithoutListenerQueueControlEvent(name, controlEvent)

	return nil
}

// QueueResumeTx resumes the queue with the given name. If the queue was
// previously paused, any clients configured to work that queue will resume
// fetching additional jobs. To resume all queues at once, use the special queue
// name "*".
//
// Clients with a configured notifier should receive a notification about the
// resumed queue(s) within a few milliseconds of the transaction commit. Clients
// in poll-only mode will resume after their next poll for queue configuration.
//
// The provided context is used for the underlying Postgres update and can be
// used to cancel the operation or apply a timeout. The opts are reserved for
// future functionality.
func (c *Client[TTx]) QueueResumeTx(ctx context.Context, tx TTx, name string, opts *QueuePauseOpts) error {
	executorTx := c.driver.UnwrapExecutor(tx)

	if err := executorTx.QueueResume(ctx, &riverdriver.QueueResumeParams{
		Name:   name,
		Now:    c.baseService.Time.NowOrNil(),
		Schema: c.config.Schema,
	}); err != nil {
		return err
	}

	if _, err := c.notifyQueuePauseOrResume(ctx, executorTx, controlActionResume, name, opts); err != nil {
		return err
	}

	return nil
}

// QueueUpdateParams are the parameters for a QueueUpdate operation.
type QueueUpdateParams struct {
	// Metadata is the new metadata for the queue. If nil or empty, the metadata
	// will not be changed.
	Metadata []byte
}

// QueueUpdate updates a queue's settings in the database. These settings
// override the settings in the client (if applied).
func (c *Client[TTx]) QueueUpdate(ctx context.Context, name string, params *QueueUpdateParams) (*rivertype.Queue, error) {
	tx, err := c.driver.GetExecutor().Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer dbutil.RollbackWithoutCancel(ctx, tx)

	queue, controlEvent, err := c.queueUpdate(ctx, tx, name, params)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	c.notifyProducerWithoutListenerQueueControlEvent(name, controlEvent)

	return queue, nil
}

// QueueUpdateTx updates a queue's settings in the database. These settings
// override the settings in the client (if applied).
func (c *Client[TTx]) QueueUpdateTx(ctx context.Context, tx TTx, name string, params *QueueUpdateParams) (*rivertype.Queue, error) {
	queue, _, err := c.queueUpdate(ctx, c.driver.UnwrapExecutor(tx), name, params)
	if err != nil {
		return nil, err
	}
	return queue, nil
}

// Notifies an internal producer of a queue control event like pause/resume.
// Only invoked if the client's driver doesn't support a listener. If a listener
// is supported, control events go out via listen/notify instead.
//
// Should only ever be invoked *outside* a transaction. If invoked within a
// transaction, the producer wouldn't yet be able to access the state that
// triggered the notification because it's not committed yet.
func (c *Client[TTx]) notifyProducerWithoutListenerQueueControlEvent(queue string, controlEvent *controlEventPayload) {
	if c.driver.SupportsListener() {
		return
	}

	c.producersMu.RLock()
	defer c.producersMu.RUnlock()

	if len(c.producersByQueueName) < 1 {
		return
	}

	if producer, ok := c.producersByQueueName[queue]; ok {
		producer.TriggerQueueControlEvent(controlEvent)
	}
}

func (c *Client[TTx]) queueUpdate(ctx context.Context, executorTx riverdriver.ExecutorTx, name string, params *QueueUpdateParams) (*rivertype.Queue, *controlEventPayload, error) {
	updateMetadata := len(params.Metadata) > 0

	queue, err := executorTx.QueueUpdate(ctx, &riverdriver.QueueUpdateParams{
		Metadata:         params.Metadata,
		MetadataDoUpdate: updateMetadata,
		Name:             name,
		Schema:           c.config.Schema,
	})
	if err != nil {
		return nil, nil, err
	}

	if !updateMetadata {
		return queue, nil, err
	}

	controlEvent := &controlEventPayload{
		Action:   controlActionMetadataChanged,
		Metadata: params.Metadata,
		Queue:    queue.Name,
	}

	payload, err := json.Marshal(controlEvent)
	if err != nil {
		return nil, nil, err
	}

	if c.driver.SupportsListenNotify() {
		if err := executorTx.NotifyMany(ctx, &riverdriver.NotifyManyParams{
			Payload: []string{string(payload)},
			Schema:  c.config.Schema,
			Topic:   string(notifier.NotificationTopicControl),
		}); err != nil {
			return nil, nil, err
		}
	}

	return queue, controlEvent, nil
}

// Schema returns the configured schema for the client.
func (c *Client[TTx]) Schema() string { return c.config.Schema }

// QueueBundle is a bundle for adding additional queues. It's made accessible
// through Client.Queues.
type QueueBundle struct {
	clientFetchCooldown     time.Duration
	clientFetchPollInterval time.Duration

	clientWillExecuteJobs bool

	fetchCtx       context.Context                                                    //nolint:containedctx
	producerAdd    func(queueName string, queueConfig QueueConfig) (*producer, error) // add producer to associated client
	producerRemove func(ctx context.Context, queueName string) error                  // remove producer from associated client

	// Mutex that's acquired when client is starting and stopping and when a
	// queue is being added so that we can be sure that a client is fully
	// stopped or fully started when adding a new queue.
	startStopMu sync.Mutex

	workCtx context.Context //nolint:containedctx
}

// Add adds a new queue to the client. If the client is already started, a
// producer for the queue is started. Context is inherited from the one given to
// Client.Start.
func (b *QueueBundle) Add(queueName string, queueConfig QueueConfig) error {
	if !b.clientWillExecuteJobs {
		return errors.New("client is not configured to execute jobs, cannot add queue")
	}

	if err := queueConfig.validate(queueName, b.clientFetchCooldown, b.clientFetchPollInterval); err != nil {
		return err
	}

	b.startStopMu.Lock()
	defer b.startStopMu.Unlock()

	producer, err := b.producerAdd(queueName, queueConfig)
	if err != nil {
		return err
	}

	// Start the queue if the client is already started.
	if b.fetchCtx != nil && b.fetchCtx.Err() == nil {
		if err := producer.StartWorkContext(b.fetchCtx, b.workCtx); err != nil {
			return err
		}
	}

	return nil
}

// Remove removes a queue from the client, stopping the producer if the client
// is running. It waits for any jobs currently being worked in the queue to
// complete before returning.
//
// If the provided context is done before the producer has fully stopped, Remove
// returns the context's error and does not fully remove the queue, though the
// queue's producer may have started stopping, leaving it in a stopped state
// where it doesn't work new jobs. Call Remove again with a new context to
// remove it completely.
//
// Returns an error if the client is not configured to execute jobs or if the
// specified queue does not exist.
func (b *QueueBundle) Remove(ctx context.Context, queueName string) error {
	if !b.clientWillExecuteJobs {
		return errors.New("client is not configured to execute jobs, cannot remove queue")
	}

	b.startStopMu.Lock()
	defer b.startStopMu.Unlock()

	return b.producerRemove(ctx, queueName)
}

// Generates a default client ID using the current hostname and time.
func defaultClientID(startedAt time.Time) string {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown_host"
	}

	return defaultClientIDWithHost(startedAt, host)
}

// Same as the above, but allows host injection for testability.
func defaultClientIDWithHost(startedAt time.Time, host string) string {
	const maxHostLength = 60

	// Truncate degenerately long host names.
	host = strings.ReplaceAll(host, ".", "_")
	if len(host) > maxHostLength {
		host = host[0:maxHostLength]
	}

	// Dots, hyphens, and colons aren't particularly friendly for double click
	// to select (depends on application and configuration), so avoid them all
	// in favor of underscores.
	//
	// Go's time package is really dumb and can't format subseconds without
	// using a dot. So use the dot, then replace it with an underscore below.
	const rfc3339Compact = "2006_01_02T15_04_05.000000"

	return host + "_" + strings.Replace(startedAt.Format(rfc3339Compact), ".", "_", 1)
}

var (
	// ErrSequenceAlreadyResolved requires inserting a new tail job instead of retrying history.
	ErrSequenceAlreadyResolved = sequenceruntime.ErrAlreadyResolved
	// ErrSequenceBlocked indicates that an earlier terminal failure must be acknowledged.
	ErrSequenceBlocked = sequenceruntime.ErrBlocked
	// ErrSequenceBypass rejects ordered inserts and retries while a group is draining unordered.
	ErrSequenceBypass = sequenceruntime.ErrBypass
	// ErrSequenceDisabled rejects new ordered inserts when acceptance is disabled.
	ErrSequenceDisabled = errors.New("river: sequence insertion is disabled")
	// ErrSequenceFastInsert requires using InsertMany for ordered jobs.
	ErrSequenceFastInsert = errors.New("river: sequence jobs are not supported by InsertManyFast; use InsertMany")
	// ErrSequenceNeedsRepair indicates inconsistent or missing coordination data.
	ErrSequenceNeedsRepair = sequenceruntime.ErrNeedsRepair
	// ErrSequenceNotDrained indicates that bypassed work has not completely finished.
	ErrSequenceNotDrained = sequenceruntime.ErrNotDrained
	// ErrSequenceNotHead prevents a successor from bypassing its predecessor on retry.
	ErrSequenceNotHead = sequenceruntime.ErrNotHead
	// ErrSequenceNotSupported indicates that the driver does not support ordered jobs.
	ErrSequenceNotSupported = sequenceruntime.ErrNotSupported
	// ErrSequencePending rejects a separate manual pending requirement on an ordered job.
	ErrSequencePending = errors.New("river: sequence jobs cannot use InsertOpts.Pending")
	// ErrSequencePositionExhausted indicates that the durable position allocator cannot advance.
	ErrSequencePositionExhausted = sequenceruntime.ErrPositionExhausted
	// ErrSequenceStateConflict indicates that an administrator's expected barrier is stale.
	ErrSequenceStateConflict = sequenceruntime.ErrStateConflict
)

// JobArgsWithSequenceOpts opts a job into durable, ordered execution. New jobs
// enter pending, including the head and jobs scheduled for the future. Only the
// independent reconciler releases them into River's ordinary job lifecycle.
// Workers still require idempotency because River provides at-least-once execution.
type JobArgsWithSequenceOpts interface {
	JobArgs
	SequenceOpts() SequenceOpts
}

// SequenceActionOpts identifies an explicit administrative ordering decision.
// Reason is required and is recorded in structured application logs. Applications
// requiring permanent audit history should also use their own audit system.
type SequenceActionOpts struct {
	Reason    string
	RequestID string
}

// SequenceConfig controls ordered inserts independently of existing work.
type SequenceConfig struct {
	// AcceptNewJobs enables inserts implementing JobArgsWithSequenceOpts.
	// Defaults to false. Turning it off does not stop reconciliation.
	AcceptNewJobs bool
	// BatchSize bounds the groups examined on each pass. Defaults to 32.
	BatchSize int
	// DisableReconciler explicitly disables the background service on this
	// client. Use only when another executing client reconciles committed work
	// or while performing the documented offline recovery procedure.
	DisableReconciler bool
	// PollInterval is the delay between bounded passes. Defaults to one second.
	PollInterval time.Duration
	// TransactionTimeout bounds independent reconciliation, administrative,
	// and sequence periodic-insert transactions. It does not override
	// caller-owned business transactions. Defaults to two seconds.
	TransactionTimeout time.Duration
}

// withDefaults copies configuration and fills bounded background service defaults.
func (c SequenceConfig) withDefaults() SequenceConfig {
	if c.BatchSize == 0 {
		c.BatchSize = 32
	}
	if c.PollInterval == 0 {
		c.PollInterval = time.Second
	}
	if c.TransactionTimeout == 0 {
		c.TransactionTimeout = 2 * time.Second
	}
	return c
}

// validate rejects values that could make coordination unbounded or excessively busy.
func (c SequenceConfig) validate() error {
	if c.BatchSize < 1 || c.BatchSize > 256 {
		return errors.New("Sequences.BatchSize must be between 1 and 256")
	}
	if c.PollInterval < 10*time.Millisecond || c.PollInterval > time.Minute {
		return errors.New("Sequences.PollInterval must be between 10ms and 1m")
	}
	if c.TransactionTimeout < 100*time.Millisecond || c.TransactionTimeout > 10*time.Second {
		return errors.New("Sequences.TransactionTimeout must be between 100ms and 10s")
	}
	return nil
}

// SequenceGroup contains persistent allocation and stop-policy information.
type SequenceGroup = riverdriver.SequenceGroup

// SequenceInfo is an operational snapshot. Concurrent job transitions may change
// its head or counts immediately after it is read; management operations always
// lock and revalidate the actual rows before changing them.
type SequenceInfo struct {
	*SequenceGroup

	ActiveCount      int64
	Head             *rivertype.JobRow
	NonterminalCount int64
	UnresolvedCount  int64
}

// SequenceListOpts selects a bounded page ordered by group key.
type SequenceListOpts struct {
	AfterKey string
	// Limit defaults to 100 and must be between 1 and 1000.
	Limit int
}

// SequenceOpts defines the grouping key and the current job's terminal policy.
// Kind participates unless ExcludeKind is set. With ByArgs, fields tagged
// river:"sequence" select the argument fingerprint; otherwise all encoded args
// participate. A group with no selected dimension falls back to kind.
type SequenceOpts struct {
	ByArgs              bool
	ByQueue             bool
	ContinueOnCancelled bool
	ContinueOnDiscarded bool
	ExcludeKind         bool
}

// SequenceReconcileResult describes one bounded reconciliation transaction.
// A Tx result describes changes that remain subject to the caller's commit.
type SequenceReconcileResult = sequenceruntime.ReconcileResult

// SequenceStatus reports local coordinator health and committed progress counters.
type SequenceStatus = sequenceruntime.Status

// SequenceResumeOpts acknowledges exactly the blocked job seen by an operator.
// The expected ID and position prevent a stale request from skipping a different
// failure. Reason is required and emitted in structured logs.
type SequenceResumeOpts struct {
	ExpectedJobID    int64
	ExpectedPosition int64
	Reason           string
	RequestID        string
}

// SequenceBypass explicitly waives ordering while preserving every job and its
// normal execution/retry state. The reconciler drains pending members in bounded
// batches. New ordered inserts are rejected until SequenceRestore succeeds.
func (c *Client[TTx]) SequenceBypass(ctx context.Context, key string, opts *SequenceActionOpts) error {
	return c.sequenceAction(ctx, key, opts, "bypass", sequenceruntime.Bypass)
}

// SequenceBypassTx changes bypass mode within the caller-owned transaction.
func (c *Client[TTx]) SequenceBypassTx(ctx context.Context, tx TTx, key string, opts *SequenceActionOpts) error {
	return c.sequenceActionTx(ctx, c.driver.UnwrapExecutor(tx), key, opts, "bypass", sequenceruntime.Bypass)
}

// SequenceGet returns a bounded operational snapshot of a sequence group.
func (c *Client[TTx]) SequenceGet(ctx context.Context, key string) (*SequenceInfo, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}
	return c.sequenceGet(ctx, c.driver.GetExecutor(), key)
}

// SequenceGetTx reads a snapshot using the caller-owned transaction.
func (c *Client[TTx]) SequenceGetTx(ctx context.Context, tx TTx, key string) (*SequenceInfo, error) {
	return c.sequenceGet(ctx, c.driver.UnwrapExecutor(tx), key)
}

// SequenceList returns groups in key order. Pass the last key as AfterKey to
// continue pagination; each snapshot includes its current head and counts.
func (c *Client[TTx]) SequenceList(ctx context.Context, opts *SequenceListOpts) ([]*SequenceInfo, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}
	return c.sequenceList(ctx, c.driver.GetExecutor(), opts)
}

// SequenceListTx reads groups using the caller-owned transaction.
func (c *Client[TTx]) SequenceListTx(ctx context.Context, tx TTx, opts *SequenceListOpts) ([]*SequenceInfo, error) {
	return c.sequenceList(ctx, c.driver.UnwrapExecutor(tx), opts)
}

// SequenceReconcile performs one bounded group check in an independent
// transaction. It never changes the outcome of already completed work.
func (c *Client[TTx]) SequenceReconcile(ctx context.Context, key string) (*SequenceReconcileResult, error) {
	if !c.driver.PoolIsSet() {
		return nil, errNoDriverDBPool
	}
	if err := validateSequenceKey(key); err != nil {
		return nil, err
	}
	var result *SequenceReconcileResult
	err := c.sequenceWithTx(ctx, func(ctx context.Context, tx riverdriver.ExecutorTx) error {
		var err error
		result, err = sequenceruntime.Reconcile(ctx, tx, c.config.Schema, key)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// SequenceReconcileTx performs one check without committing the caller's transaction.
func (c *Client[TTx]) SequenceReconcileTx(ctx context.Context, tx TTx, key string) (*SequenceReconcileResult, error) {
	if err := validateSequenceKey(key); err != nil {
		return nil, err
	}
	return sequenceruntime.Reconcile(ctx, c.driver.UnwrapExecutor(tx), c.config.Schema, key)
}

// SequenceRestore re-enables ordered inserts only after all members, including
// previously bypassed members, are terminal and their ordering is resolved.
func (c *Client[TTx]) SequenceRestore(ctx context.Context, key string, opts *SequenceActionOpts) error {
	return c.sequenceAction(ctx, key, opts, "restore", sequenceruntime.Restore)
}

// SequenceRestoreTx restores ordering within the caller-owned transaction.
func (c *Client[TTx]) SequenceRestoreTx(ctx context.Context, tx TTx, key string, opts *SequenceActionOpts) error {
	return c.sequenceActionTx(ctx, c.driver.UnwrapExecutor(tx), key, opts, "restore", sequenceruntime.Restore)
}

// SequenceResume acknowledges a cancelled or discarded barrier and resolves it
// atomically. It does not change the job's terminal outcome. An explicitly
// deleted barrier may also be acknowledged using its stored ID and position.
func (c *Client[TTx]) SequenceResume(ctx context.Context, key string, opts *SequenceResumeOpts) error {
	if !c.driver.PoolIsSet() {
		return errNoDriverDBPool
	}
	return c.sequenceWithTx(ctx, func(ctx context.Context, tx riverdriver.ExecutorTx) error {
		return c.sequenceResume(ctx, tx, key, opts)
	})
}

// SequenceResumeTx acknowledges the expected barrier in the caller's transaction.
func (c *Client[TTx]) SequenceResumeTx(ctx context.Context, tx TTx, key string, opts *SequenceResumeOpts) error {
	return c.sequenceResume(ctx, c.driver.UnwrapExecutor(tx), key, opts)
}

// SequenceStatus returns local background coordinator health, or nil for an
// insert-only client, an unsupported driver, or an explicitly disabled service.
func (c *Client[TTx]) SequenceStatus() *SequenceStatus {
	if c.sequenceService == nil {
		return nil
	}
	status := c.sequenceService.Status()
	return &status
}

// attachSequenceInsert attaches only new jobs; unique matches retain their original identity.
func (c *Client[TTx]) attachSequenceInsert(ctx context.Context, tx riverdriver.ExecutorTx, entries []*sequenceInsertEntry, results []*riverdriver.JobInsertFastResult) error {
	if len(entries) == 0 {
		return nil
	}
	if len(entries) != len(results) {
		return errors.New("river: sequence insert result count differs from input")
	}
	attachments := make([]sequenceruntime.Attachment, 0, len(entries))
	resultIndexes := make([]int, 0, len(entries))
	for i, entry := range entries {
		if entry == nil || results[i].UniqueSkippedAsDuplicate {
			continue
		}
		attachments = append(attachments, sequenceruntime.Attachment{
			ContinueOnCancelled: entry.opts.ContinueOnCancelled,
			ContinueOnDiscarded: entry.opts.ContinueOnDiscarded,
			JobID:               results[i].Job.ID,
			Key:                 entry.key,
		})
		resultIndexes = append(resultIndexes, i)
	}
	jobs, err := sequenceruntime.Attach(ctx, tx, c.config.Schema, attachments)
	if err != nil {
		return err
	}
	if len(jobs) != len(attachments) {
		return errors.New("river: sequence attach result count differs from input")
	}
	for i, job := range jobs {
		results[resultIndexes[i]].Job = job
	}
	return nil
}

// insertManyWithSequences uses an explicit savepoint only for ordered batches.
// A post-insert Go error must not leave an unattached pending job if a caller
// commits its surrounding business transaction after handling that error.
func (c *Client[TTx]) insertManyWithSequences(ctx context.Context, tx riverdriver.ExecutorTx, params []*riverdriver.JobInsertFastParams) ([]*riverdriver.JobInsertFastResult, error) {
	ordered := false
	for _, param := range params {
		if _, ok := param.Args.(JobArgsWithSequenceOpts); ok {
			ordered = true
			break
		}
	}
	if !ordered {
		return c.pilot.JobInsertMany(ctx, tx, &riverdriver.JobInsertFastManyParams{Jobs: params, Schema: c.config.Schema})
	}
	if _, ok := tx.(riverdriver.SequenceExecutor); !ok {
		return nil, ErrSequenceNotSupported
	}
	savepoint, err := tx.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("river: begin sequence insert savepoint: %w", err)
	}
	defer dbutil.RollbackWithoutCancel(ctx, savepoint)
	entries, err := c.prepareSequenceInsert(ctx, savepoint, params)
	if err != nil {
		return nil, rollbackSequenceInsert(ctx, savepoint, err)
	}
	results, err := c.pilot.JobInsertMany(ctx, savepoint, &riverdriver.JobInsertFastManyParams{Jobs: params, Schema: c.config.Schema})
	if err != nil {
		return nil, rollbackSequenceInsert(ctx, savepoint, err)
	}
	if err := c.attachSequenceInsert(ctx, savepoint, entries, results); err != nil {
		return nil, rollbackSequenceInsert(ctx, savepoint, err)
	}
	if err := savepoint.Commit(ctx); err != nil {
		return nil, rollbackSequenceInsert(ctx, savepoint, err)
	}
	return results, nil
}

// rollbackSequenceInsert makes error atomicity independent of the outer owner.
func rollbackSequenceInsert(ctx context.Context, tx riverdriver.ExecutorTx, originalErr error) error {
	if err := dbutil.RollbackWithoutCancel(ctx, tx); err != nil {
		return errors.Join(originalErr, fmt.Errorf("river: rollback sequence insert savepoint: %w", err))
	}
	return originalErr
}

// prepareSequenceInsert runs after all insert hooks, using their final queue and args.
func (c *Client[TTx]) prepareSequenceInsert(ctx context.Context, tx riverdriver.ExecutorTx, params []*riverdriver.JobInsertFastParams) ([]*sequenceInsertEntry, error) {
	var entries []*sequenceInsertEntry
	var keys []string
	for i, param := range params {
		args, ok := param.Args.(JobArgsWithSequenceOpts)
		if !ok {
			continue
		}
		if _, supported := tx.(riverdriver.SequenceExecutor); !supported {
			return nil, ErrSequenceNotSupported
		}
		if !c.config.Sequences.AcceptNewJobs {
			return nil, ErrSequenceDisabled
		}
		if param.State == rivertype.JobStatePending {
			return nil, ErrSequencePending
		}
		opts := args.SequenceOpts()
		finalArgs, err := sequenceFinalArgs(args, param.EncodedArgs)
		if err != nil {
			return nil, err
		}
		key, err := sequenceKey(param.Queue, param.Kind, param.EncodedArgs, opts, finalArgs)
		if err != nil {
			return nil, err
		}
		if entries == nil {
			entries = make([]*sequenceInsertEntry, len(params))
		}
		entries[i] = &sequenceInsertEntry{key: key, opts: opts}
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	if err := sequenceruntime.Prepare(ctx, tx, c.config.Schema, keys); err != nil {
		return nil, err
	}
	for i, entry := range entries {
		if entry != nil {
			params[i].State = rivertype.JobStatePending
		}
	}
	return entries, nil
}

// sequenceAction runs a reasoned administrative operation in an owned transaction.
func (c *Client[TTx]) sequenceAction(ctx context.Context, key string, opts *SequenceActionOpts, action string, perform func(context.Context, riverdriver.ExecutorTx, string, string) error) error {
	if !c.driver.PoolIsSet() {
		return errNoDriverDBPool
	}
	return c.sequenceWithTx(ctx, func(ctx context.Context, tx riverdriver.ExecutorTx) error {
		return c.sequenceActionTx(ctx, tx, key, opts, action, perform)
	})
}

// sequenceActionTx logs successful transaction-local administrative changes.
func (c *Client[TTx]) sequenceActionTx(ctx context.Context, tx riverdriver.ExecutorTx, key string, opts *SequenceActionOpts, action string, perform func(context.Context, riverdriver.ExecutorTx, string, string) error) error {
	if err := validateSequenceKey(key); err != nil {
		return err
	}
	if opts == nil || strings.TrimSpace(opts.Reason) == "" {
		return errors.New("river: sequence administrative action requires a reason")
	}
	if err := perform(ctx, tx, c.config.Schema, key); err != nil {
		return err
	}
	c.baseService.Logger.WarnContext(ctx, "River sequence administrative change (subject to transaction commit)",
		"schema", c.config.Schema, "key", key, "action", action, "reason", opts.Reason, "request_id", opts.RequestID)
	return nil
}

// sequenceGet reads a group without taking execution or coordination locks.
func (c *Client[TTx]) sequenceGet(ctx context.Context, exec riverdriver.Executor, key string) (*SequenceInfo, error) {
	if err := validateSequenceKey(key); err != nil {
		return nil, err
	}
	sequenceExec, ok := exec.(riverdriver.SequenceExecutor)
	if !ok {
		return nil, ErrSequenceNotSupported
	}
	group, err := sequenceExec.SequenceGroupGet(ctx, &riverdriver.SequenceGroupGetParams{Key: key, Schema: c.config.Schema})
	if err != nil {
		return nil, err
	}
	return c.sequenceInfo(ctx, sequenceExec, group)
}

// sequenceInfo combines independent diagnostic reads; mutation paths never trust this snapshot.
func (c *Client[TTx]) sequenceInfo(ctx context.Context, exec riverdriver.SequenceExecutor, group *riverdriver.SequenceGroup) (*SequenceInfo, error) {
	params := &riverdriver.SequenceGroupKeyParams{Key: group.Key, Schema: c.config.Schema}
	stats, err := exec.SequenceJobStats(ctx, params)
	if err != nil {
		return nil, err
	}
	head, err := exec.SequenceJobHead(ctx, params)
	if err != nil && !errors.Is(err, rivertype.ErrNotFound) {
		return nil, err
	}
	return &SequenceInfo{
		SequenceGroup:    group,
		ActiveCount:      stats.Active,
		Head:             head,
		NonterminalCount: stats.Nonterminal,
		UnresolvedCount:  stats.Unresolved,
	}, nil
}

// sequenceList enumerates bounded pages so administration cannot scan the entire table accidentally.
func (c *Client[TTx]) sequenceList(ctx context.Context, exec riverdriver.Executor, opts *SequenceListOpts) ([]*SequenceInfo, error) {
	sequenceExec, ok := exec.(riverdriver.SequenceExecutor)
	if !ok {
		return nil, ErrSequenceNotSupported
	}
	if opts == nil {
		opts = &SequenceListOpts{}
	}
	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return nil, errors.New("river: sequence list limit must be between 1 and 1000")
	}
	groups, err := sequenceExec.SequenceGroupList(ctx, &riverdriver.SequenceGroupListParams{AfterKey: opts.AfterKey, Limit: limit, Schema: c.config.Schema})
	if err != nil {
		return nil, err
	}
	result := make([]*SequenceInfo, 0, len(groups))
	for _, group := range groups {
		info, err := c.sequenceInfo(ctx, sequenceExec, group)
		if err != nil {
			return nil, err
		}
		result = append(result, info)
	}
	return result, nil
}

// sequenceResume validates the exact acknowledgement before the runtime takes locks.
func (c *Client[TTx]) sequenceResume(ctx context.Context, tx riverdriver.ExecutorTx, key string, opts *SequenceResumeOpts) error {
	if err := validateSequenceKey(key); err != nil {
		return err
	}
	if opts == nil || opts.ExpectedJobID <= 0 || opts.ExpectedPosition <= 0 || strings.TrimSpace(opts.Reason) == "" {
		return errors.New("river: sequence resume requires expected job ID, position, and a reason")
	}
	if err := sequenceruntime.Resume(ctx, tx, c.config.Schema, key, opts.ExpectedJobID, opts.ExpectedPosition); err != nil {
		return err
	}
	c.baseService.Logger.WarnContext(ctx, "River sequence administrative change (subject to transaction commit)",
		"schema", c.config.Schema, "key", key, "action", "resume", "job_id", opts.ExpectedJobID,
		"position", opts.ExpectedPosition, "reason", opts.Reason, "request_id", opts.RequestID)
	return nil
}

// sequenceWithTx bounds only transactions created by non-Tx sequence methods.
// Caller-owned Tx methods deliberately never change isolation or timeout settings.
func (c *Client[TTx]) sequenceWithTx(ctx context.Context, perform func(context.Context, riverdriver.ExecutorTx) error) error {
	ctx, cancel := context.WithTimeout(ctx, c.config.Sequences.TransactionTimeout)
	defer cancel()
	return dbutil.WithTx(ctx, c.driver.GetExecutor(), func(ctx context.Context, tx riverdriver.ExecutorTx) error {
		exec, ok := tx.(riverdriver.SequenceExecutor)
		if !ok {
			return ErrSequenceNotSupported
		}
		if err := exec.SequenceSetLocalTimeouts(ctx); err != nil {
			return err
		}
		return perform(ctx, tx)
	})
}

// sequenceInsertEntry holds prepared sequence identity until newly inserted rows can be attached.
type sequenceInsertEntry struct {
	key  string
	opts SequenceOpts
}

// sequenceFinalArgs preserves reference encoding for ordinary inserts while
// deriving tagged fields from the actual stored payload when a hook changes it.
func sequenceFinalArgs(args any, encodedArgs []byte) (any, error) {
	original, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("river: marshaling final sequence args: %w", err)
	}
	if bytes.Equal(original, encodedArgs) {
		return args, nil
	}
	argType := reflect.TypeOf(args)
	if argType == nil {
		return nil, errors.New("river: sequence args are nil")
	}
	if argType.Kind() == reflect.Pointer {
		argType = argType.Elem()
	}
	decoded := reflect.New(argType)
	if err := json.Unmarshal(encodedArgs, decoded.Interface()); err != nil {
		return nil, fmt.Errorf("river: decoding final sequence args: %w", err)
	}
	return decoded.Interface(), nil
}

// sequenceKey implements the stable v1 grouping protocol for ordered jobs.
func sequenceKey(queue, kind string, encodedArgs []byte, opts SequenceOpts, args any) (string, error) {
	parts := make([]string, 0, 3)
	if opts.ByQueue {
		parts = append(parts, "queue="+queue)
	}
	if !opts.ExcludeKind {
		parts = append(parts, "kind="+kind)
	}
	if opts.ByArgs {
		fingerprint := encodedArgs
		if selected := sequenceTaggedFields(args); len(selected) > 0 {
			var err error
			fingerprint, err = json.Marshal(selected)
			if err != nil {
				return "", fmt.Errorf("river: marshaling sequence fields: %w", err)
			}
		}
		parts = append(parts, "args="+string(fingerprint))
	}
	if len(parts) == 0 {
		parts = append(parts, "kind="+kind)
	}
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// sequenceTaggedFields preserves the reference's top-level field and tag semantics.
func sequenceTaggedFields(args any) map[string]any {
	selected := make(map[string]any)
	value := reflect.ValueOf(args)
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return selected
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return selected
	}
	for i := range value.NumField() {
		field := value.Type().Field(i)
		if field.PkgPath != "" || !strings.Contains(field.Tag.Get("river"), "sequence") {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		selected[name] = value.Field(i).Interface()
	}
	return selected
}

// validateSequenceKey protects management calls from accidental unbounded or empty scopes.
func validateSequenceKey(key string) error {
	if len(key) == 0 || len(key) >= 512 {
		return errors.New("river: sequence key must contain between 1 and 511 bytes")
	}
	return nil
}
