package maintenance

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/tidwall/sjson"

	"github.com/riverqueue/river/internal/pluginlookup"
	"github.com/riverqueue/river/internal/rivercommon"
	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/baseservice"
	"github.com/riverqueue/river/rivershared/riverpilot"
	"github.com/riverqueue/river/rivershared/riversharedmaintenance"
	"github.com/riverqueue/river/rivershared/startstop"
	"github.com/riverqueue/river/rivershared/testsignal"
	"github.com/riverqueue/river/rivershared/util/dbutil"
	"github.com/riverqueue/river/rivershared/util/maputil"
	"github.com/riverqueue/river/rivershared/util/sliceutil"
	"github.com/riverqueue/river/rivershared/util/testutil"
	"github.com/riverqueue/river/rivershared/util/timeutil"
	"github.com/riverqueue/river/rivertype"
)

// ErrNoJobToInsert can be returned by a PeriodicJob's JobToInsertFunc to
// signal that there's no job to insert at this time.
var ErrNoJobToInsert = errors.New("a nil job was returned, nothing to insert")

// PeriodicJobEnqueuerTestSignals are internal signals used exclusively in tests.
type PeriodicJobEnqueuerTestSignals struct {
	EnteredLoop                 testsignal.TestSignal[struct{}] // notifies when the enqueuer finishes start up and enters its initial run loop
	InsertedJobs                testsignal.TestSignal[struct{}] // notifies when a batch of jobs is inserted
	PeriodicJobKeepAliveAndReap testsignal.TestSignal[struct{}] // notifies when the background services that runs keep alive and reap on periodic jobs ticks
	PeriodicJobUpserted         testsignal.TestSignal[struct{}] // notifies when a batch of periodic job records are upserted to pilot
	SkippedJob                  testsignal.TestSignal[struct{}] // notifies when a job is skipped because of nil JobInsertParams
}

func (ts *PeriodicJobEnqueuerTestSignals) Init(tb testutil.TestingTB) {
	ts.EnteredLoop.Init(tb)
	ts.InsertedJobs.Init(tb)
	ts.PeriodicJobKeepAliveAndReap.Init(tb)
	ts.PeriodicJobUpserted.Init(tb)
	ts.SkippedJob.Init(tb)
}

// PeriodicJob is a periodic job to be run. It's similar to the top-level
// river.PeriodicJobArgs, but needs a separate type because the enqueuer is in a
// subpackage.
type PeriodicJob struct {
	ID              string
	ConstructorFunc func() (*rivertype.JobInsertParams, error)
	RunOnStart      bool
	ScheduleFunc    func(time.Time) time.Time

	nextRunAt time.Time // set on service start
}

func (j *PeriodicJob) mustValidate() *PeriodicJob {
	if err := j.validate(); err != nil {
		panic(err)
	}
	return j
}

func (j *PeriodicJob) validate() error {
	if j.ID != "" {
		if len(j.ID) >= 128 {
			return errors.New("PeriodicJob.ID must be less than 128 characters")
		}
		if !rivercommon.UserSpecifiedIDOrKindRE.MatchString(j.ID) {
			return fmt.Errorf("PeriodicJob.ID %q should match regex %s", j.ID, rivercommon.UserSpecifiedIDOrKindRE.String())
		}
	}
	if j.ConstructorFunc == nil {
		return errors.New("PeriodicJob.ConstructorFunc must be set")
	}
	if j.ScheduleFunc == nil {
		return errors.New("PeriodicJob.ScheduleFunc must be set")
	}

	return nil
}

// InsertFunc inserts jobs using the enqueuer-owned transaction. Implementations
// must not commit or roll back that transaction.
type InsertFunc func(ctx context.Context, tx riverdriver.ExecutorTx, insertParams []*rivertype.JobInsertParams) ([]*rivertype.JobInsertResult, error)

type PeriodicJobEnqueuerConfig struct {
	AdvisoryLockPrefix int32

	PluginLookupGlobal *pluginlookup.PluginLookup

	// Insert is the function to call to insert jobs into the database.
	Insert InsertFunc

	// IsolateJob identifies constructor outputs that require an independent
	// transaction on a bounded background worker. Ordinary jobs commit before
	// isolated work is dispatched, and never wait for its transaction to finish.
	IsolateJob func(*rivertype.JobInsertParams) bool

	// IsolatedInsertTimeout bounds each isolated transaction independently.
	// Zero uses the ordinary maintenance timeout.
	IsolatedInsertTimeout time.Duration

	// PeriodicJobs are the periodic jobs with which to configure the enqueuer.
	PeriodicJobs []*PeriodicJob

	// Pilot is a plugin module providing additional non-standard functionality.
	Pilot riverpilot.PilotPeriodicJob

	// Schema where River tables are located. Empty string omits schema, causing
	// Postgres to default to `search_path`.
	Schema string
}

func (c *PeriodicJobEnqueuerConfig) mustValidate() *PeriodicJobEnqueuerConfig {
	// no validations currently
	return c
}

// PeriodicJobEnqueuer inserts jobs configured to run periodically as unique
// jobs to make sure they'll run as frequently as their period dictates.
type PeriodicJobEnqueuer struct {
	riversharedmaintenance.QueueMaintainerServiceBase
	startstop.BaseStartStop

	// exported for test purposes
	Config      *PeriodicJobEnqueuerConfig
	TestSignals PeriodicJobEnqueuerTestSignals

	exec               riverdriver.Executor
	mu                 sync.RWMutex
	nextHandle         rivertype.PeriodicJobHandle
	periodicJobIDs     map[string]rivertype.PeriodicJobHandle
	periodicJobs       map[rivertype.PeriodicJobHandle]*PeriodicJob
	recalculateNextRun chan struct{}

	// Isolated occurrences survive a service restart so a cancelled attempt
	// retries its original constructor output instead of generating another job.
	isolatedOccurrences map[rivertype.PeriodicJobHandle]*periodicJobIsolatedOccurrence
	retiredIsolatedIDs  map[string]rivertype.PeriodicJobHandle
}

func NewPeriodicJobEnqueuer(archetype *baseservice.Archetype, config *PeriodicJobEnqueuerConfig, exec riverdriver.Executor) (*PeriodicJobEnqueuer, error) {
	var (
		nextHandle     rivertype.PeriodicJobHandle
		periodicJobIDs = make(map[string]rivertype.PeriodicJobHandle)
		periodicJobs   = make(map[rivertype.PeriodicJobHandle]*PeriodicJob, len(config.PeriodicJobs))
	)

	for _, periodicJob := range config.PeriodicJobs {
		if err := periodicJob.validate(); err != nil {
			return nil, err
		}

		handle := nextHandle

		if err := addUniqueID(periodicJobIDs, periodicJob.ID, handle); err != nil {
			return nil, err
		}

		periodicJobs[handle] = periodicJob
		nextHandle++
	}

	pluginLookupGlobal := config.PluginLookupGlobal
	if pluginLookupGlobal == nil {
		pluginLookupGlobal = pluginlookup.NewPluginLookup(nil)
	}

	pilot := config.Pilot
	if pilot == nil {
		pilot = &riverpilot.StandardPilot{}
	}

	svc := baseservice.Init(archetype, &PeriodicJobEnqueuer{
		Config: (&PeriodicJobEnqueuerConfig{
			AdvisoryLockPrefix:    config.AdvisoryLockPrefix,
			PluginLookupGlobal:    pluginLookupGlobal,
			Insert:                config.Insert,
			IsolateJob:            config.IsolateJob,
			IsolatedInsertTimeout: config.IsolatedInsertTimeout,
			PeriodicJobs:          config.PeriodicJobs,
			Pilot:                 pilot,
			Schema:                config.Schema,
		}).mustValidate(),

		isolatedOccurrences: make(map[rivertype.PeriodicJobHandle]*periodicJobIsolatedOccurrence),
		retiredIsolatedIDs:  make(map[string]rivertype.PeriodicJobHandle),
		exec:                exec,
		nextHandle:          nextHandle,
		periodicJobIDs:      periodicJobIDs,
		periodicJobs:        periodicJobs,
		recalculateNextRun:  make(chan struct{}, 1),
	})

	return svc, nil
}

// AddSafely adds a new periodic job to the enqueuer. The service's run loop is
// woken immediately so that the job is scheduled appropriately, and inserted if
// its RunOnStart flag is set to true.
func (s *PeriodicJobEnqueuer) AddSafely(periodicJob *PeriodicJob) (rivertype.PeriodicJobHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := periodicJob.validate(); err != nil {
		return 0, err
	}

	handle := s.nextHandle

	if err := addUniqueID(s.periodicJobIDs, periodicJob.ID, handle); err != nil {
		return 0, err
	}

	s.periodicJobs[handle] = periodicJob
	s.nextHandle++

	select {
	case s.recalculateNextRun <- struct{}{}:
	default:
	}

	return handle, nil
}

// AddManySafely adds many new periodic job to the enqueuer. The service's run loop is
// woken immediately so that the job is scheduled appropriately, and inserted if
// any RunOnStart flags are set to true.
func (s *PeriodicJobEnqueuer) AddManySafely(periodicJobs []*PeriodicJob) ([]rivertype.PeriodicJobHandle, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	handles := make([]rivertype.PeriodicJobHandle, len(periodicJobs))

	for i, periodicJob := range periodicJobs {
		if err := periodicJob.validate(); err != nil {
			return nil, err
		}

		handles[i] = s.nextHandle

		if err := addUniqueID(s.periodicJobIDs, periodicJob.ID, handles[i]); err != nil {
			return nil, err
		}

		s.periodicJobs[handles[i]] = periodicJob
		s.nextHandle++
	}

	select {
	case s.recalculateNextRun <- struct{}{}:
	default:
	}

	return handles, nil
}

// Clear clears all periodic jobs from the enqueuer.
func (s *PeriodicJobEnqueuer) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for handle, periodicJob := range s.periodicJobs {
		s.removeJobLockFree(periodicJob, handle)
	}
	s.periodicJobIDs = make(map[string]rivertype.PeriodicJobHandle)

	// `nextHandle` is _not_ reset so that even across multiple generations of
	// jobs, handles aren't reused.
	s.periodicJobs = make(map[rivertype.PeriodicJobHandle]*PeriodicJob)
}

// Remove removes a periodic job from the enqueuer. Its current target run time
// and all future runs are cancelled.
func (s *PeriodicJobEnqueuer) Remove(periodicJobHandle rivertype.PeriodicJobHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if periodicJob, ok := s.periodicJobs[periodicJobHandle]; ok {
		s.removeJobLockFree(periodicJob, periodicJobHandle)
	}
}

// RemoveByID removes a periodic job from the enqueuer by ID. Its current target run
// time and all future runs are cancelled.
func (s *PeriodicJobEnqueuer) RemoveByID(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if handle, ok := s.periodicJobIDs[id]; ok {
		s.removeJobLockFree(s.periodicJobs[handle], handle)
		return true
	}

	return false
}

// RemoveMany removes many periodic jobs from the enqueuer. Their current target
// run time and all future runs are cancelled.
func (s *PeriodicJobEnqueuer) RemoveMany(periodicJobHandles []rivertype.PeriodicJobHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, periodicJobHandle := range periodicJobHandles {
		if periodicJob, ok := s.periodicJobs[periodicJobHandle]; ok {
			s.removeJobLockFree(periodicJob, periodicJobHandle)
		}
	}
}

// RemoveManyByID removes many periodic jobs from the enqueuer by ID. Their
// current target run time and all future runs are cancelled.
func (s *PeriodicJobEnqueuer) RemoveManyByID(ids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range ids {
		if handle, ok := s.periodicJobIDs[id]; ok {
			s.removeJobLockFree(s.periodicJobs[handle], handle)
		}
	}
}

func (s *PeriodicJobEnqueuer) removeJobLockFree(periodicJob *PeriodicJob, periodicJobHandle rivertype.PeriodicJobHandle) {
	delete(s.periodicJobs, periodicJobHandle)
	delete(s.periodicJobIDs, periodicJob.ID)
	if occurrence := s.isolatedOccurrences[periodicJobHandle]; occurrence != nil {
		if occurrence.cancel != nil {
			occurrence.cancel()
		}
		// A running transaction must finish before a replacement with this ID
		// initializes its durable schedule. Keep its generation as a barrier.
		if !occurrence.inFlight {
			delete(s.isolatedOccurrences, periodicJobHandle)
		} else if occurrence.id != "" {
			s.retiredIsolatedIDs[occurrence.id] = periodicJobHandle
		}
	}
}

func (s *PeriodicJobEnqueuer) Start(ctx context.Context) error {
	ctx, shouldStart, started, stopped := s.StartInit(ctx)
	if !shouldStart {
		return nil
	}

	s.StaggerStart(ctx)

	var (
		initialPeriodicJobs []*riverpilot.PeriodicJob
		subServices         []startstop.Service
	)
	if err := func() error {
		var err error
		initialPeriodicJobs, err = s.Config.Pilot.PeriodicJobGetAll(ctx, s.exec, &riverpilot.PeriodicJobGetAllParams{
			Schema: s.Config.Schema,
		})
		if err != nil {
			return err
		}

		for _, hook := range s.Config.PluginLookupGlobal.ByKind(pluginlookup.PluginKindHookPeriodicJobsStart) {
			if err := hook.(rivertype.HookPeriodicJobsStart).Start(ctx, &rivertype.HookPeriodicJobsStartParams{ //nolint:forcetypeassert
				DurableJobs: sliceutil.Map(initialPeriodicJobs, func(job *riverpilot.PeriodicJob) *rivertype.DurablePeriodicJob {
					return (*rivertype.DurablePeriodicJob)(job)
				}),
			}); err != nil {
				return err
			}
		}

		subServices = []startstop.Service{
			startstop.StartStopFunc(s.periodicJobKeepAliveAndReapPeriodically),
		}
		stopServicesOnError := func() {
			startstop.StopAllParallel(subServices...)
		}
		if err := startstop.StartAll(ctx, subServices...); err != nil {
			stopServicesOnError()
			return err
		}

		return nil
	}(); err != nil {
		stopped()
		return err
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStarted)
		defer s.Logger.DebugContext(ctx, s.Name+riversharedmaintenance.LogPrefixRunLoopStopped)

		defer startstop.StopAllParallel(subServices...)

		// Drain the signal to recalculate next run if it's been sent (i.e. Add
		// or AddMany called before Start). We're about to schedule jobs from
		// scratch, and therefore don't need to immediately do so again.
		select {
		case <-s.recalculateNextRun:
		default:
		}

		// Initial set of periodic job IDs mapped to next run at times fetched
		// from a configured pilot. Not used in most cases.
		initialPeriodicJobsMap := sliceutil.KeyBy(initialPeriodicJobs,
			func(j *riverpilot.PeriodicJob) (string, time.Time) { return j.ID, j.NextRunAt })

		workerCtx, cancelWorker := context.WithCancel(ctx)
		work := make(chan *periodicJobIsolatedOccurrence, 1)
		results := make(chan periodicJobIsolatedResult, 2)
		workerDone := make(chan struct{})
		go s.runIsolatedWorker(workerCtx, work, results, workerDone)
		defer s.finishIsolatedWorker(ctx, cancelWorker, results, workerDone)

		initializedHandles := make(map[rivertype.PeriodicJobHandle]bool)
		var lastDispatchedHandle rivertype.PeriodicJobHandle = -1

		validateInsertRunOnStartAndScheduleNewlyAdded := func() {
			var (
				insertParamsMany        []*rivertype.JobInsertParams
				now                     = s.Time.Now()
				periodicJobUpsertParams = &riverpilot.PeriodicJobUpsertManyParams{Schema: s.Config.Schema}
			)
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				for handle := range initializedHandles {
					if s.periodicJobs[handle] == nil {
						delete(initializedHandles, handle)
					}
				}
				sortedHandles := maputil.Keys(s.periodicJobs)
				slices.Sort(sortedHandles)
				for _, handle := range sortedHandles {
					if initializedHandles[handle] {
						continue
					}
					periodicJob := s.periodicJobs[handle].mustValidate()
					if s.hasRetiredIsolatedIDLockFree(periodicJob.ID) {
						continue
					}
					initializedHandles[handle] = true
					if occurrence := s.isolatedOccurrences[handle]; occurrence != nil {
						// Another leader may have committed this tick while this
						// instance was stopped. Never rewind its durable progress.
						// Equality does not prove a RunOnStart occurrence committed.
						if nextRunAt, ok := initialPeriodicJobsMap[periodicJob.ID]; !occurrence.frozen && ok &&
							nextRunAt.After(occurrence.previousNextRunAt) {
							periodicJob.nextRunAt = nextRunAt
							delete(s.isolatedOccurrences, handle)
						}
						// Resume pending or frozen work without reconstructing it.
						delete(initialPeriodicJobsMap, periodicJob.ID)
						continue
					}
					if nextRunAt, ok := initialPeriodicJobsMap[periodicJob.ID]; periodicJob.ID != "" && ok {
						periodicJob.nextRunAt = nextRunAt
						delete(initialPeriodicJobsMap, periodicJob.ID)
					} else {
						periodicJob.nextRunAt = periodicJob.ScheduleFunc(now)
					}
					if periodicJob.RunOnStart {
						if params, ok := s.insertParamsFromConstructor(ctx, periodicJob.ID, periodicJob.ConstructorFunc, now); ok {
							if s.stageIsolatedOccurrenceLockFree(handle, periodicJob, params, now, periodicJob.nextRunAt) {
								continue
							}
							insertParamsMany = append(insertParamsMany, params)
						}
					}
					s.appendPeriodicUpsert(periodicJobUpsertParams, periodicJob)
				}
			}()
			s.insertBatch(ctx, insertParamsMany, periodicJobUpsertParams)
			if len(insertParamsMany) > 0 {
				s.Logger.DebugContext(ctx, s.Name+": Inserted RunOnStart jobs", "num_jobs", len(insertParamsMany))
			}
		}

		validateInsertRunOnStartAndScheduleNewlyAdded()
		s.dispatchIsolatedOccurrences(workerCtx, work, &lastDispatchedHandle)
		s.TestSignals.EnteredLoop.Signal(struct{}{})

		timerUntilNextRun := time.NewTimer(s.timeUntilNextRun())
		defer timerUntilNextRun.Stop()
		for {
			select {
			case <-timerUntilNextRun.C:
				var (
					insertParamsMany        []*rivertype.JobInsertParams
					periodicJobUpsertParams = &riverpilot.PeriodicJobUpsertManyParams{Schema: s.Config.Schema}
				)
				// Preserve the ordinary scheduler's small preemption margin.
				nowWithMargin := s.Time.Now().Add(100 * time.Millisecond)
				func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					for handle, periodicJob := range s.periodicJobs {
						if !initializedHandles[handle] || s.isolatedOccurrences[handle] != nil ||
							periodicJob.nextRunAt.IsZero() || !periodicJob.nextRunAt.Before(nowWithMargin) {
							continue
						}
						scheduledAt := periodicJob.nextRunAt
						nextRunAt := periodicJob.ScheduleFunc(scheduledAt)
						if params, ok := s.insertParamsFromConstructor(ctx, periodicJob.ID, periodicJob.ConstructorFunc, scheduledAt); ok {
							if s.stageIsolatedOccurrenceLockFree(handle, periodicJob, params, scheduledAt, nextRunAt) {
								continue
							}
							insertParamsMany = append(insertParamsMany, params)
						}
						// Ordinary jobs retain their original best-effort advancement.
						periodicJob.nextRunAt = nextRunAt
						s.appendPeriodicUpsert(periodicJobUpsertParams, periodicJob)
					}
				}()
				s.insertBatch(ctx, insertParamsMany, periodicJobUpsertParams)
			case result := <-results:
				s.completeIsolatedOccurrence(ctx, result)
			case <-s.recalculateNextRun:
			case <-ctx.Done():
				return
			}

			// A timer may still be armed when a completion or registration wakes
			// the loop. Drain it without blocking before scheduling the next tick.
			if !timerUntilNextRun.Stop() {
				select {
				case <-timerUntilNextRun.C:
				default:
				}
			}
			validateInsertRunOnStartAndScheduleNewlyAdded()
			s.dispatchIsolatedOccurrences(workerCtx, work, &lastDispatchedHandle)
			timerUntilNextRun.Reset(s.timeUntilNextRun())
		}
	}()

	return nil
}

// periodicJobIsolatedOccurrence owns one constructor result until it commits.
// All mutable fields are protected by the enqueuer mutex; the worker receives
// an immutable snapshot and reports completion to the scheduling loop.
type periodicJobIsolatedOccurrence struct {
	attempts          int
	cancel            context.CancelFunc
	ctx               context.Context //nolint:containedctx
	frozen            bool
	handle            rivertype.PeriodicJobHandle
	id                string
	inFlight          bool
	nextRunAt         time.Time
	params            *rivertype.JobInsertParams
	previousNextRunAt time.Time
	retryNotBefore    time.Time
	scheduledAt       time.Time
}

// periodicJobIsolatedResult distinguishes a pre-commit failure from an uncertain
// commit outcome that must not be replayed automatically.
type periodicJobIsolatedResult struct {
	err        error
	occurrence *periodicJobIsolatedOccurrence
	retryable  bool
}

// appendPeriodicUpsert records the current schedule for an identified job.
func (s *PeriodicJobEnqueuer) appendPeriodicUpsert(params *riverpilot.PeriodicJobUpsertManyParams, job *PeriodicJob) {
	if job.ID != "" {
		params.Jobs = append(params.Jobs, &riverpilot.PeriodicJobUpsertParams{
			ID: job.ID, NextRunAt: job.nextRunAt, UpdatedAt: s.Time.Now(),
		})
	}
}

// completeIsolatedOccurrence advances only the registration that dispatched
// this occurrence, or records bounded retry backoff and an uncertain outcome.
func (s *PeriodicJobEnqueuer) completeIsolatedOccurrence(ctx context.Context, result periodicJobIsolatedResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	occurrence := result.occurrence
	if s.isolatedOccurrences[occurrence.handle] != occurrence {
		return
	}
	if occurrence.cancel != nil {
		occurrence.cancel()
	}
	occurrence.inFlight = false
	job := s.periodicJobs[occurrence.handle]
	if job == nil {
		delete(s.isolatedOccurrences, occurrence.handle)
		delete(s.retiredIsolatedIDs, occurrence.id)
		if result.err != nil && !result.retryable {
			s.Logger.ErrorContext(ctx, s.Name+": Removed periodic job has an uncertain transaction outcome",
				"error", result.err, "periodic_job_handle", occurrence.handle, "periodic_job_id", occurrence.id,
				"scheduled_at", occurrence.scheduledAt)
		}
		return
	}
	if result.err == nil {
		job.nextRunAt = occurrence.nextRunAt
		delete(s.isolatedOccurrences, occurrence.handle)
		return
	}
	if result.retryable {
		occurrence.attempts++
		backoff := 100 * time.Millisecond << min(occurrence.attempts-1, 8)
		occurrence.retryNotBefore = s.Time.Now().Add(backoff)
		s.Logger.ErrorContext(ctx, s.Name+": Isolated periodic job failed before commit; retry scheduled",
			"error", result.err, "periodic_job_handle", occurrence.handle, "periodic_job_id", occurrence.id,
			"scheduled_at", occurrence.scheduledAt, "retry_not_before", occurrence.retryNotBefore)
	} else {
		occurrence.frozen = true
		s.Logger.ErrorContext(ctx, s.Name+": Isolated periodic outcome uncertain; schedule paused for reconciliation",
			"error", result.err, "periodic_job_handle", occurrence.handle, "periodic_job_id", occurrence.id,
			"scheduled_at", occurrence.scheduledAt)
	}
}

// dispatchIsolatedOccurrences offers work without blocking ordinary scheduling.
// A rotating handle order prevents a frequently failing job starving its peers.
func (s *PeriodicJobEnqueuer) dispatchIsolatedOccurrences(ctx context.Context, work chan<- *periodicJobIsolatedOccurrence, lastHandle *rivertype.PeriodicJobHandle) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ctx.Err() != nil {
		return
	}
	handles := maputil.Keys(s.isolatedOccurrences)
	slices.Sort(handles)
	start := 0
	for start < len(handles) && handles[start] <= *lastHandle {
		start++
	}
	now := s.Time.Now()
	for i := range handles {
		handle := handles[(start+i)%len(handles)]
		occurrence := s.isolatedOccurrences[handle]
		if s.periodicJobs[handle] == nil || occurrence.inFlight || occurrence.frozen || occurrence.retryNotBefore.After(now) {
			continue
		}
		occurrence.ctx, occurrence.cancel = context.WithCancel(ctx) //nolint:fatcontext // Each occurrence gets an independent child of the unchanged worker context.
		select {
		case work <- occurrence:
			occurrence.inFlight = true
			*lastHandle = handle
		default:
			occurrence.cancel()
			// Completion wakes the scheduler sooner when possible. This timer
			// also covers a worker that has not received its first item yet.
			occurrence.retryNotBefore = now.Add(100 * time.Millisecond)
		}
	}
}

// finishIsolatedWorker cancels and joins the worker while retaining every
// transaction outcome. It also releases queued work for a later service start.
func (s *PeriodicJobEnqueuer) finishIsolatedWorker(ctx context.Context, cancelWorker context.CancelFunc, results <-chan periodicJobIsolatedResult, workerDone <-chan struct{}) {
	cancelWorker()
	// Consume results while joining: a worker publishing its final
	// outcome must never wait on a scheduler that is waiting for it.
	for workerDone != nil {
		select {
		case result := <-results:
			s.completeIsolatedOccurrence(ctx, result)
		case <-workerDone:
			workerDone = nil
		}
	}
	for len(results) > 0 {
		s.completeIsolatedOccurrence(ctx, <-results)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for handle, occurrence := range s.isolatedOccurrences {
		if occurrence.cancel != nil {
			occurrence.cancel()
		}
		occurrence.inFlight = false
		if s.periodicJobs[handle] == nil {
			delete(s.isolatedOccurrences, handle)
			delete(s.retiredIsolatedIDs, occurrence.id)
		}
	}
}

// hasRetiredIsolatedIDLockFree fences reuse of a durable ID until its removed
// generation's transaction has finished. It requires the enqueuer mutex.
func (s *PeriodicJobEnqueuer) hasRetiredIsolatedIDLockFree(id string) bool {
	_, retired := s.retiredIsolatedIDs[id]
	return id != "" && retired
}

// insertIsolatedOccurrence commits a job and its durable schedule together.
// The worker owns the outer transaction. Before Commit is called, an insertion
// cannot persist, even if cancellation closed the connection before Rollback.
// Insert and Pilot callbacks must leave transaction ownership with the worker.
func (s *PeriodicJobEnqueuer) insertIsolatedOccurrence(ctx context.Context, occurrence *periodicJobIsolatedOccurrence) periodicJobIsolatedResult {
	result := periodicJobIsolatedResult{occurrence: occurrence}
	timeout := s.Config.IsolatedInsertTimeout
	if timeout <= 0 {
		timeout = riversharedmaintenance.TimeoutDefault
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		result.err, result.retryable = err, true
		return result
	}
	tx, err := s.exec.Begin(ctx)
	if err != nil {
		result.err, result.retryable = fmt.Errorf("begin isolated periodic transaction: %w", err), true
		return result
	}
	rollback := func(err error) periodicJobIsolatedResult {
		result.err, result.retryable = err, true
		if rollbackErr := dbutil.RollbackWithoutCancel(ctx, tx); rollbackErr != nil {
			// No Commit was sent. Drivers may already have rolled back or
			// closed a connection on cancellation; cleanup errors are logged
			// without turning a recoverable timeout into a frozen schedule.
			result.err = errors.Join(err, fmt.Errorf("rollback isolated periodic transaction: %w", rollbackErr))
		}
		return result
	}
	params := clonePeriodicInsertParams(occurrence.params)
	if _, err := s.Config.Insert(ctx, tx, []*rivertype.JobInsertParams{params}); err != nil {
		return rollback(fmt.Errorf("insert isolated periodic job: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return rollback(err)
	}
	if occurrence.id != "" {
		if _, err := s.Config.Pilot.PeriodicJobUpsertMany(ctx, tx, &riverpilot.PeriodicJobUpsertManyParams{
			Schema: s.Config.Schema,
			Jobs:   []*riverpilot.PeriodicJobUpsertParams{{ID: occurrence.id, NextRunAt: occurrence.nextRunAt, UpdatedAt: s.Time.Now()}},
		}); err != nil {
			return rollback(fmt.Errorf("upsert isolated periodic schedule: %w", err))
		}
	}
	if err := ctx.Err(); err != nil {
		return rollback(err)
	}
	if err := tx.Commit(ctx); err != nil {
		// A driver may have committed before losing the connection. Cleanup
		// cannot establish whether replay would duplicate this occurrence.
		result.err = fmt.Errorf("commit isolated periodic transaction: %w", err)
		_ = dbutil.RollbackWithoutCancel(ctx, tx)
		return result
	}
	s.TestSignals.InsertedJobs.Signal(struct{}{})
	if occurrence.id != "" {
		s.TestSignals.PeriodicJobUpserted.Signal(struct{}{})
	}
	return result
}

// runIsolatedWorker bounds isolated work to one active transaction and one
// queued occurrence. No registry lock is held across database operations.
func (s *PeriodicJobEnqueuer) runIsolatedWorker(ctx context.Context, work <-chan *periodicJobIsolatedOccurrence, results chan<- periodicJobIsolatedResult, done chan<- struct{}) {
	defer close(done)
	for {
		select {
		case <-ctx.Done():
			return
		case occurrence := <-work:
			result := s.insertIsolatedOccurrence(occurrence.ctx, occurrence) //nolint:contextcheck // Dispatch derives this context from the worker context and adds removal cancellation.
			// Shutdown keeps draining this channel until the worker exits, so
			// the final commit outcome is retained even when the buffer is full.
			results <- result
		}
	}
}

// stageIsolatedOccurrenceLockFree retains one occurrence per registration. It
// requires the enqueuer mutex and does not advance the in-memory schedule.
func (s *PeriodicJobEnqueuer) stageIsolatedOccurrenceLockFree(handle rivertype.PeriodicJobHandle, job *PeriodicJob, params *rivertype.JobInsertParams, scheduledAt, nextRunAt time.Time) bool {
	if s.Config.IsolateJob == nil || !s.Config.IsolateJob(params) {
		return false
	}
	s.isolatedOccurrences[handle] = &periodicJobIsolatedOccurrence{
		handle: handle, id: job.ID, params: clonePeriodicInsertParams(params),
		scheduledAt: scheduledAt, nextRunAt: nextRunAt, previousNextRunAt: job.nextRunAt,
	}
	return true
}

// insertBatch preserves the original transaction path for ordinary jobs and
// schedule updates without occurrences. Isolated jobs are staged separately.
func (s *PeriodicJobEnqueuer) insertBatch(ctx context.Context, insertParamsMany []*rivertype.JobInsertParams, periodicJobUpsertParams *riverpilot.PeriodicJobUpsertManyParams) {
	if len(insertParamsMany) < 1 && len(periodicJobUpsertParams.Jobs) < 1 {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, riversharedmaintenance.TimeoutDefault)
	defer cancel()

	tx, err := s.exec.Begin(ctx)
	if err != nil {
		s.Logger.ErrorContext(ctx, s.Name+": Error starting transaction", "error", err.Error())
		return
	}
	defer dbutil.RollbackWithoutCancel(ctx, tx)

	if len(insertParamsMany) > 0 {
		if _, err := s.Config.Insert(ctx, tx, insertParamsMany); err != nil {
			s.Logger.ErrorContext(ctx, s.Name+": Error inserting periodic jobs",
				"error", err.Error(), "num_jobs", len(insertParamsMany))
			return
		}
	}

	if len(periodicJobUpsertParams.Jobs) > 0 {
		if _, err = s.Config.Pilot.PeriodicJobUpsertMany(ctx, tx, periodicJobUpsertParams); err != nil {
			s.Logger.ErrorContext(ctx, s.Name+": Error upserting periodic job next run times",
				"error", err.Error(), "num_jobs", len(insertParamsMany), "num_next_run_at_upserts", len(periodicJobUpsertParams.Jobs))
			return
		}
	}

	if err := tx.Commit(ctx); err != nil {
		s.Logger.ErrorContext(ctx, s.Name+": Error committing transaction", "error", err.Error())
		return
	}

	if len(insertParamsMany) > 0 {
		s.TestSignals.InsertedJobs.Signal(struct{}{})
	}
	if len(periodicJobUpsertParams.Jobs) > 0 {
		s.TestSignals.PeriodicJobUpserted.Signal(struct{}{})
	}
}

func (s *PeriodicJobEnqueuer) insertParamsFromConstructor(ctx context.Context, periodicJobID string, constructorFunc func() (*rivertype.JobInsertParams, error), scheduledAt time.Time) (*rivertype.JobInsertParams, bool) {
	insertParams, err := constructorFunc()
	if err != nil {
		if errors.Is(err, ErrNoJobToInsert) {
			s.Logger.InfoContext(ctx, s.Name+": nil returned from periodic job constructor, skipping")
			s.TestSignals.SkippedJob.Signal(struct{}{})
			return nil, false
		}
		s.Logger.ErrorContext(ctx, s.Name+": Internal error generating periodic job", "error", err.Error())
		return nil, false
	}

	if insertParams.ScheduledAt == nil {
		insertParams.ScheduledAt = &scheduledAt
	}

	if periodicJobID != "" {
		var err error
		if insertParams.Metadata, err = sjson.SetBytes(insertParams.Metadata, rivercommon.MetadataKeyPeriodicJobID, periodicJobID); err != nil {
			s.Logger.ErrorContext(ctx, s.Name+": Error setting periodic metadata", "error", err.Error())
		}
	}

	if insertParams.Metadata, err = sjson.SetBytes(insertParams.Metadata, "periodic", true); err != nil {
		s.Logger.ErrorContext(ctx, s.Name+": Error setting periodic metadata", "error", err.Error())
		return nil, false
	}

	return insertParams, true
}

func (s *PeriodicJobEnqueuer) periodicJobKeepAliveAndReapPeriodically(ctx context.Context, shouldStart bool, started, stopped func()) error {
	if !shouldStart {
		return nil
	}

	go func() {
		started()
		defer stopped() // this defer should come first so it's last out

		ticker := timeutil.NewTickerWithInitialTick(ctx, 10*time.Minute)
		for {
			select {
			case <-ctx.Done():
				return

			case <-ticker.C:
				func() {
					s.mu.RLock()
					ids := maputil.Keys(s.periodicJobIDs)
					s.mu.RUnlock()

					if len(ids) > 0 {
						if _, err := s.Config.Pilot.PeriodicJobKeepAliveAndReap(ctx, s.exec, &riverpilot.PeriodicJobKeepAliveAndReapParams{
							ID:     ids,
							Schema: s.Config.Schema,
						}); err != nil {
							s.Logger.ErrorContext(ctx, s.Name+": Error executing periodic job keep alive and reap", "error", err.Error())
							return
						}
					}

					s.TestSignals.PeriodicJobKeepAliveAndReap.Signal(struct{}{})
				}()
			}
		}
	}()

	return nil
}

const periodicJobEnqueuerVeryLongDuration = 24 * time.Hour

func (s *PeriodicJobEnqueuer) timeUntilNextRun() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// With no configured jobs, just return a big duration for the loop to block
	// on.
	if len(s.periodicJobs) < 1 {
		return periodicJobEnqueuerVeryLongDuration
	}

	var (
		firstNextRunAt time.Time
		now            = s.Time.Now()
	)

	for handle, periodicJob := range s.periodicJobs {
		if s.hasRetiredIsolatedIDLockFree(periodicJob.ID) {
			continue
		}
		nextRunAt := periodicJob.nextRunAt
		if occurrence := s.isolatedOccurrences[handle]; occurrence != nil {
			if occurrence.inFlight || occurrence.frozen {
				continue
			}
			nextRunAt = occurrence.retryNotBefore
			if nextRunAt.IsZero() {
				return 0
			}
		}
		// Jobs may have been added after service start, but before this
		// function runs for the first time. They're not scheduled properly yet,
		// but they will be soon, at which point this function will run again.
		// Skip them for now.
		if nextRunAt.IsZero() {
			continue
		}

		// In case we detect a job that should've run before now, immediately short
		// circuit with a 0 duration. This avoids needlessly iterating through the
		// rest of the loop when we already know we're overdue for the next job.
		if nextRunAt.Before(now) {
			return 0
		}

		if firstNextRunAt.IsZero() || nextRunAt.Before(firstNextRunAt) {
			firstNextRunAt = nextRunAt
		}
	}

	// Only encountered unscheduled jobs (see comment above). Don't schedule
	// anything for now.
	if firstNextRunAt.IsZero() {
		return periodicJobEnqueuerVeryLongDuration
	}

	return firstNextRunAt.Sub(now)
}

// Adds a unique ID to known periodic job IDs, erroring in case of a duplicate.
func addUniqueID(periodicJobIDs map[string]rivertype.PeriodicJobHandle, id string, handle rivertype.PeriodicJobHandle) error {
	if id == "" {
		return nil
	}

	if _, ok := periodicJobIDs[id]; ok {
		return errors.New("periodic job with ID already registered: " + id)
	}

	periodicJobIDs[id] = handle
	return nil
}

// clonePeriodicInsertParams isolates mutable insertion fields between attempts.
// Args retains its application-defined concrete value for hook and sequence
// interfaces. Hooks should replace Params.Args rather than mutate that value.
func clonePeriodicInsertParams(params *rivertype.JobInsertParams) *rivertype.JobInsertParams {
	clone := *params
	clone.EncodedArgs = slices.Clone(params.EncodedArgs)
	clone.Metadata = slices.Clone(params.Metadata)
	clone.Tags = slices.Clone(params.Tags)
	clone.UniqueKey = slices.Clone(params.UniqueKey)
	if params.ID != nil {
		id := *params.ID
		clone.ID = &id
	}
	if params.CreatedAt != nil {
		createdAt := *params.CreatedAt
		clone.CreatedAt = &createdAt
	}
	if params.ScheduledAt != nil {
		scheduledAt := *params.ScheduledAt
		clone.ScheduledAt = &scheduledAt
	}
	return &clone
}
