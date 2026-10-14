package joblease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/riverdriver"
	"github.com/riverqueue/river/rivershared/riversharedtest"
	"github.com/riverqueue/river/rivertype"
)

func TestManager(t *testing.T) {
	t.Parallel()

	t.Run("AbandonedMemberDoesNotCancelOtherRenewals", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			var lostGuard *Guard
			workCtx, cancelWork := context.WithCancelCause(context.Background())
			first := true
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(ctx context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				if first {
					first = false
					require.ElementsMatch(t, []int64{1, 2}, params.IDs)
					// A exits while the shared request is in flight. Its late confirmation
					// must not revive it or prevent B from receiving confirmation.
					lostGuard.Abandon()
					lostGuard.Finalize(config.FinalizeTimeout)
					synctest.Wait()
					require.ErrorIs(t, context.Cause(workCtx), rivertype.ErrJobLeaseLost)
				} else {
					require.Equal(t, []int64{2}, params.IDs)
				}
				require.NoError(t, ctx.Err())
				rows := make([]*rivertype.JobRow, 0, len(params.IDs))
				for _, id := range params.IDs {
					rows = append(rows, &rivertype.JobRow{ID: id, LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))})
				}
				return rows, nil
			})
			lostGuard = manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), cancelWork)
			defer lostGuard.Abandon()
			defer lostGuard.Finalize(config.FinalizeTimeout)
			liveGuard := manager.Register(context.Background(), &rivertype.JobRow{ID: 2, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			defer liveGuard.Abandon()
			defer liveGuard.Finalize(config.FinalizeTimeout)
			lostDeadline := lostGuard.Deadline()
			for range 6 {
				time.Sleep(config.HeartbeatInterval)
				require.NoError(t, manager.renew(context.Background()))
				require.NoError(t, liveGuard.Check())
				require.ErrorIs(t, lostGuard.Check(), rivertype.ErrJobLeaseLost)
				require.Equal(t, lostDeadline, lostGuard.Deadline())
				require.True(t, manager.CanFetch())
			}
			liveGuard.Finalize(config.FinalizeTimeout)
			require.True(t, liveGuard.Finish())
			synctest.Wait()
			require.Empty(t, manager.entries)
		})
	})

	t.Run("CommittedRenewalWithLostResponseCanRetry", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			claimedAt := time.Now()
			expiresAt := claimedAt.Add(config.LeaseDuration)
			calls := 0
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(_ context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				require.Equal(t, []int64{1}, params.IDs)
				require.Equal(t, []time.Time{claimedAt}, params.AttemptedAt)
				require.Equal(t, []int{2}, params.Attempts)
				require.True(t, expiresAt.After(time.Now()))
				// The database commits both requests; only the first response is lost.
				expiresAt = time.Now().Add(config.LeaseDuration)
				calls++
				if calls == 1 {
					return nil, errors.New("response lost after commit")
				}
				return []*rivertype.JobRow{{ID: 1, LeaseExpiresAt: new(expiresAt)}}, nil
			})
			guard := manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 2, AttemptedAt: &claimedAt, LeaseExpiresAt: new(expiresAt)}, time.Now(), nil)
			defer guard.Abandon()
			originalDeadline := guard.Deadline()
			time.Sleep(config.HeartbeatInterval)
			require.ErrorContains(t, manager.renew(context.Background()), "response lost")
			require.Equal(t, originalDeadline, guard.Deadline())
			require.False(t, manager.CanFetch())
			time.Sleep(time.Second)
			require.NoError(t, manager.renew(context.Background()))
			require.True(t, guard.Deadline().After(originalDeadline))
			require.True(t, manager.CanFetch())
			time.Sleep(time.Until(originalDeadline) + time.Second)
			require.NoError(t, guard.Check())
			guard.Finalize(config.FinalizeTimeout)
			require.True(t, guard.Finish())
			synctest.Wait()
		})
	})

	t.Run("FailedRenewalDoesNotExtendDeadline", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(context.Context, *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				return nil, errors.New("database unavailable")
			})
			ctx, cancel := context.WithCancelCause(context.Background())
			guard := manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), cancel)
			defer guard.Abandon()
			deadline := guard.Deadline()
			for range 3 {
				time.Sleep(20 * time.Second)
				require.Error(t, manager.renew(context.Background()))
			}
			require.Equal(t, deadline, guard.Deadline())
			require.NoError(t, guard.Check(), "three failures are not the loss threshold")
			time.Sleep(40 * time.Second)
			synctest.Wait()
			require.ErrorIs(t, context.Cause(ctx), rivertype.ErrJobLeaseLost)
			guard.Finalize(config.FinalizeTimeout)
			synctest.Wait()
		})
	})

	t.Run("FinalizerFailuresDoNotCancelHealthyRenewals", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			successes, failures := 0, 0
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(ctx context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				// Renewal is available; only result confirmations are stalled.
				time.Sleep(2 * time.Second)
				if ctx.Err() != nil {
					failures++
					return nil, ctx.Err()
				}
				successes++
				rows := make([]*rivertype.JobRow, 0, len(params.IDs))
				for _, id := range params.IDs {
					rows = append(rows, &rivertype.JobRow{ID: id, LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))})
				}
				return rows, nil
			})
			require.NoError(t, manager.Start(context.Background()))
			defer manager.Stop()
			liveGuard := manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			defer liveGuard.Abandon()
			defer liveGuard.Finalize(config.FinalizeTimeout)
			finalizers := make([]*Guard, 0, 101)
			for i := range 101 {
				guard := manager.Register(context.Background(), &rivertype.JobRow{ID: int64(i + 2), Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
				finalizers = append(finalizers, guard)
				go func() {
					time.Sleep(time.Duration(i) * time.Second)
					guard.Finalize(config.FinalizeTimeout)
				}()
			}
			// Each finalizer eventually loses its lease, across many renewal batches.
			// The live execution must survive beyond its original local deadline.
			time.Sleep(141 * time.Second)
			synctest.Wait()
			for _, guard := range finalizers {
				require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
			}
			require.Positive(t, successes)
			require.Zero(t, failures)
			require.NoError(t, liveGuard.Check())
			require.True(t, manager.CanFetch())
			liveGuard.Finalize(config.FinalizeTimeout)
			require.True(t, liveGuard.Finish())
			synctest.Wait()
			require.Empty(t, manager.entries)
		})
	})

	t.Run("HealthyLeaseSurvivesOriginalDeadline", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(_ context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				return []*rivertype.JobRow{{ID: params.IDs[0], LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}}, nil
			})
			guard := manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			defer guard.Abandon()
			for range 12 {
				time.Sleep(20 * time.Second)
				require.NoError(t, manager.renew(context.Background()))
				require.NoError(t, guard.Check())
			}
			guard.Finalize(config.FinalizeTimeout)
			require.True(t, guard.Finish())
			synctest.Wait()
			require.Empty(t, manager.entries)
		})
	})

	t.Run("LateSuccessfulRenewalCannotRevive", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			var requestCancelled bool
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(ctx context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				// Model a committed renewal whose response ignores cancellation.
				time.Sleep(110 * time.Second)
				requestCancelled = ctx.Err() != nil
				return []*rivertype.JobRow{{ID: params.IDs[0], LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}}, nil
			})
			guard := manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			defer guard.Abandon()
			require.NoError(t, manager.renew(context.Background()))
			require.True(t, requestCancelled)
			require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
			guard.Finalize(config.FinalizeTimeout)
			synctest.Wait()
		})
	})

	t.Run("MissingClaimRecordNeverStartsWork", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", nil)
			guard := manager.Register(context.Background(), &rivertype.JobRow{ID: 1, LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
			guard.Finalize(config.FinalizeTimeout)
			synctest.Wait()
			require.Empty(t, manager.entries)
		})
	})

	t.Run("NormalCompletionDoesNotCancelOtherRenewals", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			var doneGuard *Guard
			first := true
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(ctx context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				if first {
					first = false
					require.ElementsMatch(t, []int64{1, 2}, params.IDs)
					// A's completion is confirmed while B's renewal awaits its response.
					doneGuard.Finalize(config.FinalizeTimeout)
					require.True(t, doneGuard.Finish())
					synctest.Wait()
				} else {
					require.Equal(t, []int64{2}, params.IDs)
				}
				require.NoError(t, ctx.Err())
				return []*rivertype.JobRow{{ID: 2, LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}}, nil
			})
			doneGuard = manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			defer doneGuard.Abandon()
			liveGuard := manager.Register(context.Background(), &rivertype.JobRow{ID: 2, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			defer liveGuard.Abandon()
			for range 6 {
				time.Sleep(config.HeartbeatInterval)
				require.NoError(t, manager.renew(context.Background()))
				require.NoError(t, liveGuard.Check())
				require.True(t, manager.CanFetch())
			}
			liveGuard.Finalize(config.FinalizeTimeout)
			require.True(t, liveGuard.Finish())
			synctest.Wait()
			require.Empty(t, manager.entries)
		})
	})

	t.Run("ParentCancellationCancelsBatch", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(ctx context.Context, _ *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				cancel()
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				return nil, ctx.Err()
			})
			guard := manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			defer guard.Abandon()
			defer guard.Finalize(config.FinalizeTimeout)
			require.ErrorIs(t, manager.renew(ctx), context.Canceled)
		})
	})

	t.Run("RenewTimeoutCancelsBatch", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(ctx context.Context, _ *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			})
			guard := manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			defer guard.Abandon()
			defer guard.Finalize(config.FinalizeTimeout)
			deadline := guard.Deadline()
			startedAt := time.Now()
			require.ErrorIs(t, manager.renew(context.Background()), context.DeadlineExceeded)
			require.Equal(t, config.RenewTimeout, time.Since(startedAt))
			require.Equal(t, deadline, guard.Deadline())
			require.NoError(t, guard.Check())
			require.False(t, manager.CanFetch())
		})
	})

	t.Run("UncooperativeWorkStopsAcquisition", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", nil)
			guard := manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			guard.Abandon()
			synctest.Wait()
			time.Sleep(config.CancelGracePeriod)
			synctest.Wait()
			require.False(t, manager.CanFetch())
			guard.Finalize(config.FinalizeTimeout)
		})
	})
}

// TestManagerClaimOwnership exercises overlapping executions of a reusable job ID.
func TestManagerClaimOwnership(t *testing.T) {
	t.Parallel()

	type testBundle struct {
		claims  map[int64]*rivertype.JobRow
		config  *Config
		manager *Manager
	}
	setup := func(t *testing.T) *testBundle {
		t.Helper()

		config := (&Config{}).WithDefaults()
		claims := make(map[int64]*rivertype.JobRow)
		manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(_ context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
			rows := make([]*rivertype.JobRow, 0, len(params.IDs))
			for i, id := range params.IDs {
				if claim := claims[id]; claim != nil && (claim.Attempt != params.Attempts[i] || !claim.AttemptedAt.Equal(params.AttemptedAt[i])) {
					continue
				}
				// Match the driver's sparse renewal receipts.
				rows = append(rows, &rivertype.JobRow{ID: id, LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))})
			}
			return rows, nil
		})
		return &testBundle{claims: claims, config: config, manager: manager}
	}

	t.Run("ChangedLocalClaimRequiresFreshConfirmation", func(t *testing.T) {
		t.Parallel()

		for _, incomingCurrent := range []bool{false, true} {
			name := "DelayedStaleReply"
			if incomingCurrent {
				name = "CurrentReplyAfterOlderReply"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				synctest.Test(t, func(t *testing.T) {
					bundle := setup(t)
					claimedAt := time.Now()
					old := bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: &claimedAt, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), nil)
					defer old.Abandon()
					defer old.Finalize(bundle.config.FinalizeTimeout)
					incomingAttempt, competingAttempt := 2, 3
					if incomingCurrent {
						incomingAttempt, competingAttempt = 3, 2
					}
					var competing *Guard
					calls := 0
					bundle.manager.renewFunc = func(ctx context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
						if params.Attempts[0] == incomingAttempt {
							calls++
							if calls == 1 {
								// Another registration installs its guard while this
								// previously committed confirmation is delayed.
								competing = bundle.manager.Register(ctx, &rivertype.JobRow{ID: 1, Attempt: competingAttempt, AttemptedAt: new(claimedAt.Add(time.Duration(competingAttempt) * time.Second)), LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), nil)
							} else if !incomingCurrent {
								return nil, nil
							}
						}
						return []*rivertype.JobRow{{ID: 1, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}}, nil
					}
					incoming := bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: incomingAttempt, AttemptedAt: new(claimedAt.Add(time.Duration(incomingAttempt) * time.Second)), LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), nil)
					defer incoming.Abandon()
					defer incoming.Finalize(bundle.config.FinalizeTimeout)
					defer competing.Abandon()
					defer competing.Finalize(bundle.config.FinalizeTimeout)
					require.Equal(t, 2, calls, "a changed local claim requires another database confirmation")
					require.ErrorIs(t, old.Check(), rivertype.ErrJobLeaseLost)
					if incomingCurrent {
						require.NoError(t, incoming.Check())
						require.ErrorIs(t, competing.Check(), rivertype.ErrJobLeaseLost)
					} else {
						require.ErrorIs(t, incoming.Check(), rivertype.ErrJobLeaseLost)
						require.NoError(t, competing.Check())
					}
				})
			})
		}
	})

	t.Run("ConcurrentRegistrationKeepsDatabaseClaim", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			bundle := setup(t)
			claimedAt := time.Now()
			guards := make([]*Guard, 8)
			bundle.claims[1] = &rivertype.JobRow{ID: 1, Attempt: len(guards), AttemptedAt: new(claimedAt.Add(time.Duration(len(guards)-1) * time.Microsecond))}
			var waitGroup sync.WaitGroup
			for i := range guards {
				waitGroup.Go(func() {
					guards[i] = bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: i + 1, AttemptedAt: new(claimedAt.Add(time.Duration(i) * time.Microsecond)), LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), nil)
				})
			}
			waitGroup.Wait()
			defer func() {
				for _, guard := range guards {
					guard.Finalize(bundle.config.FinalizeTimeout)
					guard.Abandon()
				}
			}()
			synctest.Wait()
			for _, guard := range guards[:len(guards)-1] {
				require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
				guard.Finalize(bundle.config.FinalizeTimeout)
			}
			current := guards[len(guards)-1]
			require.NoError(t, current.Check())
			synctest.Wait()
			require.Len(t, bundle.manager.entries, 1)
			deadline := current.Deadline()
			time.Sleep(bundle.config.HeartbeatInterval)
			require.NoError(t, bundle.manager.renew(context.Background()))
			require.True(t, current.Deadline().After(deadline))
			require.True(t, bundle.manager.CanFetch())
		})
	})

	t.Run("ConfirmationFailurePreservesExistingClaim", func(t *testing.T) {
		t.Parallel()

		for _, phase := range []string{"DatabaseError", "ExpiredCandidate", "LateReply", "NoMatchingClaim", "ParentCancellation", "RequestTimeout"} {
			t.Run(phase, func(t *testing.T) {
				t.Parallel()

				synctest.Test(t, func(t *testing.T) {
					bundle := setup(t)
					oldCtx, oldCancel := context.WithCancelCause(context.Background())
					defer oldCancel(nil)
					old := bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), oldCancel)
					defer old.Abandon()
					defer old.Finalize(bundle.config.FinalizeTimeout)
					parentCtx, cancelParent := context.WithCancel(context.Background())
					defer cancelParent()
					bundle.manager.renewFunc = func(ctx context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
						require.Equal(t, []int64{1}, params.IDs)
						require.Equal(t, []int{2}, params.Attempts)
						switch phase {
						case "DatabaseError":
							return nil, errors.New("database unavailable")
						case "ExpiredCandidate", "RequestTimeout":
							<-ctx.Done()
							return nil, ctx.Err()
						case "LateReply":
							// A committed response arriving after the verification budget
							// must not replace the existing guard.
							time.Sleep(bundle.config.RenewTimeout + time.Second)
							return []*rivertype.JobRow{{ID: 1, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}}, nil
						case "NoMatchingClaim":
							return nil, nil
						case "ParentCancellation":
							cancelParent()
							return nil, ctx.Err()
						default:
							panic("unexpected confirmation phase")
						}
					}
					time.Sleep(time.Second)
					newCtx, newCancel := context.WithCancelCause(context.Background())
					defer newCancel(nil)
					sentAt := time.Now()
					if phase == "ExpiredCandidate" {
						sentAt = sentAt.Add(-bundle.config.LeaseDuration + bundle.config.LocalSafetyMargin + time.Second)
					}
					candidate := bundle.manager.Register(parentCtx, &rivertype.JobRow{ID: 1, Attempt: 2, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, sentAt, newCancel)
					candidate.Finalize(bundle.config.FinalizeTimeout)
					synctest.Wait()
					require.ErrorIs(t, context.Cause(newCtx), rivertype.ErrJobLeaseLost)
					require.ErrorIs(t, candidate.Check(), rivertype.ErrJobLeaseLost)
					require.NoError(t, oldCtx.Err())
					require.NoError(t, old.Check())
					require.Len(t, bundle.manager.entries, 1)
					require.Same(t, old, bundle.manager.entries[1].guard)
				})
			})
		}
	})

	t.Run("ConfirmationWaitDoesNotBlockHealthyRenewals", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			bundle := setup(t)
			bundle.config.HeartbeatInterval = time.Second
			claimedAt := time.Now()
			old := bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: &claimedAt, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), nil)
			defer old.Abandon()
			defer old.Finalize(bundle.config.FinalizeTimeout)
			healthy := bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 2, Attempt: 1, AttemptedAt: &claimedAt, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), nil)
			defer healthy.Abandon()
			defer healthy.Finalize(bundle.config.FinalizeTimeout)
			deadline := healthy.Deadline()
			bundle.manager.renewFunc = func(ctx context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				if len(params.IDs) == 1 && params.IDs[0] == 1 && params.Attempts[0] == 2 {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return []*rivertype.JobRow{{ID: 2, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}}, nil
			}
			require.NoError(t, bundle.manager.Start(context.Background()))
			defer bundle.manager.Stop()
			time.Sleep(time.Second / 2)
			candidate := bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 2, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), nil)
			candidate.Finalize(bundle.config.FinalizeTimeout)
			require.ErrorIs(t, candidate.Check(), rivertype.ErrJobLeaseLost)
			require.True(t, healthy.Deadline().After(deadline))
			require.NoError(t, healthy.Check())
			require.NoError(t, old.Check())
		})
	})

	t.Run("DelayedAndDuplicateRegistrationCannotReplaceCurrentClaim", func(t *testing.T) {
		t.Parallel()

		for _, test := range []struct {
			name         string
			attempt      int
			claimedAfter time.Duration
		}{
			{"Duplicate", 2, time.Second},
			{"OlderClaim", 1, 0},
			{"OlderClaimWithHigherAttempt", 3, 0},
			{"SameTimestampOlderAttempt", 1, time.Second},
			{"StaleClaimWithLaterTimestamp", 3, 2 * time.Second},
		} {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()

				synctest.Test(t, func(t *testing.T) {
					bundle := setup(t)
					firstClaimedAt := time.Now()
					time.Sleep(time.Second)
					current := bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 2, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), nil)
					bundle.claims[1] = &rivertype.JobRow{ID: 1, Attempt: 2, AttemptedAt: new(time.Now())}
					defer current.Abandon()
					defer current.Finalize(bundle.config.FinalizeTimeout)
					ctx, cancel := context.WithCancelCause(context.Background())
					defer cancel(nil)
					delayed := bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: test.attempt, AttemptedAt: new(firstClaimedAt.Add(test.claimedAfter)), LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), cancel)
					delayed.Finalize(bundle.config.FinalizeTimeout)
					synctest.Wait()
					require.ErrorIs(t, context.Cause(ctx), rivertype.ErrJobLeaseLost)
					require.ErrorIs(t, delayed.Check(), rivertype.ErrJobLeaseLost)
					require.NoError(t, current.Check())
					require.Len(t, bundle.manager.entries, 1)
					deadline := current.Deadline()
					time.Sleep(bundle.config.HeartbeatInterval)
					require.NoError(t, bundle.manager.renew(context.Background()))
					require.True(t, current.Deadline().After(deadline))
					require.True(t, bundle.manager.CanFetch())
				})
			})
		}
	})

	t.Run("LateReceiptCannotConfirmReplacement", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			bundle := setup(t)
			claimedAt := time.Now()
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			old := bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: &claimedAt, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), cancel)
			defer old.Abandon()
			defer old.Finalize(bundle.config.FinalizeTimeout)
			healthy := bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 2, Attempt: 1, AttemptedAt: &claimedAt, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), nil)
			defer healthy.Abandon()
			defer healthy.Finalize(bundle.config.FinalizeTimeout)
			oldDeadline, healthyDeadline := old.Deadline(), healthy.Deadline()
			var replacement *Guard
			var replacementDeadline time.Time
			calls := 0
			bundle.manager.renewFunc = func(ctx context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				if len(params.IDs) == 1 {
					// Register confirms B independently while A's batch is in flight.
					require.Equal(t, []int64{1}, params.IDs)
					require.Equal(t, []int{2}, params.Attempts)
					return []*rivertype.JobRow{{ID: 1, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}}, nil
				}
				require.ElementsMatch(t, []int64{1, 2}, params.IDs)
				calls++
				for i, id := range params.IDs {
					if id == 1 {
						require.Equal(t, calls, params.Attempts[i])
					}
				}
				if calls == 1 {
					// Replace A while its admitted renewal is still waiting for a reply.
					replacement = bundle.manager.Register(ctx, &rivertype.JobRow{ID: 1, Attempt: 2, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), nil)
					replacementDeadline = replacement.Deadline()
					old.Finalize(bundle.config.FinalizeTimeout)
					synctest.Wait()
				}
				require.NoError(t, ctx.Err(), "replacing a member must not cancel the shared batch")
				return []*rivertype.JobRow{
					{ID: 1, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))},
					{ID: 2, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))},
				}, nil
			}
			time.Sleep(bundle.config.HeartbeatInterval)
			require.NoError(t, bundle.manager.renew(context.Background()))
			defer replacement.Abandon()
			defer replacement.Finalize(bundle.config.FinalizeTimeout)
			require.ErrorIs(t, context.Cause(ctx), rivertype.ErrJobLeaseLost)
			require.Equal(t, oldDeadline, old.Deadline())
			require.Equal(t, replacementDeadline, replacement.Deadline(), "A's receipt must not extend B")
			require.True(t, healthy.Deadline().After(healthyDeadline))
			time.Sleep(bundle.config.HeartbeatInterval)
			require.NoError(t, bundle.manager.renew(context.Background()))
			require.True(t, replacement.Deadline().After(replacementDeadline))
			require.NoError(t, healthy.Check())
			require.True(t, bundle.manager.CanFetch())
		})
	})

	t.Run("NewClaimCancelsPreviousAndSurvivesItsCleanup", func(t *testing.T) {
		t.Parallel()

		for _, test := range []struct {
			name         string
			attempt      int
			claimedAfter time.Duration
		}{
			{"ClockRollback", 3, -time.Second},
			{"ClockRollbackSameAttempt", 2, -time.Second},
			{"ClockRollbackSmallerAttempt", 1, -time.Second},
			{"Retry", 3, time.Second},
			{"SnoozeWithSameAttempt", 2, time.Second},
			{"SnoozeWithSmallerAttempt", 1, time.Second},
		} {
			t.Run(test.name, func(t *testing.T) {
				t.Parallel()

				synctest.Test(t, func(t *testing.T) {
					bundle := setup(t)
					ctx, cancel := context.WithCancelCause(context.Background())
					defer cancel(nil)
					claimedAt := time.Now()
					old := bundle.manager.Register(context.Background(), &rivertype.JobRow{ID: 1, Attempt: 2, AttemptedAt: &claimedAt, LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}, time.Now(), cancel)
					defer old.Abandon()
					defer old.Finalize(bundle.config.FinalizeTimeout)
					deadline := old.Deadline()
					time.Sleep(time.Second)
					currentClaim := &rivertype.JobRow{ID: 1, Attempt: test.attempt, AttemptedAt: new(claimedAt.Add(test.claimedAfter)), LeaseExpiresAt: new(time.Now().Add(bundle.config.LeaseDuration))}
					bundle.claims[1] = currentClaim
					current := bundle.manager.Register(context.Background(), currentClaim, time.Now(), nil)
					defer current.Abandon()
					defer current.Finalize(bundle.config.FinalizeTimeout)
					require.ErrorIs(t, context.Cause(ctx), rivertype.ErrJobLeaseLost)
					require.ErrorIs(t, old.Check(), rivertype.ErrJobLeaseLost)
					// The old worker exits after its replacement was registered.
					old.Finalize(bundle.config.FinalizeTimeout)
					synctest.Wait()
					require.Len(t, bundle.manager.entries, 1)
					currentDeadline := current.Deadline()
					time.Sleep(bundle.config.HeartbeatInterval)
					require.NoError(t, bundle.manager.renew(context.Background()))
					require.Equal(t, deadline, old.Deadline())
					require.True(t, current.Deadline().After(currentDeadline))
					require.NoError(t, current.Check())
					require.True(t, bundle.manager.CanFetch())
				})
			})
		}
	})
}
