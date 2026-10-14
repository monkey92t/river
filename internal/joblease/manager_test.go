package joblease

import (
	"context"
	"errors"
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

	t.Run("AbandonedMemberCancelsBatch", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			var guard *Guard
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(ctx context.Context, _ *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				guard.Abandon()
				synctest.Wait()
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				return nil, ctx.Err()
			})
			guard = manager.Register(&rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			defer guard.Finalize(config.FinalizeTimeout)
			require.ErrorIs(t, manager.renew(context.Background()), context.Canceled)
			require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
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
			guard := manager.Register(&rivertype.JobRow{ID: 1, Attempt: 2, AttemptedAt: &claimedAt, LeaseExpiresAt: new(expiresAt)}, time.Now(), nil)
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
			guard := manager.Register(&rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), cancel)
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

	t.Run("HealthyLeaseSurvivesOriginalDeadline", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", func(_ context.Context, params *riverdriver.JobRenewLeaseManyParams) ([]*rivertype.JobRow, error) {
				return []*rivertype.JobRow{{ID: params.IDs[0], LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}}, nil
			})
			guard := manager.Register(&rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
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
			guard := manager.Register(&rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
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
			guard := manager.Register(&rivertype.JobRow{ID: 1, LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
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
			doneGuard = manager.Register(&rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			defer doneGuard.Abandon()
			liveGuard := manager.Register(&rivertype.JobRow{ID: 2, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
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

	t.Run("UncooperativeWorkStopsAcquisition", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			config := (&Config{}).WithDefaults()
			manager := NewManager(riversharedtest.BaseServiceArchetype(t), config, "", nil)
			guard := manager.Register(&rivertype.JobRow{ID: 1, Attempt: 1, AttemptedAt: new(time.Now()), LeaseExpiresAt: new(time.Now().Add(config.LeaseDuration))}, time.Now(), nil)
			guard.Abandon()
			synctest.Wait()
			time.Sleep(config.CancelGracePeriod)
			synctest.Wait()
			require.False(t, manager.CanFetch())
			guard.Finalize(config.FinalizeTimeout)
		})
	})
}
