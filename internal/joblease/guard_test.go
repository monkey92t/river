package joblease

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/riverqueue/river/rivertype"
)

func TestGuard(t *testing.T) {
	t.Parallel()

	t.Run("AbandonCancelsUncancelledRequest", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			guard := NewGuard(time.Now().Add(time.Minute), cancel)
			defer guard.Abandon()
			ctx = WithGuard(ctx, guard)
			requestCtx, cleanup, err := FromContext(context.WithoutCancel(ctx)).Begin(context.WithoutCancel(ctx))
			require.NoError(t, err)
			defer cleanup()

			guard.Abandon()
			synctest.Wait()

			require.ErrorIs(t, context.Cause(ctx), rivertype.ErrJobLeaseLost)
			require.ErrorIs(t, context.Cause(requestCtx), rivertype.ErrJobLeaseLost)
			require.False(t, guard.Renew(time.Now().Add(time.Hour)))
			require.False(t, guard.Finish())
		})
	})

	t.Run("CancellationBudgetCannotBeRenewed", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			guard := NewGuard(time.Now().Add(time.Minute), nil)
			defer guard.Abandon()
			guard.CancelAfter(10 * time.Second)
			time.Sleep(5 * time.Second)
			require.True(t, guard.Renew(time.Now().Add(time.Hour)))
			guard.Finalize(30 * time.Second)
			time.Sleep(5 * time.Second)
			require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
		})
	})

	t.Run("ConfirmationEndsRenewal", func(t *testing.T) {
		t.Parallel()

		guard := NewGuard(time.Now().Add(time.Minute), nil)
		defer guard.Abandon()
		guard.Finalize(30 * time.Second)
		require.NoError(t, guard.Check())
		require.True(t, guard.Finish())
		require.True(t, guard.Finished())
		require.False(t, guard.Renew(time.Now().Add(time.Hour)))
		require.ErrorIs(t, guard.Check(), context.Canceled)
	})

	t.Run("DeadlineCheckedBeforeWatchdogRuns", func(t *testing.T) {
		t.Parallel()

		guard := NewGuard(time.Now().Add(-time.Second), nil)
		defer guard.Abandon()
		require.False(t, guard.Renew(time.Now().Add(time.Hour)))
		require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
		require.False(t, guard.Finish())
	})

	t.Run("FinalizeBudgetCannotBeRenewed", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			guard := NewGuard(time.Now().Add(time.Minute), nil)
			defer guard.Abandon()
			guard.Finalize(30 * time.Second)
			select {
			case <-guard.WorkReturned():
			default:
				t.Fatal("Work return was not recorded")
			}
			time.Sleep(20 * time.Second)
			require.True(t, guard.Renew(time.Now().Add(time.Hour)))
			guard.Finalize(time.Minute)
			time.Sleep(10 * time.Second)
			require.ErrorIs(t, guard.Check(), rivertype.ErrJobLeaseLost)
		})
	})

	t.Run("NilPreservesLegacyExecution", func(t *testing.T) {
		t.Parallel()

		guard := FromContext(context.Background())
		require.Nil(t, guard)
		require.NoError(t, guard.Check())
		require.True(t, guard.Finish())
		guard.Finalize(time.Second)
		guard.CancelAfter(time.Second)
		guard.Abandon()
	})

	t.Run("RenewalExtendsIndependentWatchdog", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			guard := NewGuard(time.Now().Add(10*time.Second), cancel)
			defer guard.Abandon()
			time.Sleep(5 * time.Second)
			require.True(t, guard.Renew(time.Now().Add(20*time.Second)))
			time.Sleep(6 * time.Second)
			require.NoError(t, guard.Check())
			time.Sleep(14 * time.Second)
			synctest.Wait()
			require.ErrorIs(t, context.Cause(ctx), rivertype.ErrJobLeaseLost)
		})
	})
}
