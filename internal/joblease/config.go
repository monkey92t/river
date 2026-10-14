package joblease

import (
	"cmp"
	"errors"
	"fmt"
	"time"
)

// Config controls opt-in job leases. Zero-valued durations use the defaults.
type Config struct {
	// CancelGracePeriod bounds cooperative shutdown after cancellation.
	CancelGracePeriod time.Duration
	// FinalizeTimeout bounds completion writes after Work returns.
	FinalizeTimeout time.Duration
	// HeartbeatInterval is the normal interval between renewal batches.
	HeartbeatInterval time.Duration
	// LeaseDuration is the duration granted by a confirmed database renewal.
	LeaseDuration time.Duration
	// LocalSafetyMargin makes local cancellation precede database expiration.
	LocalSafetyMargin time.Duration
	// RenewTimeout bounds each renewal, including connection acquisition.
	RenewTimeout time.Duration
	// RescueGracePeriod delays rescue after database lease expiration.
	RescueGracePeriod time.Duration
	// RescueScanInterval controls how often expired leases are scanned.
	RescueScanInterval time.Duration
}

// Validate rejects durations that cannot leave time to renew and cancel safely.
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	for _, field := range []struct {
		name  string
		value time.Duration
	}{
		{"CancelGracePeriod", c.CancelGracePeriod},
		{"FinalizeTimeout", c.FinalizeTimeout},
		{"HeartbeatInterval", c.HeartbeatInterval},
		{"LeaseDuration", c.LeaseDuration},
		{"LocalSafetyMargin", c.LocalSafetyMargin},
		{"RenewTimeout", c.RenewTimeout},
		{"RescueGracePeriod", c.RescueGracePeriod},
		{"RescueScanInterval", c.RescueScanInterval},
	} {
		if field.value <= 0 {
			return fmt.Errorf("JobLease.%s must be greater than zero", field.name)
		}
	}
	if c.LocalSafetyMargin <= c.CancelGracePeriod {
		return errors.New("JobLease.LocalSafetyMargin must be greater than CancelGracePeriod")
	}
	// Subtraction avoids overflow when validating user-supplied durations.
	remaining := c.LeaseDuration - c.LocalSafetyMargin
	if remaining <= c.HeartbeatInterval || remaining-c.HeartbeatInterval <= c.RenewTimeout {
		return errors.New("JobLease.LeaseDuration must exceed LocalSafetyMargin + HeartbeatInterval + RenewTimeout")
	}
	return nil
}

// WithDefaults copies the configuration, preserving nil as disabled.
func (c *Config) WithDefaults() *Config {
	if c == nil {
		return nil
	}
	return &Config{
		CancelGracePeriod:  cmp.Or(c.CancelGracePeriod, 10*time.Second),
		FinalizeTimeout:    cmp.Or(c.FinalizeTimeout, 30*time.Second),
		HeartbeatInterval:  cmp.Or(c.HeartbeatInterval, 20*time.Second),
		LeaseDuration:      cmp.Or(c.LeaseDuration, 120*time.Second),
		LocalSafetyMargin:  cmp.Or(c.LocalSafetyMargin, 20*time.Second),
		RenewTimeout:       cmp.Or(c.RenewTimeout, 5*time.Second),
		RescueGracePeriod:  cmp.Or(c.RescueGracePeriod, 60*time.Second),
		RescueScanInterval: cmp.Or(c.RescueScanInterval, 30*time.Second),
	}
}
