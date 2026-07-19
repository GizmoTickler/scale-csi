package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUnmountRetryConfigUsesFinerBackoffWithinPreviousCeiling(t *testing.T) {
	cfg := UnmountRetryConfig()

	assert.Equal(t, 7, cfg.MaxAttempts)
	assert.Equal(t, 100*time.Millisecond, cfg.InitialDelay)
	assert.Equal(t, 2.0, cfg.BackoffFactor)

	delay := cfg.InitialDelay
	var total time.Duration
	for attempt := 1; attempt < cfg.MaxAttempts; attempt++ {
		total += delay
		delay = time.Duration(float64(delay) * cfg.BackoffFactor)
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
	}
	assert.Equal(t, 6300*time.Millisecond, total)
	assert.LessOrEqual(t, total, 7500*time.Millisecond)
}
