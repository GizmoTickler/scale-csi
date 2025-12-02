// Package util provides retry utilities for transient failures.
package util

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"
)

// RetryConfig holds configuration for retry operations.
type RetryConfig struct {
	MaxAttempts     int           // Maximum number of attempts (default: 3)
	InitialDelay    time.Duration // Initial delay between retries (default: 100ms)
	MaxDelay        time.Duration // Maximum delay between retries (default: 5s)
	BackoffFactor   float64       // Multiplier for delay after each attempt (default: 2.0)
	RetryableErrors []string      // Substrings that indicate a retryable error
}

// DefaultRetryConfig returns sensible defaults for retry operations.
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxAttempts:   3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
		RetryableErrors: []string{
			"device or resource busy",
			"target is busy",
			"resource temporarily unavailable",
			"connection timed out",
			"transport endpoint is not connected",
		},
	}
}

// IsRetryableError checks if an error message contains any retryable error substring.
func IsRetryableError(err error, retryableErrors []string) bool {
	if err == nil {
		return false
	}
	errStr := strings.ToLower(err.Error())
	for _, retryable := range retryableErrors {
		if strings.Contains(errStr, strings.ToLower(retryable)) {
			return true
		}
	}
	return false
}

// RetryWithBackoff executes a function with exponential backoff retry.
// The function should return nil on success, or an error to trigger a retry.
// Returns the last error if all attempts fail.
func RetryWithBackoff(ctx context.Context, name string, cfg *RetryConfig, fn func() error) error {
	if cfg == nil {
		cfg = DefaultRetryConfig()
	}

	var lastErr error
	delay := cfg.InitialDelay

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		// Check context before each attempt
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: context cancelled: %w", name, ctx.Err())
		default:
		}

		err := fn()
		if err == nil {
			if attempt > 1 {
				klog.V(4).Infof("%s succeeded on attempt %d", name, attempt)
			}
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !IsRetryableError(err, cfg.RetryableErrors) {
			klog.V(4).Infof("%s: non-retryable error: %v", name, err)
			return err
		}

		// Don't sleep after last attempt
		if attempt < cfg.MaxAttempts {
			klog.V(4).Infof("%s: attempt %d/%d failed (retrying in %v): %v",
				name, attempt, cfg.MaxAttempts, delay, err)

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return fmt.Errorf("%s: context cancelled during retry: %w", name, ctx.Err())
			}

			// Exponential backoff
			delay = time.Duration(float64(delay) * cfg.BackoffFactor)
			if delay > cfg.MaxDelay {
				delay = cfg.MaxDelay
			}
		}
	}

	return fmt.Errorf("%s: failed after %d attempts: %w", name, cfg.MaxAttempts, lastErr)
}

// UnmountRetryConfig returns retry configuration optimized for unmount operations.
func UnmountRetryConfig() *RetryConfig {
	cfg := DefaultRetryConfig()
	cfg.MaxAttempts = 5
	cfg.InitialDelay = 500 * time.Millisecond
	cfg.RetryableErrors = []string{
		"device or resource busy",
		"target is busy",
		"resource temporarily unavailable",
	}
	return cfg
}

// DisconnectRetryConfig returns retry configuration for iSCSI/NVMe-oF disconnect.
func DisconnectRetryConfig() *RetryConfig {
	cfg := DefaultRetryConfig()
	cfg.MaxAttempts = 3
	cfg.InitialDelay = 200 * time.Millisecond
	cfg.RetryableErrors = []string{
		"device or resource busy",
		"session is busy",
		"connection timed out",
		"transport endpoint is not connected",
	}
	return cfg
}
