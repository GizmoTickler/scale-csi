// Package util provides utility functions and configurable settings.
package util

import (
	"sync"
	"time"
)

// UtilConfig holds configurable settings for utility functions.
// All fields have sensible defaults that are used if not explicitly set.
type UtilConfig struct {
	// Mount command timeout (default: 30s)
	MountTimeout time.Duration

	// Format command timeout (default: 5m)
	FormatTimeout time.Duration

	// iSCSI command timeout (default: 10s)
	ISCSITimeout time.Duration

	// NVMe command timeout (default: 30s)
	NVMeTimeout time.Duration

	// Discovery cache duration for iSCSI (default: 30s)
	DiscoveryCacheDuration time.Duration

	// Max concurrent iSCSI logins per portal (default: 2)
	MaxConcurrentLogins int
}

// Default configuration values
var (
	configMu sync.RWMutex
	config   = &UtilConfig{
		MountTimeout:           30 * time.Second,
		FormatTimeout:          5 * time.Minute,
		ISCSITimeout:           10 * time.Second,
		NVMeTimeout:            30 * time.Second,
		DiscoveryCacheDuration: 30 * time.Second,
		MaxConcurrentLogins:    2,
	}
)

// SetConfig updates the utility configuration.
// This should be called during driver initialization.
func SetConfig(cfg *UtilConfig) {
	if cfg == nil {
		return
	}

	configMu.Lock()
	defer configMu.Unlock()

	// Only update non-zero values to preserve defaults
	if cfg.MountTimeout > 0 {
		config.MountTimeout = cfg.MountTimeout
	}
	if cfg.FormatTimeout > 0 {
		config.FormatTimeout = cfg.FormatTimeout
	}
	if cfg.ISCSITimeout > 0 {
		config.ISCSITimeout = cfg.ISCSITimeout
	}
	if cfg.NVMeTimeout > 0 {
		config.NVMeTimeout = cfg.NVMeTimeout
	}
	if cfg.DiscoveryCacheDuration > 0 {
		config.DiscoveryCacheDuration = cfg.DiscoveryCacheDuration
	}
	if cfg.MaxConcurrentLogins > 0 {
		config.MaxConcurrentLogins = cfg.MaxConcurrentLogins
	}
}

// GetConfig returns a copy of the current configuration.
func GetConfig() UtilConfig {
	configMu.RLock()
	defer configMu.RUnlock()
	return *config
}

// getMountTimeout returns the configured mount timeout.
func getMountTimeout() time.Duration {
	configMu.RLock()
	defer configMu.RUnlock()
	return config.MountTimeout
}

// getFormatTimeout returns the configured format timeout.
func getFormatTimeout() time.Duration {
	configMu.RLock()
	defer configMu.RUnlock()
	return config.FormatTimeout
}

// getISCSITimeout returns the configured iSCSI command timeout.
func getISCSITimeout() time.Duration {
	configMu.RLock()
	defer configMu.RUnlock()
	return config.ISCSITimeout
}

// getNVMeTimeout returns the configured NVMe command timeout.
func getNVMeTimeout() time.Duration {
	configMu.RLock()
	defer configMu.RUnlock()
	return config.NVMeTimeout
}

// getDiscoveryCacheDuration returns the configured discovery cache duration.
func getDiscoveryCacheDuration() time.Duration {
	configMu.RLock()
	defer configMu.RUnlock()
	return config.DiscoveryCacheDuration
}

// getMaxConcurrentLogins returns the configured max concurrent logins.
func getMaxConcurrentLogins() int {
	configMu.RLock()
	defer configMu.RUnlock()
	return config.MaxConcurrentLogins
}
