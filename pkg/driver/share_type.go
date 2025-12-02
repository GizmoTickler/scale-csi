// Package driver provides ShareType enum for consistent protocol handling.
package driver

import "strings"

// ShareType represents a storage protocol type.
// Using a distinct type prevents typos and enables IDE refactoring.
type ShareType string

// Supported share types.
const (
	ShareTypeNFS    ShareType = "nfs"
	ShareTypeISCSI  ShareType = "iscsi"
	ShareTypeNVMeoF ShareType = "nvmeof"
)

// String returns the string representation of the ShareType.
func (s ShareType) String() string {
	return string(s)
}

// IsValid returns true if the ShareType is a recognized value.
func (s ShareType) IsValid() bool {
	switch s {
	case ShareTypeNFS, ShareTypeISCSI, ShareTypeNVMeoF:
		return true
	default:
		return false
	}
}

// IsBlockProtocol returns true if the ShareType requires block device handling (iSCSI or NVMe-oF).
func (s ShareType) IsBlockProtocol() bool {
	return s == ShareTypeISCSI || s == ShareTypeNVMeoF
}

// ZFSResourceType returns the ZFS resource type for this ShareType.
// NFS uses "filesystem", block protocols use "volume" (zvol).
func (s ShareType) ZFSResourceType() string {
	if s.IsBlockProtocol() {
		return "volume"
	}
	return "filesystem"
}

// SupportsMultiNode returns true if the ShareType supports multi-node access modes.
// Only NFS supports ReadWriteMany and similar modes.
func (s ShareType) SupportsMultiNode() bool {
	return s == ShareTypeNFS
}

// ParseShareType parses a string into a ShareType.
// Returns ShareTypeNFS for unrecognized values (safe default).
func ParseShareType(s string) ShareType {
	normalized := strings.TrimSpace(strings.ToLower(s))
	switch normalized {
	case "iscsi":
		return ShareTypeISCSI
	case "nvmeof":
		return ShareTypeNVMeoF
	case "nfs":
		return ShareTypeNFS
	default:
		return ShareTypeNFS // Safe default
	}
}

// ValidShareTypes returns all valid share type values for error messages.
func ValidShareTypes() []ShareType {
	return []ShareType{ShareTypeNFS, ShareTypeISCSI, ShareTypeNVMeoF}
}

// ValidShareTypeStrings returns all valid share type strings for error messages.
func ValidShareTypeStrings() []string {
	return []string{string(ShareTypeNFS), string(ShareTypeISCSI), string(ShareTypeNVMeoF)}
}
