# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Scale CSI is a Kubernetes Container Storage Interface (CSI) driver for TrueNAS SCALE. It communicates exclusively via WebSocket JSON-RPC 2.0 (no SSH) and supports three storage protocols: NFS, iSCSI, and NVMe-oF.

## Build Commands

```bash
# Build binary
CGO_ENABLED=0 go build -o scale-csi ./cmd/scale-csi

# Run tests
go test -v -race ./...

# Run single test
go test -v -race ./pkg/driver -run TestControllerCreateVolume

# Lint
go vet ./...
golangci-lint run

# Build Docker image
docker build -t scale-csi .
```

## Architecture

### CSI Components (Standard Kubernetes CSI Pattern)

- **Controller** (`pkg/driver/controller.go`): Deployment that manages TrueNAS storage resources
  - Creates/deletes ZFS datasets (NFS) or zvols (iSCSI/NVMe-oF)
  - Creates/deletes snapshots and clones
  - Handles volume expansion
  - Creates NFS shares, iSCSI targets/extents, or NVMe-oF subsystems/namespaces

- **Node** (`pkg/driver/node.go`): DaemonSet on every node that mounts storage
  - `NodeStageVolume`: Connects to storage (NFS mount, iSCSI login, NVMe connect)
  - `NodePublishVolume`: Bind-mounts staged volume into pod
  - `NodeExpandVolume`: Resizes filesystem after controller expansion

### TrueNAS API Client (`pkg/truenas/`)

- `client.go`: WebSocket connection pool with JSON-RPC 2.0, auto-reconnect, heartbeat
- `dataset.go`: ZFS dataset/zvol CRUD operations
- `snapshot.go`: ZFS snapshot operations
- `nfs.go`: NFS share management
- `iscsi.go`: iSCSI target/extent/targetextent management
- `nvmeof.go`: NVMe-oF subsystem/namespace management
- `interface.go`: `ClientInterface` for mocking in tests

### Utilities (`pkg/util/`)

- `mount.go`: Filesystem mount/unmount operations
- `iscsi.go`: iscsiadm wrapper for iSCSI initiator commands
- `nvme.go`: nvme-cli wrapper for NVMe-oF commands

### Volume ID Format

Volume IDs encode the storage protocol and ZFS path: `{driver}:{dataset_path}`

Examples:
- `scale-nfs:tank/k8s/volumes/pvc-abc123`
- `scale-iscsi:tank/k8s/volumes/pvc-abc123`
- `scale-nvmeof:tank/k8s/volumes/pvc-abc123`

### ZFS Custom Properties

The driver tracks CSI metadata using ZFS user properties prefixed with `truenas-csi:`. Key properties defined in `controller.go`:
- `truenas-csi:managed_resource` - Marks CSI-managed datasets
- `truenas-csi:csi_volume_name` - Original PVC name
- `truenas-csi:truenas_nfs_share_id` - Associated NFS share ID
- `truenas-csi:truenas_iscsi_target_id` - Associated iSCSI target ID

## Testing

Tests use mock clients defined in `pkg/truenas/mock_client.go`. The `MockClient` implements `ClientInterface` for unit testing controller logic without a real TrueNAS instance.

## Helm Chart

Located in `charts/scale-csi/`. Key templates:
- `controller-deployment.yaml`: CSI controller with sidecar containers
- `node-daemonset.yaml`: CSI node plugin with privileged access
- `configmap.yaml`: Driver configuration
- `secret.yaml`: TrueNAS API key
