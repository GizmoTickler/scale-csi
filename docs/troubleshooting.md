# Troubleshooting Guide

This guide helps diagnose and resolve common issues with the Scale CSI driver.

## Quick Diagnostics

### Check Driver Status

```bash
# Check controller pod status
kubectl get pods -n scale-csi -l app.kubernetes.io/component=controller

# Check node plugin status (should have one per node)
kubectl get pods -n scale-csi -l app.kubernetes.io/component=node

# Check CSI driver registration
kubectl get csidrivers
```

The chart labels pods with `app.kubernetes.io/name`,
`app.kubernetes.io/instance` (the Helm release name), and
`app.kubernetes.io/component` (`controller` or `node`) — there is no `app=`
label. Combine with the instance label to scope to one release, e.g.
`-l app.kubernetes.io/instance=scale-csi,app.kubernetes.io/component=controller`.

### View Logs

```bash
# Controller logs
kubectl logs -n scale-csi -l app.kubernetes.io/component=controller -c scale-csi --tail=100

# Node plugin logs (on specific node)
kubectl logs -n scale-csi -l app.kubernetes.io/component=node -c scale-csi --tail=100

# With Helm, enable verbose logging with: --set logging.verbosity=4
```

> **CHAP credentials are never logged.** CSI request `secrets` are structurally
> stripped before the V(5) request dump, and iSCSI CHAP argv values are redacted
> (`***`) in any param-set error. Raising `-v`/`logging.verbosity` will NOT expose
> a CHAP username/password secret value.

## Common Issues

### Volume Provisioning Failures

#### Symptom: PVC stuck in Pending

**Check Events:**
```bash
kubectl describe pvc <pvc-name>
kubectl get events --field-selector involvedObject.name=<pvc-name>
```

**Possible Causes:**

1. **TrueNAS Connection Issues**
   - Verify TrueNAS is accessible from the cluster
   - Check API key validity
   - Review controller logs for connection errors

   ```bash
   # Test TCP/TLS reachability from the cluster. An HTTP 400 response is
   # expected without a WebSocket upgrade and still proves the endpoint is reachable.
   kubectl run truenas-connectivity --rm -it --restart=Never \
     --image=curlimages/curl -- \
     curl -vk https://<truenas-host>/api/current
   ```

2. **Storage Pool Full**
   - Check available space on TrueNAS
   - Review ZFS pool status

3. **Invalid StorageClass or driver configuration**
   - Verify the chart's `zfs.parentDataset` exists on TrueNAS (it renders as
     `zfs.datasetParentName` in the strict driver configuration)
   - Verify the StorageClass has the required `protocol` parameter
   - Check protocol-specific Helm values (NFS host, iSCSI portal, and so on)

4. **Circuit Breaker Open**
   - If too many API failures occurred, the circuit breaker may be open
   - Check logs for "circuit breaker is open" messages
   - Wait for the timeout period or restart the controller

#### Symptom: Volume creation succeeds but share creation fails

**For NFS:**
- Verify NFS service is enabled on TrueNAS
- Check that `nfs.server` is correct and resolvable (defaults to the TrueNAS host)
- Verify `nfs.shareAllowedNetworks` includes your node IPs

**For iSCSI:**
- Verify iSCSI service is enabled on TrueNAS
- Check that portal groups and initiator groups are configured
- Verify `iscsi.portal` is correct (defaults to the TrueNAS host)

**For NVMe-oF:**
- Verify NVMe-oF service is enabled on TrueNAS
- Check that `nvmeof.address` is correct (defaults to the TrueNAS host)
- Verify subsystem hosts configuration (`nvmeof.subsystemHosts`)

### Volume Mount Failures

#### Symptom: Pod stuck in ContainerCreating

**Check Events:**
```bash
kubectl describe pod <pod-name>
```

**Possible Causes:**

1. **NFS Mount Issues**
   ```bash
   # Check NFS client on node
   kubectl exec -n scale-csi <node-pod> -c scale-csi -- showmount -e <nfs-server>

   # Verify NFS version support
   kubectl exec -n scale-csi <node-pod> -c scale-csi -- mount -t nfs4 <server>:<path> /mnt/test
   ```

2. **iSCSI Connection Issues**
   ```bash
   # Check iscsiadm is available
   kubectl exec -n scale-csi <node-pod> -c scale-csi -- iscsiadm --version

   # Check active iSCSI sessions
   kubectl exec -n scale-csi <node-pod> -c scale-csi -- iscsiadm -m session

   # Test target discovery
   kubectl exec -n scale-csi <node-pod> -c scale-csi -- iscsiadm -m discovery -t sendtargets -p <portal>
   ```

3. **NVMe-oF Connection Issues**
   ```bash
   # Check nvme CLI is available
   kubectl exec -n scale-csi <node-pod> -c scale-csi -- nvme version

   # List connected subsystems
   kubectl exec -n scale-csi <node-pod> -c scale-csi -- nvme list-subsys

   # Test discovery
   kubectl exec -n scale-csi <node-pod> -c scale-csi -- nvme discover -t tcp -a <address> -s 4420
   ```

4. **Device Not Appearing**
   - For iSCSI, increase the chart value `iscsi.deviceWaitTimeout`
   - `nvmeof.deviceWaitTimeout` exists in raw driver configuration but is not
     exposed by the Helm chart; do not add it to chart values
   - Check for kernel module issues (`iscsi_tcp`, `nvme-tcp`)
   - Ensure dm-multipath has not claimed an iSCSI component device; scale-csi
     intentionally rejects iSCSI multipath

#### Symptom: Mount succeeds but filesystem is read-only

- Check the PVC access mode and pod volume-mount `readOnly` setting
- Verify TrueNAS dataset isn't set to read-only
- Check for filesystem errors (run fsck if necessary)

### Session/Connection Issues

#### Symptom: Duplicate iSCSI/NVMe-oF sessions

The driver includes automatic session garbage collection. If you see duplicate sessions:

1. **Check GC configuration:**
   ```yaml
   sessionGC:
     enabled: true
     interval: 300      # Check every 5 minutes
     gracePeriod: 60    # Wait 1 minute before cleanup
   ```

2. **Manual cleanup:**
   ```bash
   # List orphaned iSCSI sessions
   kubectl exec -n scale-csi <node-pod> -c scale-csi -- iscsiadm -m session

   # Disconnect specific session
   kubectl exec -n scale-csi <node-pod> -c scale-csi -- iscsiadm -m node -T <iqn> -p <portal> --logout
   ```

#### Symptom: Stale sessions after node restart

- Session GC runs on startup by default (`sessionGC.runOnStartup: true`)
- Increase `sessionGC.startupDelay` if sessions need more time to initialize

### Job Dispatcher Pure-Poll Fallback

The driver waits on TrueNAS job-completion events through a `core.get_jobs`
WebSocket subscription. When no pooled connection holds a live subscription it
degrades to a **pure-poll fallback**: correct, but with higher API load and
latency. `scale_csi_job_dispatcher_subscribed` is `1` while subscribed and `0`
in the fallback; the `ScaleCSIJobDispatcherUnsubscribed` alert fires when it
stays `0`.

This is usually transient (a reconnecting socket re-subscribes on its next
generation). If it persists:

1. Check the controller log for repeated `core.get_jobs subscribe failed
   (pure-poll fallback)` lines and the underlying WebSocket error.
2. Confirm `scale_csi_truenas_connections_active` is non-zero and
   `scale_csi_truenas_connection_status` is `1` — a dead connection cannot
   subscribe.
3. Restart the controller pod; the subscription is re-established at connect.

### Performance Issues

#### Slow Volume Operations

1. **Reduce TrueNAS API load:**
   ```yaml
   truenas:
     maxConcurrentRequests: 10  # Limit concurrent API calls

   resilience:
     rateLimiting:
       maxConcurrentLogins: 2   # Limit iSCSI login concurrency
   ```

   The chart exposes only `maxConcurrentLogins` under
   `resilience.rateLimiting` (iSCSI login concurrency). The former
   `maxConcurrentRequests` key was deprecated and removed — the API
   concurrency limit is `truenas.maxConcurrentRequests`.
   `discoveryCacheDuration` is not a valid chart value.

2. **Tune retry settings:**
   ```yaml
   resilience:
     retry:
       maxAttempts: 3
       initialDelay: 500   # milliseconds
       maxDelay: 5000
   ```

3. **Check circuit breaker status:**
   - If frequently opening, investigate underlying TrueNAS issues
   - Adjust thresholds if needed:
   ```yaml
   resilience:
     circuitBreaker:
       enabled: true
       failureThreshold: 5
       timeout: 30
   ```

### Snapshot Issues

#### Symptom: Snapshot creation fails

- Verify the snapshot CRDs and external snapshot-controller are installed
- Verify the `VolumeSnapshotClass` uses driver `csi.scale.io`
- Check TrueNAS has sufficient snapshot quota
- Ensure source volume exists and is accessible

#### Symptom: Clone from snapshot fails

- Verify source snapshot exists
- Check that cloned volume capacity >= snapshot source capacity
- For **standard (clone-mode)** restores, `zfs.zvolReadyTimeout` does not apply:
  verification is one `DatasetGet` plus a single 250 ms retry, and exhaustion
  returns `Unavailable` for the sidecar to retry — investigate backend/API
  propagation latency. `zfs.zvolReadyTimeout` only affects the **detached**
  snapshot-copy path (readiness failure there maps to `Internal`).

## Error Messages Reference

| Error Message | Cause | Solution |
|--------------|-------|----------|
| `circuit breaker is open` | Too many consecutive API failures | Wait for timeout or fix TrueNAS connectivity |
| `discovery failed` | Can't reach iSCSI portal | Check network connectivity and portal address |
| `device not found after timeout` | Device didn't appear in time | Increase `deviceWaitTimeout` |
| `target is busy` | Volume still in use | Ensure all pods using volume are terminated |
| `already exists` | A backend object with that name already exists | Retry is safe **only** if it is a compatible, fully stamped object owned by this driver instance. Foreign, unstamped, incompatible, or conflicting objects return terminal ownership/compatibility errors by design — inspect the object's `scale-csi:*` ownership properties and resolve the name collision; do **not** blindly retry, and do not delete/rename the backend object to force success |
| `not found` | Resource doesn't exist | Check TrueNAS for dataset/share existence |
| `connection lost` | WebSocket disconnected | Will auto-reconnect; check TrueNAS status |

## Health Checks

The liveness sidecar serves `/healthz` on port 9808. Driver readiness and
Prometheus metrics use `metrics.port`, which defaults to 9809:

```bash
# Liveness check
curl http://<node-ip>:9808/healthz

# Readiness check
curl http://<node-ip>:9809/readyz

# Metrics (if enabled)
curl http://<node-ip>:9809/metrics
```

## Collecting Debug Information

When reporting issues, include:

1. **Driver version:**
   ```bash
   kubectl exec -n scale-csi <pod> -c scale-csi -- /usr/local/bin/scale-csi -version
   ```
   (The binary is on `PATH`, so `scale-csi -version` also works. Flags use the
   single-dash `-version` form.)

2. **Configuration (sanitized):**
   ```bash
   kubectl get configmap -n scale-csi scale-csi-config -o yaml
   ```

3. **Recent logs:**
   ```bash
   kubectl logs -n scale-csi -l app.kubernetes.io/component=controller --tail=500
   kubectl logs -n scale-csi -l app.kubernetes.io/component=node --tail=500
   ```

4. **Events:**
   ```bash
   kubectl get events -n scale-csi --sort-by='.lastTimestamp'
   ```

5. **TrueNAS version and configuration (if possible)**

## Recovery Procedures

### Force Delete Stuck PVC

```bash
# Remove finalizer (use with caution)
kubectl patch pvc <pvc-name> -p '{"metadata":{"finalizers":null}}'
kubectl delete pvc <pvc-name> --grace-period=0 --force
```

### Reset Circuit Breaker

Restart the controller pod to reset the circuit breaker:
```bash
kubectl rollout restart deployment -n scale-csi scale-csi-controller
```

### Clean Up Orphaned TrueNAS Resources

The controller detects old CSI-managed backend resources automatically and
exports `scale_csi_orphan_volumes`, `scale_csi_orphan_snapshots`,
`scale_csi_remnant_volumes`, `scale_csi_tombstone_snapshots`, and
`scale_csi_spent_restore_snapshots` count gauges. Byte gauges exist only for
orphan volumes (`scale_csi_orphan_volumes_bytes`), orphan snapshots
(`scale_csi_orphan_snapshots_bytes`), and tombstones
(`scale_csi_tombstone_snapshots_bytes`) — there is no remnant or spent-restore
byte gauge. Use `scale_csi_reconcile_last_success_timestamp_seconds` to detect a
stalled loop and `scale_csi_reconcile_failures_total{phase}` to isolate partial
object failures. Orphan **detection** is enabled by default; destructive orphan
**deletion** stays off until `reconcile.delete.enabled: true`. (A pass is not
wholly read-only — independent of the delete gate it performs always-on repair
mutations: legacy ownership-stamp adoption, stale marker/publication repair, and
the replication-job `core.job_abort` sweep.)

```yaml
reconcile:
  enabled: true
  interval: 1h
  minOrphanAge: 24h
  delete:
    enabled: false
```

Inspect the controller logs and metrics first. To opt into cleanup, set
`reconcile.delete.enabled: true`; the chart then creates a scheduled run-once
job. It never issues an unguarded raw ZFS destroy. The cleanup uses different
guarded paths per object class: orphan volumes and snapshots go through the CSI
`DeleteVolume`/`DeleteSnapshot` paths (which refuse resources with live clone or
snapshot dependencies); tombstones and marker-proven remnant orphans use
separately guarded direct TrueNAS-client destroys after re-proving provenance;
and spent-restore cleanup deletes the Kubernetes `VolumeSnapshot`. A single
`reconcile.delete.maxPerRun` cap (default 5) is shared across orphan volumes,
orphan snapshots, tombstones, remnants, and spent restores; orphaned-share
cleanup has its own separate `maxPerRun` invocation.

#### Tombstones that never drain

A snapshot deleted while it still had dependent clones is renamed to an internal
tombstone (`scale-csi:tombstone_*`) and destroyed deferred once the last clone
releases it. The reaper normally acts on tombstones through its durable ledger.
If ledger entries were lost (for example a pre-v1.2.30 controller that never
recorded them), the tombstone-shaped snapshots can strand. Two provenance belts
recover them:

- **Legacy stamp adoption** re-stamps `driver_instance_id` onto pre-v1.2.21
  managed datasets that a live Bound PV references, which unblocks reaping of
  their tombstones. This always-on step deletes nothing.
- **Scan fallback** is opt-in and off by default
  (`reconcile.tombstoneReaper.scanFallback.enabled: true`). It runs on **every**
  pass, independent of the strict ledger backlog. It issues no separate query —
  it reuses the pass's already-fetched recursive, unpaginated snapshot set and
  processes at most 500 accepted candidates. A candidate is reaped only when it
  has **no** ledger property at either bookkeeping location and carries retained
  creation-time identity that exactly reproduces the driver's nonce-derived
  tombstone rename (retained snapshot/instance identity, exact tombstone name,
  local source-instance ownership, age gate, and the inheritance-mask guard).

Tombstones the reaper refuses because no belt can prove provenance are counted
as `manualRecoveryTombstones` in the reconcile summary line — and that inventory
is populated **only while scan fallback is enabled**. They require operator
inspection and are never destroyed automatically.

You can inspect the same managed-resource boundary on TrueNAS with:

```bash
zfs list -o name,scale-csi:managed_resource -r <pool>
```

> **Legacy property spelling:** volumes and snapshots created before v1.10.0 may
> still carry these properties under the older `truenas-csi:*` spelling. The
> driver reads both spellings equivalently; datasets are re-stamped to
> `scale-csi:*` automatically by the reconciler, but pre-rename **snapshots**
> keep the legacy keys on disk (TrueNAS 26.0 has no snapshot property mutation
> API), so check both spellings when inspecting snapshots by hand.

> **DANGER:** never share one configured `zfs.parentDataset` between Kubernetes
> clusters. Reconcile cannot see handles owned by the other cluster and would
> classify its managed objects as orphaned.

## Alerts → Runbook

Every alert the chart's `PrometheusRule` can render (when
`metrics.prometheusRule.enabled: true`), cross-linked to a runbook anchor. Nine
of the rendered alerts additionally emit a `runbook_url` annotation pointing at
the same target; the others (including `ScaleCSIPoolNearFull`) rely on this table
for their runbook link.

| Alert | Severity | Runbook |
|-------|----------|---------|
| `ScaleCSIControllerDown` | critical | [Check Driver Status](#check-driver-status) |
| `ScaleCSICircuitBreakerOpen` | warning | [Reset Circuit Breaker](#reset-circuit-breaker) |
| `ScaleCSITrueNASConnectionDown` | critical | [Session/Connection Issues](#sessionconnection-issues) |
| `ScaleCSIHighTrueNASAPIFailureRate` | warning | [Session/Connection Issues](#sessionconnection-issues) |
| `ScaleCSISustainedLockContention` | warning | [Performance Issues](#performance-issues) |
| `ScaleCSIOperationErrorsSustained` | warning | [Volume Provisioning Failures](#volume-provisioning-failures) |
| `ScaleCSISpentRestoreSnapshotBacklog` | warning | [Clean Up Orphaned TrueNAS Resources](#clean-up-orphaned-truenas-resources) |
| `ScaleCSISessionGCDisconnects` | warning | [Session/Connection Issues](#sessionconnection-issues) |
| `ScaleCSIFencingTakeoverSpike` | warning | [Fencing takeover for a confirmed-dead node](guides/disaster-recovery.md#runbook-fencing-takeover-for-a-confirmed-dead-node) |
| `ScaleCSIFencingProvenanceOverflow` | critical | [Fencing takeover for a confirmed-dead node](guides/disaster-recovery.md#runbook-fencing-takeover-for-a-confirmed-dead-node) |
| `ScaleCSIJobDispatcherUnsubscribed` | warning | [Job Dispatcher Pure-Poll Fallback](#job-dispatcher-pure-poll-fallback) |
| `ScaleCSIDeleteResidualCleanupFailing` | warning | [Clean Up Orphaned TrueNAS Resources](#clean-up-orphaned-truenas-resources) |
| `ScaleCSIOrphanVolumesDetected` | warning | [Clean Up Orphaned TrueNAS Resources](#clean-up-orphaned-truenas-resources) |
| `ScaleCSIOrphanSnapshotsDetected` | warning | [Clean Up Orphaned TrueNAS Resources](#clean-up-orphaned-truenas-resources) |
| `ScaleCSIManualRecoveryTombstones` | warning | [Tombstones that never drain](#tombstones-that-never-drain) |
| `ScaleCSIRemnantVolumesDetected` | warning | [Clean Up Orphaned TrueNAS Resources](#clean-up-orphaned-truenas-resources) |
| `ScaleCSITombstoneBacklog` | warning | [Tombstones that never drain](#tombstones-that-never-drain) |
| `ScaleCSIReconcileStalled` | critical | [Clean Up Orphaned TrueNAS Resources](#clean-up-orphaned-truenas-resources) |
| `ScaleCSIPoolNearFull` | warning | [Performance Issues](#performance-issues) |
