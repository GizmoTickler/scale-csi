# Troubleshooting Guide

This guide helps diagnose and resolve common issues with the Scale CSI driver.

## Quick Diagnostics

### Check Driver Status

```bash
# Check controller pod status
kubectl get pods -n scale-csi -l app=scale-csi-controller

# Check node plugin status (should have one per node)
kubectl get pods -n scale-csi -l app=scale-csi-node

# Check CSI driver registration
kubectl get csidrivers
```

### View Logs

```bash
# Controller logs
kubectl logs -n scale-csi -l app=scale-csi-controller -c scale-csi --tail=100

# Node plugin logs (on specific node)
kubectl logs -n scale-csi -l app=scale-csi-node -c scale-csi --tail=100

# With Helm, enable verbose logging with: --set logging.verbosity=4
```

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
- Check that `nfs.shareHost` is correct and resolvable
- Verify `nfs.shareAllowedNetworks` includes your node IPs

**For iSCSI:**
- Verify iSCSI service is enabled on TrueNAS
- Check that portal groups and initiator groups are configured
- Verify `iscsi.targetPortal` is correct

**For NVMe-oF:**
- Verify NVMe-oF service is enabled on TrueNAS
- Check that `nvmeof.transportAddress` is correct
- Verify subsystem hosts configuration

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

   The chart exposes only `maxConcurrentRequests` and
   `maxConcurrentLogins` under `resilience.rateLimiting`;
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
- Review `zfs.zvolReadyTimeout` for slow clone operations

## Error Messages Reference

| Error Message | Cause | Solution |
|--------------|-------|----------|
| `circuit breaker is open` | Too many consecutive API failures | Wait for timeout or fix TrueNAS connectivity |
| `discovery failed` | Can't reach iSCSI portal | Check network connectivity and portal address |
| `device not found after timeout` | Device didn't appear in time | Increase `deviceWaitTimeout` |
| `target is busy` | Volume still in use | Ensure all pods using volume are terminated |
| `already exists` | Resource already created | Usually idempotent - retry should work |
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
   kubectl exec -n scale-csi <pod> -c scale-csi -- /scale-csi --version
   ```

2. **Configuration (sanitized):**
   ```bash
   kubectl get configmap -n scale-csi scale-csi-config -o yaml
   ```

3. **Recent logs:**
   ```bash
   kubectl logs -n scale-csi -l app=scale-csi-controller --tail=500
   kubectl logs -n scale-csi -l app=scale-csi-node --tail=500
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
exports `scale_csi_orphan_volumes`, `scale_csi_orphan_snapshots`, and byte
gauges. Use `scale_csi_reconcile_last_success_timestamp_seconds` to detect a
stalled loop and `scale_csi_reconcile_failures_total{phase}` to isolate partial
object failures. Detection is read-only and enabled by default:

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
job. The job never issues a raw ZFS destroy. It calls the existing guarded CSI
delete paths, which refuse resources with live clone or snapshot dependencies.

You can inspect the same managed-resource boundary on TrueNAS with:

```bash
zfs list -o name,truenas-csi:managed_resource -r <pool>
```

> **DANGER:** never share one configured `zfs.parentDataset` between Kubernetes
> clusters. Reconcile cannot see handles owned by the other cluster and would
> classify its managed objects as orphaned.
