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

# Enable verbose logging by setting --v=4 in the container args
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
   # Test connectivity from controller pod
   kubectl exec -n scale-csi <controller-pod> -c scale-csi -- \
     wget -q --spider https://<truenas-host>/api/v2.0/system/info
   ```

2. **Storage Pool Full**
   - Check available space on TrueNAS
   - Review ZFS pool status

3. **Invalid StorageClass Parameters**
   - Verify `datasetParentName` exists on TrueNAS
   - Check protocol-specific settings (NFS host, iSCSI portal, etc.)

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
   - Increase `iscsi.deviceWaitTimeout` or `nvmeof.deviceWaitTimeout`
   - Check for kernel module issues (`iscsi_tcp`, `nvme-tcp`)
   - Verify multipath configuration if used

#### Symptom: Mount succeeds but filesystem is read-only

- Check volume capability in StorageClass
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
       discoveryCacheDuration: 30  # Cache discovery results
   ```

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

- Verify snapshots are enabled in your StorageClass
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

The driver exposes health endpoints on the configured port (default: 9808):

```bash
# Liveness check
curl http://<node-ip>:9808/healthz

# Readiness check
curl http://<node-ip>:9808/readyz

# Metrics (if enabled)
curl http://<node-ip>:9808/metrics
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

5. **TrueNAS version and configuration (if possible)

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

If CSI resources are orphaned on TrueNAS:

1. List CSI-managed datasets:
   ```bash
   zfs list -o name,truenas-csi:managed_resource -r <pool>
   ```

2. Remove orphaned datasets (verify they're not in use first):
   ```bash
   zfs destroy <pool>/<dataset>
   ```

3. Remove orphaned shares via TrueNAS UI or API
