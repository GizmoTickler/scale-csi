# Upgrades

This guide covers upgrading scale-csi itself, rolling it back, and upgrading
TrueNAS underneath it. Which TrueNAS and Kubernetes releases are supported is
in the [compatibility reference](../reference/compatibility.md).

## Upgrading the driver

### Before you start

1. Read the release notes for every release between the one you run and the
   target: the [GitHub releases](https://github.com/GizmoTickler/scale-csi/releases)
   and [`docs/release-notes-next.md`](../release-notes-next.md). Each release
   has an **Upgrade Notes** section; follow any action it lists.
2. Check what is running now:

   ```bash
   helm -n scale-csi list
   kubectl -n scale-csi get deploy,ds -o wide
   ```

3. Preview the change. The chart validates values against its schema, so this
   also catches keys a newer chart renamed or removed:

   ```bash
   helm diff upgrade scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
     --namespace scale-csi --version <X.Y.Z> --values values.yaml
   ```

   (`helm diff` is the [helm-diff plugin](https://github.com/databus23/helm-diff);
   `helm template … --version <X.Y.Z>` works too.)

### Apply the CRD, then upgrade

The controller stores publication records as `VolumePublication` objects whose
CRD ships in the chart's `crds/` directory. Helm installs `crds/` on the first
install only, never on upgrade, and the controller refuses to start without
the current CRD. Apply it from the target release first:

```bash
VERSION=v<X.Y.Z>
kubectl apply -f "https://raw.githubusercontent.com/GizmoTickler/scale-csi/${VERSION}/charts/scale-csi/crds/volumepublications.scale-csi.io.yaml"

helm upgrade scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi --version "${VERSION#v}" --values values.yaml
```

With Flux, set `install.crds` and `upgrade.crds` to `CreateReplace` on the
HelmRelease (as in [the Flux example](../../examples/gitops/flux/scale-csi.yaml))
and skip the manual step. Argo CD applies `crds/` on every sync.

### What happens during the upgrade

- The controller Deployment rolls (with `Recreate` when fencing is `additive`
  or `strict`, so two controllers never overlap).
- The node DaemonSet rolls one node at a time (`maxUnavailable: 1`). Mounted
  volumes keep working while a node pod restarts; only new mounts and unmounts
  on that node wait for it.
- With the optional ublk data path, the `nvmeublkd` DaemonSet also rolls. Each
  node's ublk volumes pause I/O for a few seconds during the handover.
- ConfigMap and chart-managed Secret changes roll both workloads through
  checksum annotations.

### Verify

```bash
kubectl -n scale-csi rollout status deploy/scale-csi-controller
kubectl -n scale-csi rollout status ds/scale-csi-node
kubectl -n scale-csi get pods
kubectl -n scale-csi logs deploy/scale-csi-controller -c scale-csi --tail=50
```

Then run a smoke test from [`examples/workloads/`](../../examples/workloads/) in
a scratch namespace: create, mount, expand, snapshot, restore, delete.

### Rolling back

`helm rollback scale-csi <revision> -n scale-csi` restores the previous chart
and image; it does not roll back the CRD (Helm never touches `crds/` after
the first install). Some features change backend state that outlives a rollback; check
before going back past them:

- **Encryption at rest** — do not roll back below a release that supports it
  while encrypted volumes exist; see
  [Do not roll back below encryption support](../reference/storageclass.md#-do-not-roll-back-below-encryption-support).
- **Snapshot holds** (`zfs.holdCsiSnapshots`) — release them first; see the
  [hold rollback runbook](../production.md#rollback-runbook-releasing-zfs-holds-after-disabling-zfsholdcsisnapshots).
- **Driver-managed periodic snapshots** — see the
  [periodic-snapshot runbook](../production.md#rollback-runbook-driver-managed-periodic-snapshot-tasks).
- **Bookkeeping relocation** (`reconcile.bookkeeping.enabled`) is one-way; do
  not turn it back off.

Beyond the documented node-first fencing migration, arbitrary controller and
node version skew is not promised: roll controller and node to the same
release.

### Special migrations

- **Fencing** (`fencing.mode` `off` → `additive` → `strict`) needs a node-first
  rollout; follow the
  [chart README procedure](../../charts/scale-csi/README.md#publication-fencing-and-ownership).
- **StorageClass parameters are immutable.** Adding `protocol` (required once
  more than one protocol is enabled) means creating a replacement class; see
  [Upgrade: add `protocol` safely](../reference/storageclass.md#upgrade-add-protocol-safely).
- **Node agent** (`node.implementation: rust | go`) can be switched in either
  direction with a normal upgrade; each agent adopts what the other staged.
- **Production details** — immutable `CSIDriver` fields, sidecar images,
  snapshot CRD versions — are in [Production › Upgrades](../production.md#upgrades).

### Rotating the TrueNAS API key

1. Create a new key for the same user on TrueNAS.
2. Update the Secret:

   ```bash
   kubectl -n scale-csi create secret generic truenas-api-key \
     --from-literal=api-key='<new-key>' --dry-run=client -o yaml | kubectl apply -f -
   ```

3. Restart the controller (only the controller holds the key; node pods do not
   need a restart):

   ```bash
   kubectl -n scale-csi rollout restart deploy/scale-csi-controller
   kubectl -n scale-csi rollout status deploy/scale-csi-controller
   ```

4. Revoke the old key on TrueNAS once the controller is `Ready`.

A key set through `truenas.apiKey` (chart-managed Secret) rolls the controller
automatically on `helm upgrade`.

## Upgrading TrueNAS

TrueNAS upgrades reboot the NAS. Every volume is unreachable for the length of
the reboot, so treat it as a storage maintenance window.

> **Upgrade scale-csi to v1.25.0 or later BEFORE moving TrueNAS to 27.0.**
> The REST API removal does not affect scale-csi (it speaks only the WebSocket
> JSON-RPC API), but TrueNAS 27 changed two things that do:
>
> - **NVMe/TCP:** TrueNAS 27's kernel (6.18.52) rejects any NVMe/TCP command
>   larger than 4 MiB while still advertising no transfer limit (MDTS=0).
>   Older drivers let initiators issue up to 32 MiB writes, which fail
>   (`I/O Error (sct 0x0 / sc 0xf) DNR`) and drop ext4 to read-only. v1.25.0
>   clamps `max_sectors_kb` on every head and path device at stage
>   (`nvmeof.maxSectorsKB`, default `auto`). See
>   [troubleshooting](../troubleshooting.md) if you already upgraded.
> - **Encryption:** `pool.dataset.create` no longer accepts
>   `encryption_options.algorithm` and requires `pbkdf2iters` ≥ 1,300,000;
>   older drivers fail every encrypted `CreateVolume`.
>
> Check the [compatibility table](../reference/compatibility.md) for the driver
> release you run before upgrading TrueNAS.

### What happens to mounted volumes during a NAS outage

| Protocol | During the outage | After TrueNAS returns |
|---|---|---|
| NFS (`hard` mounts, the default) | I/O blocks; processes wait | I/O resumes |
| iSCSI | I/O queues while the initiator retries the session; `open-iscsi` fails I/O once its replacement timeout expires | Sessions log back in; a volume whose I/O already failed may have a read-only or shut-down filesystem and needs its pod restarted |
| NVMe-oF, single path (default) | I/O queues (the driver connects with `--ctrl-loss-tmo=-1`) | I/O resumes |
| NVMe-oF, multipath | Paths fail fast and queue on the subsystem | Paths reconnect |
| Encrypted volumes | Locked after the reboot (the passphrase is not stored on TrueNAS) | The controller's reconciler unlocks them; until then I/O fails |

Long outages with workloads still running risk application timeouts and
filesystem errors. The safe procedure stops the consumers first.

### Procedure

1. **Read the TrueNAS release notes** for removed features, and confirm the
   target release is supported by your driver release.

2. **Check the driver is healthy** — controller `Ready`, no PVCs `Pending`, no
   failing operations:

   ```bash
   kubectl -n scale-csi get pods
   kubectl get pvc -A | grep -v Bound
   ```

3. **Find every workload using a scale-csi volume:**

   ```bash
   # PVCs provisioned by scale-csi
   kubectl get pv -o json | jq -r '.items[]
     | select(.spec.csi.driver == "csi.scale.io" and .spec.claimRef != null)
     | "\(.spec.claimRef.namespace)/\(.spec.claimRef.name)"' | sort > scale-csi-pvcs.txt

   # Pods mounting them
   kubectl get pods -A -o json | jq -r --rawfile pvcs scale-csi-pvcs.txt '
     ($pvcs | split("\n")) as $set
     | .items[]
     | .metadata.namespace as $ns
     | select([.spec.volumes[]?.persistentVolumeClaim.claimName // empty
               | "\($ns)/\(.)"] | any(. as $c | $set | index($c)))
     | "\($ns)/\(.metadata.name)\t\(.metadata.ownerReferences[0].kind // "-")/\(.metadata.ownerReferences[0].name // "-")"'
   ```

   Use your `csiDriverName` if you changed it.

4. **Stop the consumers.** Scale the owning Deployments and StatefulSets to
   zero (record the replica counts first), suspend CronJobs, and pause backup
   tools such as VolSync or Velero that mount snapshots. Wait until the volumes
   are detached:

   ```bash
   kubectl get volumeattachments -o json | jq -r '.items[]
     | select(.spec.attacher == "csi.scale.io") | .metadata.name'
   ```

   An empty list means nothing is attached. Leave the scale-csi controller and
   node pods running; they tolerate the NAS outage (liveness does not depend on
   TrueNAS) and reconnect on their own.

5. **Upgrade TrueNAS** and let it reboot. Keep the previous boot environment so
   you can boot back into it.

6. **Verify TrueNAS:** the NFS / iSCSI / NVMe-oF services you use are running,
   the pool is healthy, and the parent dataset is present.

7. **Verify the driver reconnected:**

   ```bash
   kubectl -n scale-csi get pods     # controller Ready again
   kubectl -n scale-csi logs deploy/scale-csi-controller -c scale-csi --since=15m | grep -i -E 'error|fail' | tail
   ```

   With metrics, `scale_csi_truenas_connection_status` returns to `1`. With
   encryption, wait until the locked-volume reconciler has unlocked every
   encrypted volume before starting their workloads.

8. **Smoke test** with a manifest from
   [`examples/workloads/`](../../examples/workloads/) for each protocol you use,
   then delete it.

9. **Start the consumers** again (restore the replica counts, resume CronJobs
   and backup tools) and watch for mount errors:

   ```bash
   kubectl get events -A --field-selector reason=FailedMount -w
   ```

### If you cannot stop the workloads

Upgrading with volumes attached works for NFS (hard mounts) and single-path
NVMe-oF, which wait for the NAS to return. For iSCSI, raise the initiator's
`node.session.timeo.replacement_timeout` on the nodes beforehand or expect to
restart pods whose filesystems went read-only. Always schedule the upgrade in
a window when brief I/O stalls are acceptable.

### Release-specific notes

- **25.x → 26.0:** CSI snapshots created by older driver versions without the
  `scale-csi:csi_snapshot_name` property are omitted from `ListSnapshots`
  afterwards; restore and delete by snapshot ID keep working.
- **26.x → 27.0:** upgrade scale-csi to v1.25.0 or later first (see the note above).
