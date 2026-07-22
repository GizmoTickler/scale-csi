# Topology behavior

The node service reads its Kubernetes Node object at startup. If the Node has
`topology.kubernetes.io/zone` or `topology.kubernetes.io/region`, scale-csi
automatically enables its node topology and returns those per-node segments in
`NodeGetInfo`. Node RBAC already permits that lookup.

There is no `node.topology` value in the bundled Helm chart. In particular,
these values are invalid and rejected by `values.schema.json`:

```text
# Unsupported chart values — do not use.
node:
  topology:
    enabled: true
    zone: zone-a
```

The distinction matters because the chart runs controller and node services in
separate pods. Node pods can auto-detect their own labels, but the controller
does not have a single node identity to detect and the chart does not inject a
static topology into it. Consequently, the chart's controller does not
advertise CSI `VOLUME_ACCESSIBILITY_CONSTRAINTS`, and it does not promise that a
StorageClass `allowedTopologies` rule will constrain provisioning.

The driver configuration type still supports a static `node.topology` block for
non-chart or monolithic deployments where one configuration correctly
represents both controller and node scope. A single static zone in the chart's
cluster-wide DaemonSet would be unsafe: it could label every node as the same
zone even when Kubernetes says otherwise. That is why this batch removes the
dead chart recipe instead of exposing it.

For the common deployment—a single TrueNAS backend reachable from every
eligible node—topology constraints are unnecessary. Use the default
`Immediate` binding mode, or `WaitForFirstConsumer` for workload scheduling
reasons unrelated to a scale-csi backend locality guarantee.

If multiple TrueNAS backends must map to distinct failure domains, deploy
separate driver instances with distinct `csiDriverName`,
`driverInstanceId`, and `zfs.parentDataset` values and validate the complete
scheduler/provisioner behavior. Automatic node-label reporting alone is not a
multi-backend routing policy.

See also: [StorageClass reference](../reference/storageclass.md) and
[Production](../production.md).
