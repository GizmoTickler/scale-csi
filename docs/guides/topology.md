# Topology-aware provisioning

scale-csi implements the CSI `VOLUME_ACCESSIBILITY_CONSTRAINTS` capability. When
topology is enabled, each node plugin advertises topology segments in `NodeGetInfo`
and the controller returns a matching `AccessibleTopology` on `CreateVolume`, so the
scheduler and the external-provisioner can place volumes with zone/region affinity.

> **Do you need this?** For the common scale-csi deployment — a **single TrueNAS
> backend serving all nodes equally** — topology is unnecessary: every node is
> equidistant from the one backend, so there is nothing to constrain. Leave it
> disabled (the default). Topology matters only when nodes are partitioned into
> zones/regions that map to *different* storage backends or network locality you want
> to honor.

## What gets advertised

Enabling `node.topology` makes each node plugin publish these segments from its config
(`pkg/driver/config.go` `TopologyConfig`):

| Config key | Advertised topology key |
|------------|-------------------------|
| `node.topology.zone` | `topology.kubernetes.io/zone` |
| `node.topology.region` | `topology.kubernetes.io/region` |
| `node.topology.customLabels` (map) | each key/value verbatim |

The controller echoes the same segments as the volume's `AccessibleTopology`, which
external-provisioner records on the `PV` as `nodeAffinity`.

## Enabling it

```yaml
# values.yaml
node:
  topology:
    enabled: true
    zone: zone-a          # this DaemonSet's nodes are in zone-a
    region: us-west-1
    customLabels: {}
```

Then constrain provisioning in the StorageClass:

```yaml
volumeBindingMode: WaitForFirstConsumer   # delay binding until a pod is scheduled
allowedTopologies:
  - matchLabelExpressions:
      - key: topology.kubernetes.io/zone
        values: [zone-a]
```

`WaitForFirstConsumer` is what makes topology useful: the volume is provisioned only
after the pod is scheduled, so its `AccessibleTopology` matches the node the pod landed
on. With the default `Immediate` binding, the volume is provisioned before scheduling
and topology has no effect on placement.

## Important limitation: topology values are static per deployment

The `zone`/`region` come from the **node plugin's config**, which is a single value for
the whole DaemonSet. There is no per-node auto-detection. So a genuinely **multi-zone**
cluster requires one of:

- **Per-zone node pools, one DaemonSet each** — deploy scale-csi once per zone
  (separate release/values) with each pinned (via `nodeSelector`/affinity) to that
  zone's nodes and its own `node.topology.zone`. This is the correct pattern for
  multiple backends.
- **A single zone label for all nodes** — only meaningful if you also run multiple
  backends and select them another way.

Because of this, and because the single-backend model makes all nodes equivalent,
**topology is an advanced/niche feature for scale-csi.** Most deployments should leave
`node.topology.enabled: false`. Enable it only when you run distinct backends per zone
and want the scheduler to honor that locality.

See also: [StorageClass reference](../reference/storageclass.md) ·
[Production](../production.md).
