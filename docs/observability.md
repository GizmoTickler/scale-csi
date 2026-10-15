# Observability

How to get metrics, alerts, dashboards, and logs out of scale-csi. The
meaning of each metric and alert threshold is in
[Production › Monitoring](production.md#monitoring); what to do when an alert
fires is in [Troubleshooting › Alerts → Runbook](troubleshooting.md#alerts--runbook).

## Endpoints

| Endpoint | Where | Port |
|---|---|---|
| `/metrics` (Prometheus) | Controller and node driver containers | `metrics.port` (default `9809`) |
| `/readyz` (readiness; on the controller this includes TrueNAS reachability) | Controller and node driver containers | `metrics.port` |
| `/healthz` (process liveness, independent of TrueNAS) | `liveness-probe` sidecar | `9808` |

With `metrics.enabled: true` (the default) the chart creates a
`<release>-controller-metrics` Service and a headless `<release>-node-metrics`
Service selecting those ports. Node pods use host networking, so the node
port is bound on every node.

## Prometheus Operator

```yaml
metrics:
  serviceMonitor:
    enabled: true
    # Match your Prometheus's serviceMonitorSelector
    labels:
      release: kube-prometheus-stack
    interval: 30s
  prometheusRule:
    enabled: true
    additionalLabels:
      release: kube-prometheus-stack
  dashboards:
    # ConfigMap labeled grafana_dashboard: "1" for the Grafana sidecar
    enabled: true
```

The ServiceMonitor and PrometheusRule kinds need the Prometheus Operator CRDs
(`monitoring.coreos.com/v1`). `metrics.prometheusRule.rules` replaces the
bundled rules when non-empty; thresholds for the pool-usage and tombstone
alerts have their own values (see the
[chart README](../charts/scale-csi/README.md#workloads-rbac-and-metrics)).

Some alerts only exist when their feature is on, because they need its
metrics:

| Alert | Needs |
|---|---|
| `ScaleCSIPoolNearFull` | `capacity.gaugeEnabled: true` |
| `ScaleCSIVolumeNearQuota` | `zfs.reportVolumeUsage: true` |
| Tombstone reap alerts | `reconcile.delete.enabled: true` |
| Scheduled-snapshot alerts | `zfs.snapshotSchedule` or a class `snapshotSchedule` |

## Plain Prometheus

Without the operator, scrape the metrics Services with annotations:

```yaml
metrics:
  service:
    annotations:
      prometheus.io/scrape: "true"
      prometheus.io/port: "9809"
```

## Signals worth watching first

| Question | Metric |
|---|---|
| Is the controller connected to TrueNAS? | `scale_csi_truenas_connection_status` |
| Are CSI operations failing? | `scale_csi_operations_total{status="error"}` by `operation` |
| Is TrueNAS slow? | `scale_csi_truenas_requests_duration_seconds` |
| Is the pool filling up? | `scale_csi_pool_available_bytes` / `scale_csi_pool_capacity_bytes` (with `capacity.gaugeEnabled`) |
| Are orphans or tombstones accumulating? | `scale_csi_orphan_volumes`, `scale_csi_tombstone_oldest_age_seconds` |

The full list, with the semantics of each label, is in
[Production › Monitoring](production.md#monitoring).

## Logs

```bash
kubectl -n scale-csi logs deploy/scale-csi-controller -c scale-csi
kubectl -n scale-csi logs ds/scale-csi-node -c scale-csi          # one pod
kubectl -n scale-csi logs -l app.kubernetes.io/component=node -c scale-csi --prefix
```

Raise verbosity with `logging.verbosity` (default `2`; higher values add
detail, and `5` dumps CSI requests with their secrets stripped). Credentials are never
logged at any level.

For profiling, an opt-in pprof endpoint exists; read
[Debug endpoint](production.md#debug-endpoint-debuglistenaddress) before
enabling it.
