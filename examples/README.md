# Examples

Self-contained, commented examples for installing scale-csi and using its
volumes. Hostnames, IPs, and NQNs are placeholders
(`truenas.example.com`, `192.0.2.10`, `nqn…:uuid:0000…`); replace them with
yours. New to scale-csi? Follow [Getting started](../docs/getting-started.md)
first: it covers the TrueNAS and node preparation these files assume.

Every Helm values file here is rendered against the chart in this repository
and schema-validated, and every manifest is checked against the Kubernetes API
schemas (see [Validating the examples](#validating-the-examples)).

## Installing with Helm — [`helm/`](helm/)

| File | What it sets up |
|---|---|
| [`values-nfs.yaml`](helm/values-nfs.yaml) | Minimal NFS install with one `scale-nfs` class |
| [`values-iscsi.yaml`](helm/values-iscsi.yaml) | Minimal iSCSI install with one `scale-iscsi` class |
| [`values-nvmeof.yaml`](helm/values-nvmeof.yaml) | Minimal NVMe/TCP install with a host-NQN allowlist |
| [`values-multi-protocol.yaml`](helm/values-multi-protocol.yaml) | NFS + iSCSI + NVMe-oF classes, a detached-restore class, and a VolumeSnapshotClass |
| [`values-production.yaml`](helm/values-production.yaml) | Two controller replicas with a PDB, explicit resources, CA-pinned TLS, capacity tracking, ServiceMonitor, PrometheusRule, Grafana dashboard, and the Rust node agent |
| [`values-encryption.yaml`](helm/values-encryption.yaml) | An NFS class with ZFS-native encryption at rest (pair with [`workloads/encryption-secret.yaml`](workloads/encryption-secret.yaml)) |
| [`values-second-instance.yaml`](helm/values-second-instance.yaml) | A second driver instance for a second TrueNAS or failure domain |

All values files assume an existing API-key Secret named `truenas-api-key`:

```bash
kubectl create namespace scale-csi
kubectl label namespace scale-csi pod-security.kubernetes.io/enforce=privileged
kubectl -n scale-csi create secret generic truenas-api-key \
  --from-literal=api-key='<your-truenas-api-key>'
```

Install, upgrade, and remove:

```bash
# Install the latest release (add --version X.Y.Z to pin one)
helm install scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi --values examples/helm/values-nfs.yaml

# Preview what an upgrade changes (needs the helm-diff plugin), then apply it
helm diff upgrade scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi --values examples/helm/values-nfs.yaml
helm upgrade scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi --values examples/helm/values-nfs.yaml

# Inspect the chart's defaults
helm show values oci://ghcr.io/gizmotickler/charts/scale-csi

# Remove the driver (volumes on TrueNAS are not deleted)
helm uninstall scale-csi --namespace scale-csi
```

Before upgrading across releases read [Upgrades](../docs/guides/upgrades.md).
Every key is documented in the [chart README](../charts/scale-csi/README.md).

## Installing with GitOps — [`gitops/`](gitops/)

| File | Tool |
|---|---|
| [`flux/scale-csi.yaml`](gitops/flux/scale-csi.yaml) | Flux 2.6+: Namespace, `OCIRepository`, and `HelmRelease` (with CRD upgrades enabled) |
| [`argocd/application.yaml`](gitops/argocd/application.yaml) | Argo CD `Application` sourcing the public OCI chart |

Both expect the API-key Secret to be provided by your secret tooling
(SOPS, Sealed Secrets, External Secrets); neither file contains credentials.

## Using volumes — [`workloads/`](workloads/)

Each file's header lists the commands to apply it, check it, and remove it.
They use the StorageClass names from the Helm examples above.

| File | Shows |
|---|---|
| [`nfs-pvc-pod.yaml`](workloads/nfs-pvc-pod.yaml) | NFS PVC + pod (smoke test) |
| [`iscsi-pvc-pod.yaml`](workloads/iscsi-pvc-pod.yaml) | iSCSI PVC + pod (smoke test) |
| [`nvmeof-pvc-pod.yaml`](workloads/nvmeof-pvc-pod.yaml) | NVMe-oF PVC + pod (smoke test) |
| [`nfs-rwx-deployment.yaml`](workloads/nfs-rwx-deployment.yaml) | One `ReadWriteMany` NFS volume shared by three replicas on different nodes |
| [`raw-block.yaml`](workloads/raw-block.yaml) | `volumeMode: Block` — an unformatted device in the pod |
| [`volume-expansion.yaml`](workloads/volume-expansion.yaml) | Growing a mounted volume with `kubectl patch` |
| [`snapshot-restore.yaml`](workloads/snapshot-restore.yaml) | `VolumeSnapshotClass`, a `VolumeSnapshot`, and a PVC restored from it |
| [`pvc-clone.yaml`](workloads/pvc-clone.yaml) | Cloning a PVC directly from another PVC |
| [`statefulset.yaml`](workloads/statefulset.yaml) | A StatefulSet with one iSCSI volume per replica |
| [`encryption-secret.yaml`](workloads/encryption-secret.yaml) | The passphrase Secret for an encrypted StorageClass |

More StorageClass variants (NFS ACLs, Kerberos, CHAP, performance classes,
scheduled snapshots, ublk) are in the
[StorageClass reference](../docs/reference/storageclass.md).

## Without Helm — top-level `scale-*.yaml`

[`scale-nfs.yaml`](scale-nfs.yaml), [`scale-iscsi.yaml`](scale-iscsi.yaml),
and [`scale-nvmeof.yaml`](scale-nvmeof.yaml) are complete driver
configuration files (the format the chart renders into its ConfigMap), for
running the binary directly — for example under
[Nomad](../docs/guides/nomad.md). Kubernetes users do not need them. A unit
test (`TestRepositoryExampleConfigsParseStrictly`) loads exactly these three
files with the driver's strict parser, so keep any other example out of the
top level of this directory.

## Validating the examples

What the maintainers run before changing these files:

```bash
# Every Helm values file renders and passes the chart's JSON schema
for f in examples/helm/values-*.yaml; do
  helm template scale-csi charts/scale-csi --namespace scale-csi \
    --api-versions monitoring.coreos.com/v1 --values "$f" > /dev/null || echo "FAILED: $f"
done

# Rendered charts and workload manifests match the Kubernetes API schemas
# (kubeconform: https://github.com/yannh/kubeconform)
for f in examples/helm/values-*.yaml; do
  helm template scale-csi charts/scale-csi --namespace scale-csi \
    --api-versions monitoring.coreos.com/v1 --values "$f" |
    kubeconform -strict -summary -ignore-missing-schemas
done
kubeconform -strict -summary \
  -schema-location default \
  -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
  examples/workloads examples/gitops
```
