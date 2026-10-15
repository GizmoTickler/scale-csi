# Encryption at rest

scale-csi can create volumes as ZFS-natively encrypted datasets or zvols, with
a passphrase you keep in a Kubernetes Secret. It works for all three
protocols. This page is the task-oriented walkthrough; the full contract
(Secret keys, rotation, inherited keys, failure modes) is in the
[StorageClass reference › Encryption at rest](../reference/storageclass.md#encryption-at-rest).

## Understand the trade-off first

- **A TrueNAS reboot locks every encrypted volume.** TrueNAS does not store a
  passphrase key, so after a reboot the controller's reconciler has to unlock
  each volume again. Until it does, pods on those volumes get I/O errors. See
  [the availability model](../reference/storageclass.md#-a-locked-dataset-serves-zero-io-the-availability-model).
- **Losing the Secret is permanent data loss.** There is no escrow on the
  TrueNAS side. Back the Secret up somewhere that does not depend on the
  cluster.
- **Encryption is chosen at creation.** An existing plaintext volume cannot be
  encrypted in place (and vice versa).
- **No snapshot restores or clones across encryption.** An encrypted class
  cannot restore from a snapshot or clone a PVC, and an encrypted volume
  cannot be the source of a restore or clone into any class.
- **Rolling back** to a driver release without encryption support while
  encrypted volumes exist is unsafe; see
  [the rollback warning](../reference/storageclass.md#-do-not-roll-back-below-encryption-support).

If your pool or parent dataset is already encrypted by TrueNAS, you may not
need this feature at all; see
[Encryption the driver manages vs. encryption your pool already has](../reference/storageclass.md#encryption-the-driver-manages-vs-encryption-your-pool-already-has).

## 1. Create the passphrase Secret

One Secret per encrypted StorageClass, shared by every volume of that class.
The only required key is `passphrase` (at least 8 characters):

```bash
PASSPHRASE="$(openssl rand -base64 32)"
kubectl -n scale-csi create secret generic scale-nfs-encrypted \
  --from-literal=passphrase="${PASSPHRASE}"
# Store ${PASSPHRASE} in your password manager or vault now.
```

The optional keys (a previous passphrase for rotation, key-derivation tuning,
and others) are described in
[The encryption Secret](../reference/storageclass.md#the-encryption-secret).
A file version is in
[`examples/workloads/encryption-secret.yaml`](../../examples/workloads/encryption-secret.yaml).

## 2. Enable encryption and add an encrypted class

In your Helm values, opt the controller in and add a class that names the
Secret:

```yaml
encryption:
  enabled: true

storageClasses:
  - name: scale-nfs-encrypted
    protocol: nfs
    encryptionSecretName: scale-nfs-encrypted
    encryptionSecretNamespace: ""     # defaults to the release namespace
    isDefault: false
    reclaimPolicy: Delete
    allowVolumeExpansion: true
    volumeBindingMode: Immediate
    mountOptions: [nfsvers=4.1, hard, noatime]
    extraParameters: {}
```

Setting `encryptionSecretName` makes the chart add `encryption: "true"` and the
CSI secret references (provisioner, controller-publish, node-stage) to the
class, and grants the controller permission to read Secrets. The chart never
reads or renders the passphrase. A complete file is
[`examples/helm/values-encryption.yaml`](../../examples/helm/values-encryption.yaml).

```bash
helm upgrade --install scale-csi oci://ghcr.io/gizmotickler/charts/scale-csi \
  --namespace scale-csi --values values.yaml
```

The same works for `iscsi` and `nvmeof` classes.

## 3. Create and check an encrypted volume

```bash
sed 's/scale-nfs-test/scale-enc-test/; s/storageClassName: scale-nfs/storageClassName: scale-nfs-encrypted/' \
  examples/workloads/nfs-pvc-pod.yaml | kubectl apply -f -
kubectl wait --for=condition=Ready pod/scale-enc-test --timeout=180s
kubectl exec scale-enc-test -- cat /data/hello.txt
```

On TrueNAS the dataset reports encryption:

```bash
PV=$(kubectl get pvc scale-enc-test -o jsonpath='{.spec.volumeName}')
midclt call pool.dataset.query "[[\"id\", \"=\", \"tank/k8s/volumes/${PV}\"]]" \
  | jq '.[0] | {id, encrypted, locked, key_format: .key_format.value}'
```

## Operating encrypted volumes

- **After a TrueNAS reboot**, watch the controller unlock volumes before you
  start their workloads (see [Upgrading TrueNAS](upgrades.md#upgrading-truenas)).
- **Rotating the passphrase** uses the Secret's previous-passphrase key; follow
  [Key rotation](../reference/storageclass.md#key-rotation).
