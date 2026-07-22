# Nomad support

Nomad can run scale-csi as separate controller and node plugins. Current Nomad
releases can dynamically create and register a CSI volume with `nomad volume
create` when the plugin implements the Controller service; externally created
volumes can still be added with `nomad volume register`.

This integration is community-level and has been exercised only with NFS. Test
the exact Nomad, container runtime, protocol, and host-tool combination before
production use.

## Driver contract

The binary accepts these relevant flags:

| Flag | Purpose |
|---|---|
| `-config=/path/config.yaml` | Strict driver YAML (required) |
| `-endpoint=unix:///csi/csi.sock` | CSI gRPC endpoint |
| `-node-id=<stable-id>` | Node identity (node mode) |
| `-driver-name=csi.scale.io` | Unified CSI driver name |
| `-mode=controller` or `-mode=node` | Service set |
| `-health-port=9809` | Readiness/metrics port; `0` disables HTTP |
| `-v=2` | klog verbosity |
| `-version` | Print version and exit |

There are no `--csi-version`, `--csi-name`, `--driver-config-file`,
`--log-level`, `--csi-mode`, `--server-address`, or `--server-port` flags. The
plugin speaks over the Unix socket mounted by Nomad's `csi_plugin` block.

Start from one complete strict config in [`examples/`](../examples). The same
non-secret configuration must be available to controller and node tasks. Only
the controller needs `TRUENAS_API_KEY`; inject it through Nomad Variables or
Vault instead of embedding it in the job file.

## Controller plugin job

This job assumes the complete config is stored on clients at
`/opt/scale-csi/config.yaml`. Replace the image tag with the release you
verified.

```hcl
job "scale-csi-controller" {
  datacenters = ["dc1"]
  type        = "service"

  group "controller" {
    count = 1

    task "scale-csi" {
      driver = "docker"

      config {
        image = "ghcr.io/gizmotickler/scale-csi:v1.2.23"
        args = [
          "-config=/etc/scale-csi/config.yaml",
          "-endpoint=unix:///csi/csi.sock",
          "-driver-name=csi.scale.io",
          "-mode=controller",
          "-health-port=9809",
          "-v=2",
        ]
        volumes = [
          "/opt/scale-csi/config.yaml:/etc/scale-csi/config.yaml:ro",
        ]
      }

      # Supply this with a template { env = true } block backed by Nomad
      # Variables or Vault in a real job.
      env {
        TRUENAS_API_KEY = "replace-through-a-secret-provider"
      }

      csi_plugin {
        id             = "scale-csi"
        type           = "controller"
        mount_dir      = "/csi"
        health_timeout = "30s"
      }

      resources {
        cpu    = 50
        memory = 256
      }
    }
  }
}
```

## Node plugin job

The node plugin must be privileged and able to see the host device, sysfs,
udev, mount, and protocol state. Paths below match the Helm deployment and may
need runtime-specific mount-propagation settings.

```hcl
job "scale-csi-node" {
  datacenters = ["dc1"]
  type        = "system"

  constraint {
    operator = "distinct_hosts"
    value    = true
  }

  group "node" {
    task "scale-csi" {
      driver = "docker"

      config {
        image      = "ghcr.io/gizmotickler/scale-csi:v1.2.23"
        privileged = true
        args = [
          "-config=/etc/scale-csi/config.yaml",
          "-endpoint=unix:///csi/csi.sock",
          "-node-id=${node.unique.name}",
          "-driver-name=csi.scale.io",
          "-mode=node",
          "-health-port=9809",
          "-v=2",
        ]
        volumes = [
          "/opt/scale-csi/config.yaml:/etc/scale-csi/config.yaml:ro",
          "/:/host:ro",
          "/dev:/dev",
          "/sys:/sys",
          "/run/udev:/run/udev",
          "/etc/iscsi:/etc/iscsi",
          "/var/lib/iscsi:/var/lib/iscsi",
        ]
      }

      csi_plugin {
        id                     = "scale-csi"
        type                   = "node"
        mount_dir              = "/csi"
        stage_publish_base_dir = "/local/csi"
        health_timeout         = "30s"
      }

      resources {
        cpu    = 50
        memory = 256
      }
    }
  }
}
```

## Dynamically create a volume

`parameters.protocol` is required whenever the driver config enables more than
one protocol. This NFS volume file is accepted by `nomad volume create`:

```hcl
id           = "shared-media"
name         = "shared-media"
type         = "csi"
plugin_id    = "scale-csi"
capacity_min = "100GiB"
capacity_max = "100GiB"

capability {
  access_mode     = "multi-node-multi-writer"
  attachment_mode = "file-system"
}

mount_options {
  mount_flags = ["nfsvers=4", "noatime"]
}

parameters {
  protocol = "nfs"
}
```

```bash
nomad volume create shared-media.hcl
nomad volume status shared-media
```

For iSCSI or NVMe-oF use `protocol = "iscsi"` or `"nvmeof"`, a
single-node access mode, and optionally `fs_type = "ext4"` or `"xfs"` under
`mount_options`.

The NFS volume context returned by scale-csi contains
`node_attach_driver`, `server`, and `share`. Block volumes return the matching
transport fields (`iqn`/`portal`/`lun` or
`nqn`/`transport`/`address`/`port`). The stale `provisioner_driver` context key
is not emitted and must not be added to static registrations.
