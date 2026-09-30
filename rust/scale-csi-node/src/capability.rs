//! What a stage or publish request asks for, reduced to what idempotency
//! compares (`pkg/driver/node.go`: nodeCapabilityForRequest, volumeMountFlags,
//! normalizeMountSource, stageSourceIdentity, nodeAttachDriver): a repeated
//! request with the same signature is the same operation, a different one on
//! the same path is a conflict.

use std::collections::HashMap;

use tonic::Status;

use crate::csi;
use crate::locks::go_clean;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    Mount,
    Block,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    pub access_type: AccessType,
    /// csi.VolumeCapability.AccessMode.Mode; 0 (UNKNOWN) when absent.
    pub access_mode: i32,
    /// Lower-cased; empty when the request names none.
    pub fs_type: String,
    /// De-duplicated, trimmed and sorted, joined with ",".
    pub mount_flags: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareType {
    Nfs,
    Iscsi,
    Nvmeof,
}

pub fn signature(capability: Option<&csi::VolumeCapability>) -> Result<Signature, Status> {
    use csi::volume_capability::AccessType as Csi;
    let capability = capability.ok_or_else(|| Status::invalid_argument("volume capability is required"))?;
    // A capability with neither block nor mount is a filesystem mount, as the
    // Go node (and older COs that omitted the oneof) treat it.
    let (access_type, fs_type) = match &capability.access_type {
        Some(Csi::Block(_)) => (AccessType::Block, String::new()),
        Some(Csi::Mount(m)) => (AccessType::Mount, m.fs_type.to_lowercase()),
        None => (AccessType::Mount, String::new()),
    };
    let mut flags = mount_flags(capability);
    flags.sort();
    Ok(Signature {
        access_type,
        access_mode: capability.access_mode.as_ref().map_or(0, |m| m.mode),
        fs_type,
        mount_flags: flags.join(","),
    })
}

/// The request's mount flags, trimmed, empties dropped, first occurrence kept.
pub fn mount_flags(capability: &csi::VolumeCapability) -> Vec<String> {
    let Some(csi::volume_capability::AccessType::Mount(mount)) = &capability.access_type else {
        return Vec::new();
    };
    let mut out: Vec<String> = Vec::with_capacity(mount.mount_flags.len());
    for flag in &mount.mount_flags {
        let flag = flag.trim();
        if !flag.is_empty() && !out.iter().any(|f| f == flag) {
            out.push(flag.to_string());
        }
    }
    out
}

/// Mount flags for a filesystem: xfs always gets `nouuid`, since clones share
/// the source's XFS UUID.
pub fn mount_flags_for_fs(capability: &csi::VolumeCapability, fs_type: &str) -> Vec<String> {
    let mut flags = mount_flags(capability);
    if fs_type.eq_ignore_ascii_case("xfs") && !flags.iter().any(|f| f.eq_ignore_ascii_case("nouuid")) {
        flags.push("nouuid".into());
    }
    flags
}

/// A mount source as the mount table shows it and as compared: trimmed, a
/// trailing `[subpath]` dropped (not a leading `[` of an IPv6 NFS source),
/// absolute paths cleaned.
pub fn normalize_mount_source(source: &str) -> String {
    let mut source = source.trim();
    if let Some(bracket) = source.find('[')
        && bracket > 0
        && source.ends_with(']')
    {
        source = &source[..bracket];
    }
    if source.starts_with('/') {
        go_clean(source)
    } else {
        source.to_string()
    }
}

pub fn mount_sources_equal(left: &str, right: &str) -> bool {
    normalize_mount_source(left) == normalize_mount_source(right)
}

/// `server:share`, IPv6 servers bracketed.
pub fn nfs_source(address: &str, share: &str) -> String {
    if address.contains(':') {
        format!("[{address}]:{share}")
    } else {
        format!("{address}:{share}")
    }
}

/// The identity a staged volume must show: `server:share` for NFS,
/// `iscsi:<iqn>`, `nvmeof:<nqn>`.
pub fn stage_source_identity(share: ShareType, context: &HashMap<String, String>) -> Result<String, Status> {
    let get = |k: &str| context.get(k).map(String::as_str).unwrap_or_default();
    match share {
        ShareType::Nfs => {
            let (server, path) = (get("server"), get("share"));
            if server.is_empty() || path.is_empty() {
                return Err(Status::invalid_argument(
                    "NFS server and share are required in volume context",
                ));
            }
            Ok(normalize_mount_source(&nfs_source(server, path)))
        }
        ShareType::Iscsi if !get("iqn").is_empty() => Ok(format!("iscsi:{}", get("iqn"))),
        ShareType::Iscsi => Err(Status::invalid_argument("iSCSI IQN is required in volume context")),
        ShareType::Nvmeof if !get("nqn").is_empty() => Ok(format!("nvmeof:{}", get("nqn"))),
        ShareType::Nvmeof => Err(Status::invalid_argument("NVMe-oF NQN is required in volume context")),
    }
}

/// The protocol a volume is attached with: the volume context's
/// `node_attach_driver` (case-insensitive; anything unknown is NFS), else the
/// driver name's default (`org.scale.csi.{nfs,iscsi,nvmeof}`, anything else NFS).
pub fn attach_driver(context: &HashMap<String, String>, driver_name: &str) -> ShareType {
    let parse = |s: &str| match s.trim().to_lowercase().as_str() {
        "iscsi" => ShareType::Iscsi,
        "nvmeof" => ShareType::Nvmeof,
        _ => ShareType::Nfs,
    };
    match context.get("node_attach_driver").filter(|v| !v.is_empty()) {
        Some(v) => parse(v),
        None => match driver_name {
            "org.scale.csi.iscsi" => ShareType::Iscsi,
            "org.scale.csi.nvmeof" => ShareType::Nvmeof,
            _ => ShareType::Nfs,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use csi::volume_capability::{AccessMode, AccessType as Csi, BlockVolume, MountVolume};

    fn mount(fs: &str, flags: &[&str], mode: i32) -> csi::VolumeCapability {
        csi::VolumeCapability {
            access_type: Some(Csi::Mount(MountVolume {
                fs_type: fs.into(),
                mount_flags: flags.iter().map(|s| s.to_string()).collect(),
                volume_mount_group: String::new(),
            })),
            access_mode: Some(AccessMode { mode }),
        }
    }

    #[test]
    fn signatures() {
        let a = signature(Some(&mount("EXT4", &[" noatime", "discard", "noatime", ""], 7))).unwrap();
        assert_eq!(
            a,
            Signature {
                access_type: AccessType::Mount,
                access_mode: 7,
                fs_type: "ext4".into(),
                mount_flags: "discard,noatime".into()
            }
        );
        let b = signature(Some(&mount("ext4", &["discard", "noatime"], 7))).unwrap();
        assert_eq!(a, b, "order, case of the fs type and duplicates do not matter");
        let block = csi::VolumeCapability {
            access_type: Some(Csi::Block(BlockVolume {})),
            access_mode: None,
        };
        let s = signature(Some(&block)).unwrap();
        assert_eq!((s.access_type, s.access_mode), (AccessType::Block, 0));
        let neither = csi::VolumeCapability {
            access_type: None,
            access_mode: Some(AccessMode { mode: 1 }),
        };
        assert_eq!(signature(Some(&neither)).unwrap().access_type, AccessType::Mount);
        assert_eq!(signature(None).unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn xfs_gets_nouuid_once() {
        assert_eq!(
            mount_flags_for_fs(&mount("xfs", &["noatime"], 1), "XFS"),
            ["noatime", "nouuid"]
        );
        assert_eq!(mount_flags_for_fs(&mount("xfs", &["NOUUID"], 1), "xfs"), ["NOUUID"]);
        assert_eq!(mount_flags_for_fs(&mount("ext4", &[], 1), "ext4"), Vec::<String>::new());
    }

    #[test]
    fn mount_sources() {
        assert_eq!(normalize_mount_source(" /dev/ublkb3 "), "/dev/ublkb3");
        assert_eq!(normalize_mount_source("/dev/mapper/x[/sub]"), "/dev/mapper/x");
        assert_eq!(normalize_mount_source("/a//b/../c/"), "/a/c");
        assert_eq!(
            normalize_mount_source("[2001:db8::1]:/mnt/share"),
            "[2001:db8::1]:/mnt/share"
        );
        assert_eq!(normalize_mount_source("192.0.2.1:/mnt/share"), "192.0.2.1:/mnt/share");
        assert!(mount_sources_equal("/dev//nvme0n1", "/dev/nvme0n1/"));
    }

    #[test]
    fn stage_identities_and_attach_drivers() {
        let ctx = |pairs: &[(&str, &str)]| {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>()
        };
        assert_eq!(
            stage_source_identity(ShareType::Nvmeof, &ctx(&[("nqn", "nqn.x")])).unwrap(),
            "nvmeof:nqn.x"
        );
        assert_eq!(
            stage_source_identity(ShareType::Iscsi, &ctx(&[("iqn", "iqn.x")])).unwrap(),
            "iscsi:iqn.x"
        );
        assert_eq!(
            stage_source_identity(ShareType::Nfs, &ctx(&[("server", "2001:db8::1"), ("share", "/mnt/s")])).unwrap(),
            "[2001:db8::1]:/mnt/s"
        );
        assert!(stage_source_identity(ShareType::Nvmeof, &ctx(&[])).is_err());
        assert!(stage_source_identity(ShareType::Nfs, &ctx(&[("server", "a")])).is_err());
        assert_eq!(
            attach_driver(&ctx(&[("node_attach_driver", " NVMeoF ")]), "csi.scale.io"),
            ShareType::Nvmeof
        );
        assert_eq!(
            attach_driver(&ctx(&[("node_attach_driver", "fc")]), "org.scale.csi.nvmeof"),
            ShareType::Nfs
        );
        assert_eq!(attach_driver(&ctx(&[]), "org.scale.csi.iscsi"), ShareType::Iscsi);
        assert_eq!(attach_driver(&ctx(&[]), "csi.scale.io"), ShareType::Nfs);
    }
}
