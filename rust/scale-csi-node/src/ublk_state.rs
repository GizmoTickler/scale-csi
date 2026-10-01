//! Node-side state of the ublk data path that must stay compatible with the Go
//! node (`pkg/driver/node_nvmeublk.go`, `node_identity.go`), so either can
//! unstage what the other staged:
//!
//! - the marker: `<socket dir>/scale-csi/<first 16 bytes of sha256(driver
//!   name, NUL, volume id) in hex>`, content `driver\nvolume\n`, file 0600 in
//!   a 0700 directory, written before an attach (an attach that times out may
//!   still complete in the daemon, and unstage must know to detach);
//! - the NVMe host identity the daemon connects with: the node's own host NQN,
//!   as its node id carries it (the one fencing admits), and the host ID the
//!   kernel's `nvme connect` would present.

use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use crate::node_id;

const MARKER_DIR: &str = "scale-csi";

pub fn marker_path(socket: &Path, driver: &str, volume: &str) -> PathBuf {
    let mut hash = Sha256::new();
    hash.update(driver.as_bytes());
    hash.update([0u8]);
    hash.update(volume.as_bytes());
    let digest = hash.finalize();
    let name: String = digest[..16].iter().map(|b| format!("{b:02x}")).collect();
    socket.parent().unwrap_or(Path::new("/")).join(MARKER_DIR).join(name)
}

pub fn write_marker(socket: &Path, driver: &str, volume: &str) -> Result<()> {
    let path = marker_path(socket, driver, volume);
    let dir = path.parent().expect("a marker has a directory");
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
        .context("create ublk marker directory")?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&path)
        .context("write ublk marker")?;
    file.write_all(format!("{driver}\n{volume}\n").as_bytes())
        .context("write ublk marker")?;
    Ok(())
}

pub fn marker_exists(socket: &Path, driver: &str, volume: &str) -> Result<bool> {
    match std::fs::metadata(marker_path(socket, driver, volume)) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e).context("check ublk marker"),
    }
}

pub fn remove_marker(socket: &Path, driver: &str, volume: &str) -> Result<()> {
    match std::fs::remove_file(marker_path(socket, driver, volume)) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e).context("remove ublk marker"),
    }
}

/// `(host NQN, host ID)` for the daemon, from this node's own node id.
pub fn host_identity(node_id: &str, host_id_files: &[PathBuf]) -> Result<(String, String)> {
    let identity = node_id::parse(node_id).context("decode this node's identity")?;
    let nqn = identity.nvme_nqn.trim().to_string();
    if nqn.is_empty() {
        bail!(
            "this node reported no NVMe host NQN at startup (nvme show-hostnqn); the daemon must connect with the node's own NQN, which is what publication fencing admits"
        );
    }
    let id = host_id(&nqn, host_id_files)?;
    Ok((nqn, id))
}

/// The host ID `nvme connect` would present: the first non-empty host ID file
/// (a malformed one is an error, not a fallback: the kernel would present it),
/// else the UUID of a `...:uuid:<id>` host NQN.
pub fn host_id(host_nqn: &str, files: &[PathBuf]) -> Result<String> {
    for path in files {
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        let raw = text.trim();
        if raw.is_empty() {
            continue;
        }
        return canonical_uuid(raw)
            .with_context(|| format!("{} does not hold a UUID host ID: {raw:?}", path.display()));
    }
    if let Some((_, uuid)) = host_nqn.trim().split_once(":uuid:")
        && let Some(id) = canonical_uuid(uuid)
    {
        return Ok(id);
    }
    bail!("no NVMe host ID: /etc/nvme/hostid is absent and host NQN {host_nqn:?} is not UUID-based")
}

/// Lower-case 8-4-4-4-12 from 32 hex digits with dashes anywhere or none.
pub fn canonical_uuid(raw: &str) -> Option<String> {
    let hex: String = raw.to_lowercase().chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    ))
}

/// The default host ID files: the host root the node pod mounts at /host, then
/// an image that bind-mounts /etc/nvme itself.
pub fn default_host_id_files() -> Vec<PathBuf> {
    vec![
        PathBuf::from("/host/etc/nvme/hostid"),
        PathBuf::from("/etc/nvme/hostid"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn the_marker_is_where_and_what_the_go_node_writes() {
        let dir = tempfile::tempdir().unwrap();
        let socket = dir.path().join("nvmeublkd.sock");
        let path = marker_path(&socket, "csi.scale.io", "pvc-0b1c2d3e");
        // The name the Go node computes for the same driver and volume.
        assert_eq!(path, dir.path().join("scale-csi/b0e11c7edeea8ec2914ff149c673ec18"));
        assert!(!marker_exists(&socket, "csi.scale.io", "pvc-0b1c2d3e").unwrap());
        write_marker(&socket, "csi.scale.io", "pvc-0b1c2d3e").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "csi.scale.io\npvc-0b1c2d3e\n");
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(
            std::fs::metadata(path.parent().unwrap()).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert!(marker_exists(&socket, "csi.scale.io", "pvc-0b1c2d3e").unwrap());
        remove_marker(&socket, "csi.scale.io", "pvc-0b1c2d3e").unwrap();
        remove_marker(&socket, "csi.scale.io", "pvc-0b1c2d3e").unwrap();
        assert!(!marker_exists(&socket, "csi.scale.io", "pvc-0b1c2d3e").unwrap());
        assert_ne!(
            marker_path(&socket, "other.driver", "pvc-0b1c2d3e"),
            path,
            "the driver name salts it"
        );
    }

    #[test]
    fn host_ids() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (dir.path().join("a"), dir.path().join("b"));
        let files = [a.clone(), b.clone()];
        let nqn = "nqn.2014-08.org.nvmexpress:uuid:0B1C2D3E-4F50-4A61-8B72-C3D4E5F60718";
        assert_eq!(
            host_id(nqn, &files).unwrap(),
            "0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718",
            "derived from the NQN"
        );
        std::fs::write(&b, "  \n").unwrap();
        assert_eq!(
            host_id(nqn, &files).unwrap(),
            "0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718",
            "an empty file is skipped"
        );
        std::fs::write(&b, "9A8B7C6D5E4F4A3B9C2D1E0F2A3B4C5D\n").unwrap();
        assert_eq!(
            host_id(nqn, &files).unwrap(),
            "9a8b7c6d-5e4f-4a3b-9c2d-1e0f2a3b4c5d",
            "the file wins"
        );
        std::fs::write(&a, "not-a-uuid").unwrap();
        assert!(
            host_id(nqn, &files).is_err(),
            "a malformed file is an error, not a fallback"
        );
        assert!(host_id("nqn.2014-08.com.example:host", &[]).is_err());
        assert_eq!(
            canonical_uuid("0b1c-2d3e4f504a61-8b72c3d4e5f6-0718").as_deref(),
            Some("0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718")
        );
        assert_eq!(canonical_uuid("0b1c2d3e4f504a618b72c3d4e5f6071g"), None);
    }

    #[test]
    fn host_identity_comes_from_this_nodes_id() {
        let nqn = "nqn.2014-08.org.nvmexpress:uuid:0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718";
        let id = node_id::encode(&node_id::NodeIdentity {
            name: "k8s-0".into(),
            nvme_nqn: nqn.into(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            host_identity(&id, &[]).unwrap(),
            (nqn.to_string(), "0b1c2d3e-4f50-4a61-8b72-c3d4e5f60718".to_string())
        );
        let no_nqn = node_id::encode(&node_id::NodeIdentity {
            name: "k8s-0".into(),
            ..Default::default()
        })
        .unwrap();
        assert!(host_identity(&no_nqn, &[]).is_err());
    }
}
