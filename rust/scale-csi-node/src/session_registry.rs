//! The NVMe-oF sessions this node plugin connected (`pkg/driver/session_registry.go`):
//! session GC only ever disconnects a session recorded here, never one an
//! administrator or another initiator made to the same portals.
//!
//! Compatible with the Go node's files, so either can take over: one empty file
//! per NQN under `<socket dir>/sessions/nvmeof`, named by the NQN's bytes in hex
//! (NQNs contain ':' and may contain '/'), its modification time the last time
//! it was recorded. A stage records before it connects; a disconnect forgets.

use std::collections::HashMap;
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result, bail};

#[derive(Debug, Clone)]
pub struct SessionRegistry {
    dir: PathBuf,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(name: &str) -> Option<String> {
    if name.is_empty() || !name.len().is_multiple_of(2) {
        return None;
    }
    let bytes: Option<Vec<u8>> = (0..name.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&name[i..i + 2], 16).ok())
        .collect();
    String::from_utf8(bytes?).ok()
}

impl SessionRegistry {
    /// The registry beside the CSI socket; it must be an absolute path.
    pub fn beside(socket: &Path) -> Result<Self> {
        let dir = socket
            .parent()
            .context("the socket has a directory")?
            .join("sessions/nvmeof");
        Self::at(dir)
    }

    pub fn at(dir: PathBuf) -> Result<Self> {
        if !dir.is_absolute() {
            bail!("session registry directory {} is not an absolute path", dir.display());
        }
        Ok(SessionRegistry { dir })
    }

    fn path(&self, id: &str) -> PathBuf {
        self.dir.join(hex(id.as_bytes()))
    }

    /// Records `id` as connected by this plugin (or refreshes its time).
    pub fn record(&self, id: &str) -> Result<()> {
        if id.is_empty() {
            bail!("session registry unavailable");
        }
        let path = self.path(id);
        if std::fs::File::options()
            .write(true)
            .open(&path)
            .and_then(|f| f.set_modified(SystemTime::now()))
            .is_ok()
        {
            return Ok(());
        }
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&self.dir)
            .with_context(|| format!("record session {id}"))?;
        let tmp = path.with_extension("tmp");
        let written = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .and_then(|mut f| {
                f.flush()?;
                f.sync_all()
            })
            .and_then(|()| std::fs::rename(&tmp, &path));
        if let Err(e) = written {
            let _ = std::fs::remove_file(&tmp);
            return Err(e).with_context(|| format!("record session {id}"));
        }
        Ok(())
    }

    /// Forgets `id`; a missing entry is fine.
    pub fn forget(&self, id: &str) -> Result<()> {
        if id.is_empty() {
            return Ok(());
        }
        match std::fs::remove_file(self.path(id)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("forget session {id}")),
        }
    }

    /// Whether `id` was recorded; any error is "no" (without proof of
    /// ownership, GC leaves a session alone).
    pub fn has(&self, id: &str) -> bool {
        !id.is_empty() && std::fs::metadata(self.path(id)).is_ok()
    }

    /// Every recorded identity with its last record time.
    pub fn entries(&self) -> Result<HashMap<String, SystemTime>> {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(e) => return Err(e).context("read session registry"),
        };
        let mut out = HashMap::new();
        for entry in entries.flatten() {
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };
            if name.ends_with(".tmp") {
                continue;
            }
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                continue;
            }
            let (Some(id), Ok(modified)) = (unhex(&name), meta.modified()) else {
                continue;
            };
            out.insert(id, modified);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_like_the_go_node() {
        let dir = tempfile::tempdir().unwrap();
        let registry = SessionRegistry::beside(&dir.path().join("csi.sock")).unwrap();
        let nqn = "nqn.2011-06.com.example:uuid:1/pvc-1";
        assert!(!registry.has(nqn));
        registry.record(nqn).unwrap();
        // Go: filepath.Join(dir, hex.EncodeToString([]byte(id))).
        let file = dir
            .path()
            .join("sessions/nvmeof")
            .join("6e716e2e323031312d30362e636f6d2e6578616d706c653a757569643a312f7076632d31");
        assert!(file.is_file(), "the Go node's file name");
        assert_eq!(std::fs::metadata(&file).unwrap().len(), 0);
        assert!(registry.has(nqn));

        // A re-record refreshes the time.
        let old = SystemTime::now() - std::time::Duration::from_secs(3600);
        std::fs::File::options()
            .write(true)
            .open(&file)
            .unwrap()
            .set_modified(old)
            .unwrap();
        registry.record(nqn).unwrap();
        assert!(std::fs::metadata(&file).unwrap().modified().unwrap() > old);

        // Entries skip temporaries, directories and names that are not hex.
        std::fs::write(dir.path().join("sessions/nvmeof/6e71.tmp"), b"").unwrap();
        std::fs::write(dir.path().join("sessions/nvmeof/zz"), b"").unwrap();
        std::fs::create_dir(dir.path().join("sessions/nvmeof/6e71")).unwrap();
        let entries = registry.entries().unwrap();
        assert_eq!(entries.keys().collect::<Vec<_>>(), [nqn]);

        registry.forget(nqn).unwrap();
        registry.forget(nqn).unwrap();
        assert!(!registry.has(nqn));
        assert!(SessionRegistry::at(PathBuf::from("relative")).is_err());
        let empty = SessionRegistry::at(dir.path().join("none")).unwrap();
        assert!(empty.entries().unwrap().is_empty());
    }
}
