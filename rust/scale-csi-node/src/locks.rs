//! Per-volume and per-target operation locks, as the Go node takes them
//! (`acquireOperationLock`): a try-lock, so a second RPC for a key already in
//! flight fails at once with Aborted and kubelet retries it; nothing queues.
//! Keys: `node:<volume id>` and `node-target:<cleaned target path>`; the
//! controller's `volume:` keys are a different keyspace.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

#[derive(Default, Clone)]
pub struct OperationLocks(Arc<Mutex<HashSet<String>>>);

/// Held while the operation runs; dropping it releases the key.
pub struct Held {
    locks: OperationLocks,
    key: String,
}

impl OperationLocks {
    pub fn try_lock(&self, key: String) -> Option<Held> {
        let mut held = self.0.lock().expect("lock table");
        if !held.insert(key.clone()) {
            return None;
        }
        Some(Held {
            locks: self.clone(),
            key,
        })
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        self.locks.0.lock().expect("lock table").remove(&self.key);
    }
}

pub fn node_volume_key(volume_id: &str) -> String {
    format!("node:{volume_id}")
}

pub fn node_target_key(target_path: &str) -> String {
    format!("node-target:{}", go_clean(target_path))
}

/// Go's `filepath.Clean` (Unix): the shortest lexically equivalent path. No
/// symlinks are resolved and nothing is read from disk.
pub fn go_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".into();
    }
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|p| *p != "..") {
                    parts.pop();
                } else if !rooted {
                    parts.push("..");
                }
            }
            p => parts.push(p),
        }
    }
    let joined = parts.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".into(),
        (false, false) => joined,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_key_refuses_a_second_operation_until_released() {
        let locks = OperationLocks::default();
        let first = locks.try_lock(node_volume_key("v1")).expect("free");
        assert!(locks.try_lock(node_volume_key("v1")).is_none(), "in flight");
        assert!(locks.try_lock(node_volume_key("v2")).is_some(), "another volume");
        assert!(
            locks.try_lock("volume:v1".into()).is_some(),
            "the controller keyspace is separate"
        );
        drop(first);
        assert!(locks.try_lock(node_volume_key("v1")).is_some(), "released on drop");
    }

    #[test]
    fn target_keys_are_cleaned_like_go() {
        let t = "/var/lib/kubelet/pods/x/volumes/kubernetes.io~csi/pvc/mount";
        assert_eq!(node_target_key(&format!("{t}/")), node_target_key(t));
        assert_eq!(node_target_key(&format!("{t}/./")), format!("node-target:{t}"));
    }

    #[test]
    fn clean_matches_go_filepath_clean() {
        // Go's filepath.Clean gives exactly these.
        for (input, want) in [
            ("", "."),
            ("/", "/"),
            ("//", "/"),
            ("/a/b/", "/a/b"),
            ("/a//b", "/a/b"),
            ("/a/./b", "/a/b"),
            ("/a/../b", "/b"),
            ("/../a", "/a"),
            ("a/../..", ".."),
            ("./a", "a"),
            ("a/b/../../..", ".."),
            ("..", ".."),
            (".", "."),
            ("a/..", "."),
            ("/a/b/c/../../d/./e//", "/a/d/e"),
        ] {
            assert_eq!(go_clean(input), want, "{input:?}");
        }
    }
}
