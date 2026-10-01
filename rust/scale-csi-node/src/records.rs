//! What this process staged and published, by target path (Go
//! `nodeMountRecord`, `stagedTargets`, `publishedTargets`). In memory only: a
//! restart rebuilds them from the live mounts as requests replay, and they only
//! ever add a refusal (a path recorded for one volume is never handed to
//! another), never skip a check.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::capability::Signature;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountRecord {
    pub volume_id: String,
    pub target_path: String,
    pub expected_source: String,
    pub live_source: String,
    pub capability: Signature,
    pub readonly: bool,
}

#[derive(Default)]
pub struct Records {
    staged: Mutex<HashMap<String, MountRecord>>,
    published: Mutex<HashMap<String, MountRecord>>,
}

impl Records {
    pub fn stage(&self, target: &str) -> Option<MountRecord> {
        self.staged.lock().expect("records").get(target).cloned()
    }

    pub fn store_stage(&self, record: MountRecord) {
        self.staged
            .lock()
            .expect("records")
            .insert(record.target_path.clone(), record);
    }

    pub fn delete_stage(&self, target: &str) {
        self.staged.lock().expect("records").remove(target);
    }

    pub fn is_stage_target(&self, target: &str) -> bool {
        self.staged.lock().expect("records").contains_key(target)
    }

    pub fn publication(&self, target: &str) -> Option<MountRecord> {
        self.published.lock().expect("records").get(target).cloned()
    }

    pub fn publications(&self) -> Vec<MountRecord> {
        self.published.lock().expect("records").values().cloned().collect()
    }

    pub fn store_publication(&self, record: MountRecord) {
        self.published
            .lock()
            .expect("records")
            .insert(record.target_path.clone(), record);
    }

    pub fn delete_publication(&self, target: &str) {
        self.published.lock().expect("records").remove(target);
    }
}
