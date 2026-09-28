//! Bulk admission gate (noisy-neighbour isolation; design doc
//! nvmeublk-userspace-architecture.md, "Build log: isolation round").
//!
//! What a latency-sensitive small request waits for when bulk transfers
//! share the node is the target, not the daemon: with 7 x 1 MiB QD16
//! aggressors the victim's 4 KiB read spent 19.8 ms (p99) between its
//! capsule going out and its first data coming back, against 0.64 ms in
//! the daemon (fetch, submit, commit) (run iso d1-sep7: per-I/O trace
//! joined with fio's per-I/O log). The target's cost grows with the bulk
//! commands it holds, not with the bytes it moves: 112 x 1 MiB outstanding
//! kept nas01 at ~33 busy cores for 4.0 GB/s, 14 outstanding at ~15 cores
//! for 3.7 GB/s, and the victim's p99 fell from 20-35 ms to 1.5 ms (d2).
//!
//! So while small commands are being served anywhere in the process (the
//! node daemon), each target path admits at most `cap` bytes of bulk
//! commands (payload > SMALL_IO) at a time, counted over every engine of
//! every volume; the rest wait in their engine, in order, until bulk
//! completions make room. Without small traffic the gate is open, so bulk-
//! only workloads keep their full depth. Progress: an engine with no bulk
//! command of its own in flight is always admitted one, so every waiting
//! engine has a completion of its own coming that re-checks its queue
//! (no cross-thread wake-up needed), and the bound is soft by at most one
//! command per engine.
//!
//! NVMEUBLK_BULK_GATE_KB: the per-path cap (0 = gate off);
//! NVMEUBLK_BULK_GATE_MS: how long after the last small command the gate
//! stays closed.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Per-path cap by default (bytes; 0 = off).
pub const DEFAULT_CAP_KB: u64 = 0;
/// The gate stays closed this long after the last small command by default.
pub const DEFAULT_RECENT_MS: u64 = 50;

/// Bulk bytes in flight on one target path, over the whole process.
#[derive(Default)]
pub struct PathGate {
    pub bytes: AtomicI64,
}

static GATES: LazyLock<Mutex<HashMap<SocketAddr, Arc<PathGate>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// The process-wide gate of the path to `addr`.
pub fn for_path(addr: SocketAddr) -> Arc<PathGate> {
    GATES.lock().unwrap_or_else(PoisonError::into_inner).entry(addr).or_default().clone()
}

#[cfg(test)]
thread_local! {
    /// Tests: (cap, recent) for engines on this thread, whatever the
    /// environment.
    pub(crate) static TEST_CONFIG: std::cell::Cell<Option<(i64, Duration)>> = const { std::cell::Cell::new(None) };
}

/// (per-path cap in bytes, recent window); cap 0 = off.
pub fn config() -> (i64, Duration) {
    #[cfg(test)]
    if let Some(c) = TEST_CONFIG.with(|c| c.get()) {
        return c;
    }
    static C: LazyLock<(i64, Duration)> = LazyLock::new(|| {
        (
            (crate::env_u64("NVMEUBLK_BULK_GATE_KB", DEFAULT_CAP_KB) << 10) as i64,
            Duration::from_millis(crate::env_u64("NVMEUBLK_BULK_GATE_MS", DEFAULT_RECENT_MS)),
        )
    });
    *C
}

fn mono_ns() -> u64 {
    static BASE: LazyLock<Instant> = LazyLock::new(Instant::now);
    BASE.elapsed().as_nanos() as u64 + 1
}

/// When a small command was last dispatched in this process (mono ns, 0 =
/// never).
static LAST_SMALL: AtomicU64 = AtomicU64::new(0);

/// A small command is being dispatched. Stored at most once a millisecond
/// (the line is shared by every reactor).
pub fn note_small() {
    let (now, last) = (mono_ns(), LAST_SMALL.load(Ordering::Relaxed));
    if last == 0 || now.saturating_sub(last) > 1_000_000 {
        LAST_SMALL.store(now, Ordering::Relaxed);
    }
}

/// Whether the gate is closed now (small commands within `recent`).
pub fn closed(recent: Duration) -> bool {
    let last = LAST_SMALL.load(Ordering::Relaxed);
    last != 0 && mono_ns().saturating_sub(last) < recent.as_nanos() as u64
}

/// Whether a bulk command of `len` bytes may go on a path holding
/// `path_bytes` bulk bytes now: always while the gate is open or while the
/// engine has none of its own in flight (progress), otherwise within `cap`.
pub fn admits(closed: bool, own_bulk: usize, path_bytes: i64, len: usize, cap: i64) -> bool {
    !closed || cap <= 0 || own_bulk == 0 || path_bytes + len as i64 <= cap
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_gate_admits_within_the_cap_and_always_one_per_engine() {
        let cap = 4 << 20;
        let mib = 1 << 20;
        assert!(admits(false, 5, 100 * mib as i64, mib, cap), "open gate: no limit");
        assert!(admits(true, 1, 3 * mib as i64, mib, cap), "room for one more");
        assert!(!admits(true, 1, 3 * mib as i64 + 1, mib, cap), "full");
        assert!(admits(true, 0, 100 * mib as i64, mib, cap), "an engine with nothing in flight always gets one (progress)");
        assert!(admits(true, 3, 100 * mib as i64, mib, 0), "cap 0 = off");
    }

    #[test]
    fn paths_share_one_gate_per_address() {
        let a: SocketAddr = "192.0.2.1:4420".parse().unwrap();
        let b: SocketAddr = "192.0.2.2:4420".parse().unwrap();
        let (g1, g2, g3) = (for_path(a), for_path(a), for_path(b));
        assert!(Arc::ptr_eq(&g1, &g2));
        assert!(!Arc::ptr_eq(&g1, &g3));
    }
}
