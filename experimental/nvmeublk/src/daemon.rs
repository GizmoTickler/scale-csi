//! Per-node daemon: one process owns every nvmeublk device on the node and is
//! driven over a local control socket (scale-csi's node plugin is the client).
//!
//! Lifecycle:
//! - attach/detach create and delete devices; each device is served by its
//!   own queue threads (device.rs). Operations on one volume are serialised;
//!   operations on different volumes do not wait for each other.
//! - Every device is recorded in a state file under /run (so it survives a
//!   daemon restart but not a reboot, which also removes the devices).
//! - On start, every recorded device that still exists is reattached through
//!   ublk user recovery, in the background and in parallel: the control
//!   socket serves at once, and a volume whose target is unreachable keeps
//!   retrying without holding up the others. The kernel holds a device's I/O
//!   until it is reattached. Until then the volume is listed as recovering,
//!   and attach and detach of it fail so that the caller retries.
//! - SIGTERM/SIGINT is a *handover*, not a teardown: new I/O parks, in-flight
//!   commands finish (bounded), each device is marked clean, and the process
//!   exits leaving the devices in place. A clean device is recovered without
//!   the write hold; after a crash (not clean) its writes are held for one
//!   fence, because the dead daemon's writes may still land on the target.
//!
//! Protocol: one JSON request per connection, one JSON response line.
//!   {"op":"attach", ...DeviceSpec}   -> {"ok":true,"dev_id":N,"path":"/dev/ublkbN"}
//!   {"op":"detach","volume":"..."}   -> {"ok":true}
//!   {"op":"list"}                    -> {"ok":true,"devices":[...]}
//!                                       (a volume still being recovered has
//!                                       "recovering":true, its paths down)
//!   {"op":"stats","volume":"..."}    -> {"ok":true,"stats":{...}}

use crate::device::{self, DeviceSpec, Running};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

pub const DEFAULT_SOCKET: &str = "/run/nvmeublk/nvmeublkd.sock";
pub const DEFAULT_STATE: &str = "/run/nvmeublk/state.json";

/// How many recovered devices are brought up at once after a restart.
/// Bringing one up starts all its queue threads, and each of those a connect
/// thread per path, at once; with many volumes that burst is bounded here.
/// A volume still waiting for a reachable path holds no lane.
const RECOVERY_LANES: usize = 4;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct StateEntry {
    spec: DeviceSpec,
    dev_id: i32,
    /// Set only by a graceful handover that drained every in-flight command.
    #[serde(default)]
    clean: bool,
}

/// Lock order where both maps are held: `devices`, then `recovering`.
struct Daemon {
    devices: Mutex<BTreeMap<String, Running>>,
    /// Devices a previous daemon left behind that are not served again yet
    /// (see `recover`), as they were recorded.
    recovering: Mutex<BTreeMap<String, StateEntry>>,
    state_path: String,
    /// Serialises the operations on one volume (attach, detach, recovery) so
    /// two requests for it cannot race.
    ops: VolumeLocks,
    lanes: Lanes,
    /// Serialises writing the state file.
    saving: Mutex<()>,
}

/// The state file's entries. A served device is clean only when `clean` says
/// so (a handover drained it). A device not served again yet keeps the flag
/// it was recorded with: this daemon has sent nothing for it, so what its
/// last server left still holds.
fn state_entries<'a>(
    serving: impl Iterator<Item = (&'a DeviceSpec, i32)>,
    recovering: impl Iterator<Item = &'a StateEntry>,
    clean: &BTreeMap<String, bool>,
) -> Vec<StateEntry> {
    serving
        .map(|(spec, dev_id)| StateEntry { spec: spec.clone(), dev_id, clean: *clean.get(&spec.volume).unwrap_or(&false) })
        .chain(recovering.cloned())
        .collect()
}

/// Per-volume operation locks: two operations on one volume never overlap,
/// operations on different volumes never wait for each other. A volume is in
/// `busy` only while an operation holds its lock.
#[derive(Default)]
struct VolumeLocks {
    busy: Mutex<BTreeSet<String>>,
    released: Condvar,
}

struct VolumeOp<'a> {
    locks: &'a VolumeLocks,
    volume: String,
}

impl VolumeLocks {
    fn lock(&self, volume: &str) -> VolumeOp<'_> {
        let mut busy = self.busy.lock().unwrap_or_else(PoisonError::into_inner);
        while busy.contains(volume) {
            busy = self.released.wait(busy).unwrap_or_else(PoisonError::into_inner);
        }
        busy.insert(volume.to_string());
        VolumeOp { locks: self, volume: volume.to_string() }
    }
}

impl Drop for VolumeOp<'_> {
    /// Also on unwinding: a panicking operation must not wedge its volume.
    fn drop(&mut self) {
        self.locks.busy.lock().unwrap_or_else(PoisonError::into_inner).remove(&self.volume);
        self.locks.released.notify_all();
    }
}

/// A counting semaphore (see RECOVERY_LANES).
struct Lanes {
    free: Mutex<usize>,
    released: Condvar,
}

struct Lane<'a>(&'a Lanes);

impl Lanes {
    fn new(n: usize) -> Self {
        Lanes { free: Mutex::new(n.max(1)), released: Condvar::new() }
    }

    fn acquire(&self) -> Lane<'_> {
        let mut free = self.free.lock().unwrap_or_else(PoisonError::into_inner);
        while *free == 0 {
            free = self.released.wait(free).unwrap_or_else(PoisonError::into_inner);
        }
        *free -= 1;
        Lane(self)
    }
}

impl Drop for Lane<'_> {
    fn drop(&mut self) {
        *self.0.free.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        self.0.released.notify_one();
    }
}

impl Daemon {
    fn new(state_path: &str) -> Self {
        Daemon {
            devices: Mutex::new(BTreeMap::new()),
            recovering: Mutex::new(BTreeMap::new()),
            state_path: state_path.to_string(),
            ops: VolumeLocks::default(),
            lanes: Lanes::new(RECOVERY_LANES),
            saving: Mutex::new(()),
        }
    }

    fn save(&self, clean: &BTreeMap<String, bool>) {
        // One writer at a time, snapshot taken inside: concurrent operations
        // must not interleave on the temporary file, and the last write must
        // carry the latest state.
        let _w = self.saving.lock().unwrap_or_else(PoisonError::into_inner);
        let entries = {
            let devs = self.devices.lock().unwrap();
            let rec = self.recovering.lock().unwrap();
            state_entries(devs.values().map(|r| (&r.spec, r.dev_id)), rec.values(), clean)
        };
        let tmp = format!("{}.tmp", self.state_path);
        let data = serde_json::to_vec_pretty(&entries).unwrap_or_default();
        if std::fs::write(&tmp, data).and_then(|_| std::fs::rename(&tmp, &self.state_path)).is_err() {
            log::error!("could not write state file {}", self.state_path);
        }
    }

    fn attach(&self, spec: DeviceSpec) -> Result<Value> {
        let _op = self.ops.lock(&spec.volume);
        {
            let devs = self.devices.lock().unwrap();
            if let Some(r) = devs.get(&spec.volume) {
                if r.spec.subnqn != spec.subnqn {
                    anyhow::bail!("volume {} is already attached to a different subsystem ({})", spec.volume, r.spec.subnqn);
                }
                // Idempotent: NodeStage retries must get the same device back.
                return Ok(json!({"ok": true, "dev_id": r.dev_id, "path": r.path(), "existing": true}));
            }
            // Its I/O is held until then; handing it out now would hang the caller.
            if let Some(e) = self.recovering.lock().unwrap().get(&spec.volume) {
                anyhow::bail!("volume {} is still being recovered (ublk device {}); retry", spec.volume, e.dev_id);
            }
        }
        let vol = spec.volume.clone();
        let r = device::start(spec, None)?;
        let out = json!({"ok": true, "dev_id": r.dev_id, "path": r.path()});
        self.devices.lock().unwrap().insert(vol, r);
        self.save(&BTreeMap::new());
        Ok(out)
    }

    fn detach(&self, volume: &str) -> Result<Value> {
        let _op = self.ops.lock(volume);
        let r = {
            let mut devs = self.devices.lock().unwrap();
            match devs.remove(volume) {
                Some(r) => r,
                None if self.recovering.lock().unwrap().contains_key(volume) => anyhow::bail!("volume {volume} is still being recovered; retry"),
                // Idempotent: NodeUnstage retries after success must succeed.
                None => return Ok(json!({"ok": true, "absent": true})),
            }
        };
        self.save(&BTreeMap::new());
        r.detach()?;
        Ok(json!({"ok": true}))
    }

    fn list(&self) -> Value {
        let devs = self.devices.lock().unwrap();
        let mut list: Vec<Value> = devs
            .values()
            .map(|r| {
                let paths: Vec<Value> = r.ctrls.paths.iter().map(|p| json!({"addr": p.addr.to_string(), "up": p.cntlid().is_some()})).collect();
                json!({"volume": r.spec.volume, "subnqn": r.spec.subnqn, "dev_id": r.dev_id, "path": r.path(), "paths": paths})
            })
            .collect();
        // The device node exists and belongs to the volume; nothing serves it yet.
        list.extend(self.recovering.lock().unwrap().values().map(|e| {
            let paths: Vec<Value> = e.spec.addrs.iter().map(|a| json!({"addr": a, "up": false})).collect();
            json!({"volume": e.spec.volume, "subnqn": e.spec.subnqn, "dev_id": e.dev_id, "path": format!("/dev/ublkb{}", e.dev_id), "paths": paths, "recovering": true})
        }));
        json!({"ok": true, "devices": list})
    }

    fn stats(&self, volume: &str) -> Result<Value> {
        let devs = self.devices.lock().unwrap();
        if !devs.contains_key(volume) && self.recovering.lock().unwrap().contains_key(volume) {
            anyhow::bail!("volume {volume} is still being recovered");
        }
        let r = devs.get(volume).with_context(|| format!("volume {volume} is not attached"))?;
        let s = &r.stats;
        let l = |a: &std::sync::atomic::AtomicU64| a.load(Ordering::Relaxed);
        Ok(json!({"ok": true, "stats": {
            "inflight": s.inflight.load(Ordering::Relaxed), "done": l(&s.done),
            "failovers": l(&s.failovers), "resubmits": l(&s.resubmits), "parked": l(&s.parked), "fenced": l(&s.fenced),
            "path_errors": l(&s.path_errors), "protocol_errors": l(&s.protocol_errors), "no_path_eio": l(&s.no_path_eio),
            "reconnects": l(&s.reconnects), "stall_kills": l(&s.stall_kills), "epoch_kills": l(&s.epoch_kills),
            "zc_rx_bytes": l(&s.zc_bytes), "zc_tx_bytes": l(&s.zc_tx_bytes)
        }}))
    }

    /// Graceful handover: park new I/O everywhere, wait (bounded) for what is
    /// on the wire, record which devices drained, and exit without deleting
    /// any device.
    fn handover(&self) -> ! {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut clean = BTreeMap::new();
        loop {
            let devs = self.devices.lock().unwrap();
            let all = devs.values().all(|r| r.drain());
            if all || Instant::now() > deadline {
                for r in devs.values() {
                    clean.insert(r.spec.volume.clone(), r.stats.inflight.load(Ordering::Acquire) <= 0);
                }
                drop(devs);
                break;
            }
            drop(devs);
            std::thread::sleep(Duration::from_millis(20));
        }
        self.save(&clean);
        let dirty: Vec<&String> = clean.iter().filter(|(_, c)| !**c).map(|(v, _)| v).collect();
        let recovering = self.recovering.lock().unwrap().len();
        log::info!(
            "handover: {} device(s) left for the next daemon ({recovering} not yet recovered); not drained (writes will be held on recovery): {:?}",
            clean.len() + recovering,
            dirty
        );
        std::process::exit(0)
    }

    /// Serve again a device a previous daemon left behind. Each such device
    /// has its own short-lived thread for this, so a volume whose target is
    /// unreachable delays only itself: it retries in the background, holding
    /// no lane and no lock, while the other volumes recover and the control
    /// socket serves.
    fn recover(&self, e: StateEntry) {
        let vol = e.spec.volume.clone();
        let ctrls = match device::connect(&e.spec, true) {
            Ok(c) => c,
            // Only an address that does not resolve gets here.
            Err(err) => return self.recovered(&vol, Err(err.context(format!("could not reattach ublk device {}", e.dev_id)))),
        };
        let _lane = self.lanes.acquire();
        let _op = self.ops.lock(&vol);
        // From here on this daemon may send the device's I/O, so a crash from
        // now on must hold its writes on the next recovery. Recorded before
        // recovery starts.
        if let Some(p) = self.recovering.lock().unwrap().get_mut(&vol) {
            p.clean = false;
        }
        self.save(&BTreeMap::new());
        let r = device::start_connected(e.spec, ctrls, Some((e.dev_id, !e.clean)));
        self.recovered(&vol, r.with_context(|| format!("could not reattach ublk device {}", e.dev_id)));
    }

    /// End of `recover`: the volume is served, or dropped from the state as a
    /// failed reattach always was. Both maps change in one step, so no state
    /// snapshot sees it twice or not at all.
    fn recovered(&self, vol: &str, r: Result<Running>) {
        {
            let mut devs = self.devices.lock().unwrap();
            self.recovering.lock().unwrap().remove(vol);
            match r {
                Ok(r) => {
                    log::info!("{vol}: reattached /dev/ublkb{}", r.dev_id);
                    devs.insert(vol.to_string(), r);
                }
                Err(err) => log::error!("{vol}: {err:#}"),
            }
        }
        self.save(&BTreeMap::new());
    }
}

fn handle(d: &Daemon, stream: UnixStream) {
    let mut w = match stream.try_clone() {
        Ok(w) => w,
        Err(_) => return,
    };
    let mut line = String::new();
    if BufReader::new(stream).read_line(&mut line).is_err() {
        return;
    }
    let resp = (|| -> Result<Value> {
        let req: Value = serde_json::from_str(line.trim()).context("request is not JSON")?;
        match req.get("op").and_then(Value::as_str) {
            Some("attach") => d.attach(serde_json::from_value(req.clone()).context("bad attach request")?),
            Some("detach") => d.detach(req.get("volume").and_then(Value::as_str).context("detach needs volume")?),
            Some("list") => Ok(d.list()),
            Some("stats") => d.stats(req.get("volume").and_then(Value::as_str).context("stats needs volume")?),
            other => anyhow::bail!("unknown op {other:?}"),
        }
    })()
    .unwrap_or_else(|e| json!({"ok": false, "error": format!("{e:#}")}));
    let _ = writeln!(w, "{resp}");
}

pub fn run(socket: &str, state_path: &str) -> Result<()> {
    std::fs::create_dir_all("/run/nvmeublk")?;
    let d = Arc::new(Daemon::new(state_path));

    // What a previous daemon left behind is reattached in the background
    // (below); until then it stays recorded as it was.
    let previous: Vec<StateEntry> = std::fs::read(state_path).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
    for e in previous {
        if libublk::ctrl::UblkCtrl::new_simple(e.dev_id).is_err() {
            log::warn!("{}: ublk device {} is gone; dropping it from state", e.spec.volume, e.dev_id);
            continue;
        }
        d.recovering.lock().unwrap().insert(e.spec.volume.clone(), e);
    }
    d.save(&BTreeMap::new());

    let _ = std::fs::remove_file(socket);
    let listener = UnixListener::bind(socket).with_context(|| format!("bind {socket}"))?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    let dh = d.clone();
    ctrlc::set_handler(move || dh.handover())?;
    // Every device recovers on its own thread, all at once (bounded by the
    // lanes): none of them, and not the control socket, waits for another's
    // target.
    let todo: Vec<StateEntry> = d.recovering.lock().unwrap().values().cloned().collect();
    let n = todo.len();
    for e in todo {
        let (dr, vol) = (d.clone(), e.spec.volume.clone());
        if let Err(err) = std::thread::Builder::new().name(format!("recov-{}", device::short(&vol))).spawn(move || dr.recover(e)) {
            // It stays recorded, so the next daemon start recovers it.
            log::error!("{vol}: cannot start its recovery ({err}); left for the next daemon start");
        }
    }
    log::info!("nvmeublkd listening on {socket} ({n} device(s) being recovered)");
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                let d = d.clone();
                std::thread::spawn(move || handle(&d, s));
            }
            Err(e) => log::warn!("control socket accept: {e}"),
        }
    }
    Ok(())
}

/// `nvmeublk ctl <json>`: send one request and print the response.
pub fn ctl(socket: &str, request: &str) -> Result<()> {
    let mut s = UnixStream::connect(socket).with_context(|| format!("connect {socket}"))?;
    writeln!(s, "{request}")?;
    let mut resp = String::new();
    BufReader::new(s).read_line(&mut resp)?;
    print!("{resp}");
    let v: Value = serde_json::from_str(resp.trim()).unwrap_or(Value::Null);
    if v.get("ok") != Some(&Value::Bool(true)) {
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn entry(volume: &str, dev_id: i32, clean: bool) -> StateEntry {
        let spec = serde_json::from_value(json!({"volume": volume, "subnqn": "nqn.2026-09.test:sub", "addrs": ["192.0.2.1:4420"]})).unwrap();
        StateEntry { spec, dev_id, clean }
    }

    fn state_path(test: &str) -> String {
        std::env::temp_dir().join(format!("nvmeublk-{}-{test}.json", std::process::id())).to_string_lossy().into_owned()
    }

    #[test]
    fn one_volume_waits_for_itself_only() {
        let locks = Arc::new(VolumeLocks::default());
        let a = locks.lock("a");
        let (tx, rx) = mpsc::channel();
        let l = locks.clone();
        std::thread::spawn(move || {
            let _b = l.lock("b");
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(5)).expect("an operation on volume b waited for volume a");
        let (tx, rx) = mpsc::channel();
        let l = locks.clone();
        let second = std::thread::spawn(move || {
            let _a = l.lock("a");
            tx.send(()).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err(), "two operations on volume a overlapped");
        drop(a);
        rx.recv_timeout(Duration::from_secs(5)).expect("volume a was never released");
        second.join().unwrap();
        assert!(locks.busy.lock().unwrap().is_empty());
    }

    #[test]
    fn a_panicking_operation_releases_its_volume() {
        let locks = Arc::new(VolumeLocks::default());
        let l = locks.clone();
        assert!(std::thread::spawn(move || {
            let _a = l.lock("a");
            panic!("operation failed");
        })
        .join()
        .is_err());
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _a = locks.lock("a");
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(5)).expect("volume a stayed locked after a panic");
    }

    #[test]
    fn lanes_bound_concurrent_recoveries() {
        let lanes = Arc::new(Lanes::new(2));
        let (a, b) = (lanes.acquire(), lanes.acquire());
        let (tx, rx) = mpsc::channel();
        let l = lanes.clone();
        let third = std::thread::spawn(move || {
            let _c = l.acquire();
            tx.send(()).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err(), "a third lane was handed out");
        drop(a);
        rx.recv_timeout(Duration::from_secs(5)).expect("a released lane was not handed out");
        drop(b);
        third.join().unwrap();
        assert_eq!(*lanes.free.lock().unwrap(), 2);
    }

    /// A volume still being recovered stays in the state file with the flag
    /// it was recorded with, whatever a save (or a handover) says about the
    /// served ones; a served device is clean only when drained.
    #[test]
    fn state_keeps_unrecovered_volumes_and_their_flags() {
        let path = state_path("state");
        let d = Daemon::new(&path);
        d.recovering.lock().unwrap().insert("v1".into(), entry("v1", 7, true));
        d.recovering.lock().unwrap().insert("v2".into(), entry("v2", 8, false));
        d.save(&BTreeMap::from([("v1".to_string(), false), ("v2".to_string(), true)]));
        let back: Vec<StateEntry> = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(back, vec![entry("v1", 7, true), entry("v2", 8, false)]);

        let (s3, s4) = (entry("v3", 1, false).spec, entry("v4", 2, false).spec);
        let served = [(&s3, 1), (&s4, 2)];
        let out = state_entries(served.into_iter(), std::iter::empty(), &BTreeMap::from([("v3".to_string(), true)]));
        assert_eq!(out, vec![entry("v3", 1, true), entry("v4", 2, false)]);
    }

    /// While its recovery runs in the background the volume is visible and
    /// owned: attach and detach fail (the caller retries) instead of adding
    /// a second device or reporting it absent; other volumes are unaffected.
    #[test]
    fn recovering_volume_is_listed_and_refuses_attach_and_detach() {
        let path = state_path("recovering");
        let d = Daemon::new(&path);
        d.recovering.lock().unwrap().insert("v1".into(), entry("v1", 7, true));
        let list = d.list();
        let dev = &list["devices"][0];
        assert_eq!((dev["volume"].as_str(), dev["dev_id"].as_i64(), dev["path"].as_str()), (Some("v1"), Some(7), Some("/dev/ublkb7")));
        assert_eq!((dev["recovering"].as_bool(), dev["paths"][0]["up"].as_bool()), (Some(true), Some(false)));
        let err = d.attach(entry("v1", 0, false).spec).unwrap_err();
        assert!(format!("{err:#}").contains("being recovered"), "{err:#}");
        let err = d.detach("v1").unwrap_err();
        assert!(format!("{err:#}").contains("being recovered"), "{err:#}");
        assert!(d.stats("v1").is_err());
        assert_eq!(d.detach("v2").unwrap()["absent"], true);
        assert!(d.recovering.lock().unwrap().contains_key("v1"));
        let _ = std::fs::remove_file(&path);
    }
}
