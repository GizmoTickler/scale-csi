//! Per-node daemon: one process owns every nvmeublk device on the node and is
//! driven over a local control socket (scale-csi's node plugin is the client).
//!
//! Lifecycle:
//! - attach/detach create and delete devices; each device is served by its
//!   own thread group (device.rs).
//! - Every device is recorded in a state file under /run (so it survives a
//!   daemon restart but not a reboot, which also removes the devices).
//! - On start, every recorded device that still exists is reattached through
//!   ublk user recovery. The kernel held its I/O meanwhile.
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
//!   {"op":"stats","volume":"..."}    -> {"ok":true,"stats":{...}}

use crate::device::{self, DeviceSpec, Running};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const DEFAULT_SOCKET: &str = "/run/nvmeublk/nvmeublkd.sock";
pub const DEFAULT_STATE: &str = "/run/nvmeublk/state.json";

#[derive(Serialize, Deserialize, Clone)]
struct StateEntry {
    spec: DeviceSpec,
    dev_id: i32,
    /// Set only by a graceful handover that drained every in-flight command.
    #[serde(default)]
    clean: bool,
}

struct Daemon {
    devices: Mutex<BTreeMap<String, Running>>,
    state_path: String,
    /// Serialises attach/detach so two requests for one volume cannot race.
    ops: Mutex<()>,
}

impl Daemon {
    fn save(&self, clean: &BTreeMap<String, bool>) {
        let devs = self.devices.lock().unwrap();
        let entries: Vec<StateEntry> = devs
            .values()
            .map(|r| StateEntry { spec: r.spec.clone(), dev_id: r.dev_id, clean: *clean.get(&r.spec.volume).unwrap_or(&false) })
            .collect();
        let tmp = format!("{}.tmp", self.state_path);
        let data = serde_json::to_vec_pretty(&entries).unwrap_or_default();
        if std::fs::write(&tmp, data).and_then(|_| std::fs::rename(&tmp, &self.state_path)).is_err() {
            log::error!("could not write state file {}", self.state_path);
        }
    }

    fn attach(&self, spec: DeviceSpec) -> Result<Value> {
        let _g = self.ops.lock().unwrap();
        if let Some(r) = self.devices.lock().unwrap().get(&spec.volume) {
            if r.spec.subnqn != spec.subnqn {
                anyhow::bail!("volume {} is already attached to a different subsystem ({})", spec.volume, r.spec.subnqn);
            }
            // Idempotent: NodeStage retries must get the same device back.
            return Ok(json!({"ok": true, "dev_id": r.dev_id, "path": r.path(), "existing": true}));
        }
        let vol = spec.volume.clone();
        let r = device::start(spec, None)?;
        let out = json!({"ok": true, "dev_id": r.dev_id, "path": r.path()});
        self.devices.lock().unwrap().insert(vol, r);
        self.save(&BTreeMap::new());
        Ok(out)
    }

    fn detach(&self, volume: &str) -> Result<Value> {
        let _g = self.ops.lock().unwrap();
        let Some(r) = self.devices.lock().unwrap().remove(volume) else {
            // Idempotent: NodeUnstage retries after success must succeed.
            return Ok(json!({"ok": true, "absent": true}));
        };
        self.save(&BTreeMap::new());
        r.detach()?;
        Ok(json!({"ok": true}))
    }

    fn list(&self) -> Value {
        let devs = self.devices.lock().unwrap();
        let list: Vec<Value> = devs
            .values()
            .map(|r| {
                let paths: Vec<Value> = r.ctrls.paths.iter().map(|p| json!({"addr": p.addr.to_string(), "up": p.cntlid().is_some()})).collect();
                json!({"volume": r.spec.volume, "subnqn": r.spec.subnqn, "dev_id": r.dev_id, "path": r.path(), "paths": paths})
            })
            .collect();
        json!({"ok": true, "devices": list})
    }

    fn stats(&self, volume: &str) -> Result<Value> {
        let devs = self.devices.lock().unwrap();
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
        log::info!("handover: {} device(s) left for the next daemon; not drained (writes will be held on recovery): {:?}", clean.len(), dirty);
        std::process::exit(0)
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
    let d = Arc::new(Daemon { devices: Mutex::new(BTreeMap::new()), state_path: state_path.to_string(), ops: Mutex::new(()) });

    // Reattach what a previous daemon left behind.
    let previous: Vec<StateEntry> = std::fs::read(state_path).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default();
    for e in previous {
        if libublk::ctrl::UblkCtrl::new_simple(e.dev_id).is_err() {
            log::warn!("{}: ublk device {} is gone; dropping it from state", e.spec.volume, e.dev_id);
            continue;
        }
        let vol = e.spec.volume.clone();
        match device::start(e.spec, Some((e.dev_id, !e.clean))) {
            Ok(r) => {
                log::info!("{vol}: reattached /dev/ublkb{}", r.dev_id);
                d.devices.lock().unwrap().insert(vol, r);
            }
            Err(err) => log::error!("{vol}: could not reattach ublk device {}: {err:#}", e.dev_id),
        }
    }
    d.save(&BTreeMap::new());

    let _ = std::fs::remove_file(socket);
    let listener = UnixListener::bind(socket).with_context(|| format!("bind {socket}"))?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    let dh = d.clone();
    ctrlc::set_handler(move || dh.handover())?;
    log::info!("nvmeublkd listening on {socket} ({} device(s) attached)", d.devices.lock().unwrap().len());
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
