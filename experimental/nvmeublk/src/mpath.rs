//! Userspace NVMe/TCP multipath: least-outstanding path selection, failover
//! of in-flight I/O, queue-if-no-path with a deadline, reconnect, keep-alive
//! and a stall detector for paths that go silent without a socket error.

use crate::conn::*;
use anyhow::{bail, Result};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct Config {
    pub kato: Duration,
    /// A request older than this on a live path kills that path (the
    /// userspace analog of fast_io_fail_tmo: fail over instead of waiting).
    pub io_timeout: Duration,
    /// How long I/O is held while NO path is usable before it fails with EIO.
    /// Zero means hold forever (queue_if_no_path).
    pub no_path_timeout: Duration,
    pub qsize: u16,
    pub max_attempts: u32,
}

struct Path {
    addr: SocketAddr,
    admin: Mutex<Option<AdminConn>>,
    io: RwLock<Option<Arc<IoConn>>>,
    next_retry: Mutex<Instant>,
    backoff: Mutex<Duration>,
}

#[derive(Default)]
pub struct Stats {
    pub failovers: AtomicU64,
    pub resubmits: AtomicU64,
    pub parked: AtomicU64,
    pub no_path_eio: AtomicU64,
    pub reconnects: AtomicU64,
    pub stall_kills: AtomicU64,
}

pub struct Mpath {
    paths: Vec<Path>,
    id: Ident,
    pub info: NsInfo,
    cfg: Config,
    parked: Mutex<VecDeque<Req>>,
    pub stats: Stats,
    stop: AtomicBool,
}

impl Mpath {
    /// Connect every path that answers. At least one must, to learn the
    /// namespace geometry; the rest are retried by the maintenance thread.
    pub fn new(addrs: Vec<SocketAddr>, id: Ident, cfg: Config) -> Result<Arc<Self>> {
        let mut info = None;
        for a in &addrs {
            match AdminConn::connect(*a, &id, cfg.kato.as_millis() as u32).and_then(|mut ad| ad.enable_and_identify()) {
                Ok((i, _)) => {
                    info = Some(i);
                    break;
                }
                Err(e) => log::warn!("{a}: initial probe failed: {e:#}"),
            }
        }
        let Some(info) = info else { bail!("no path reachable") };
        log::info!(
            "namespace: {} blocks of {} bytes ({} MiB), in-capsule {} B, mdts {}",
            info.nsze,
            1u64 << info.lba_shift,
            (info.nsze << info.lba_shift) >> 20,
            info.incapsule_bytes,
            if info.mdts_bytes == usize::MAX { "unlimited".into() } else { info.mdts_bytes.to_string() }
        );
        let paths = addrs
            .into_iter()
            .map(|addr| Path {
                addr,
                admin: Mutex::new(None),
                io: RwLock::new(None),
                next_retry: Mutex::new(Instant::now()),
                backoff: Mutex::new(Duration::from_millis(250)),
            })
            .collect();
        let m = Arc::new(Mpath { paths, id, info, cfg, parked: Mutex::new(VecDeque::new()), stats: Stats::default(), stop: AtomicBool::new(false) });
        for i in 0..m.paths.len() {
            if let Err(e) = m.connect_path(i) {
                log::warn!("path {i} ({}): {e:#}", m.paths[i].addr);
            }
        }
        Ok(m)
    }

    fn connect_path(self: &Arc<Self>, i: usize) -> Result<()> {
        let p = &self.paths[i];
        let mut admin = AdminConn::connect(p.addr, &self.id, self.cfg.kato.as_millis() as u32)?;
        let (info, mqes) = admin.enable_and_identify()?;
        if info.nsze != self.info.nsze || info.lba_shift != self.info.lba_shift {
            bail!("namespace geometry differs from the other paths; refusing (not the same namespace?)");
        }
        let qsize = self.cfg.qsize.min(mqes.saturating_add(1)).max(2);
        let (io, reader) = IoConn::connect(i, p.addr, &self.id, admin.cntlid, qsize, self.info.clone())?;
        *p.admin.lock().unwrap() = Some(admin);
        *p.io.write().unwrap() = Some(io.clone());
        *p.backoff.lock().unwrap() = Duration::from_millis(250);
        let me = self.clone();
        std::thread::Builder::new().name(format!("nvme-rx-{i}")).spawn(move || {
            let orphans = io.run_receiver(reader);
            me.path_failed(i, &io, orphans);
        })?;
        log::info!("path {i} ({}) up: cntlid {}, qsize {qsize}", p.addr, self.paths[i].admin.lock().unwrap().as_ref().unwrap().cntlid);
        self.drain_parked();
        Ok(())
    }

    fn path_failed(self: &Arc<Self>, i: usize, io: &Arc<IoConn>, orphans: Vec<Req>) {
        let p = &self.paths[i];
        {
            let mut cur = p.io.write().unwrap();
            if cur.as_ref().is_some_and(|c| Arc::ptr_eq(c, io)) {
                *cur = None;
            }
        }
        if self.stop.load(Ordering::Acquire) {
            for r in orphans {
                r.done.complete(-libc::EIO);
            }
            return;
        }
        if !orphans.is_empty() {
            self.stats.failovers.fetch_add(1, Ordering::Relaxed);
            log::warn!("path {i} ({}) down: failing over {} in-flight request(s)", p.addr, orphans.len());
        } else {
            log::warn!("path {i} ({}) down", p.addr);
        }
        // Fail over FIRST: tearing down the admin queue can wait behind a
        // keep-alive blocked on this very (silent) path.
        for r in orphans {
            self.stats.resubmits.fetch_add(1, Ordering::Relaxed);
            self.resubmit(r);
        }
        if let Some(a) = p.admin.lock().unwrap().take() {
            a.shutdown();
        }
    }

    fn resubmit(&self, mut r: Req) {
        r.attempts += 1;
        if r.attempts > self.cfg.max_attempts {
            log::error!("{:?} slba {}: giving up after {} attempts", r.op, r.slba, r.attempts - 1);
            r.done.complete(-libc::EIO);
            return;
        }
        self.submit(r);
    }

    fn live_paths(&self) -> Vec<Arc<IoConn>> {
        self.paths.iter().filter_map(|p| p.io.read().unwrap().clone()).filter(|c| !c.is_dead()).collect()
    }

    /// Dispatch a request: least-outstanding live path first; wait briefly if
    /// every live path is at queue depth; park if no path is live.
    pub fn submit(&self, mut r: Req) {
        loop {
            let mut live = self.live_paths();
            if live.is_empty() {
                self.stats.parked.fetch_add(1, Ordering::Relaxed);
                self.parked.lock().unwrap().push_back(r);
                return;
            }
            live.sort_by_key(|c| c.outstanding());
            let mut all_busy = true;
            for c in &live {
                match c.try_submit(r) {
                    Ok(()) => return,
                    Err(SubmitErr::Busy(back)) => r = back,
                    Err(SubmitErr::Dead(back)) => {
                        all_busy = false;
                        r = back;
                    }
                }
            }
            if all_busy {
                live[0].wait_for_cid(Duration::from_millis(50));
            }
        }
    }

    fn drain_parked(&self) {
        let reqs: Vec<Req> = self.parked.lock().unwrap().drain(..).collect();
        if !reqs.is_empty() {
            log::info!("path available again: resubmitting {} parked request(s)", reqs.len());
        }
        for r in reqs {
            self.submit(r);
        }
    }

    fn expire_parked(&self) {
        if self.cfg.no_path_timeout.is_zero() {
            return;
        }
        let mut q = self.parked.lock().unwrap();
        let mut keep = VecDeque::new();
        while let Some(r) = q.pop_front() {
            if r.first_submit.elapsed() > self.cfg.no_path_timeout {
                self.stats.no_path_eio.fetch_add(1, Ordering::Relaxed);
                r.done.complete(-libc::EIO);
            } else {
                keep.push_back(r);
            }
        }
        *q = keep;
    }

    /// Start one supervisor per path plus the watchdog. Anything that can
    /// block on a network peer (connect, keep-alive) runs on the path's own
    /// supervisor, so a silent path can never delay failover of the others.
    pub fn maintain(self: &Arc<Self>) {
        for i in 0..self.paths.len() {
            let me = self.clone();
            std::thread::Builder::new().name(format!("nvme-path-{i}")).spawn(move || me.supervise(i)).expect("spawn path supervisor");
        }
        // Watchdog: non-blocking work only.
        while !self.stop.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(200));
            for (i, p) in self.paths.iter().enumerate() {
                let Some(c) = p.io.read().unwrap().clone() else { continue };
                if let Some(age) = c.oldest_inflight() {
                    if age > self.cfg.io_timeout && !c.is_dead() {
                        self.stats.stall_kills.fetch_add(1, Ordering::Relaxed);
                        log::warn!("path {i} ({}): request stalled {age:?} > io_timeout; killing path", p.addr);
                        c.kill();
                    }
                }
            }
            self.expire_parked();
            self.fault_injection();
        }
    }

    /// Test hook: `echo "kill N" > /run/nvmeublk-fault` drops path N's I/O
    /// connection; `stall N` makes it go silent. Lets drills run against a
    /// production fabric without touching links other volumes share.
    fn fault_injection(&self) {
        let path = "/run/nvmeublk-fault";
        let Ok(cmd) = std::fs::read_to_string(path) else { return };
        let _ = std::fs::remove_file(path);
        let mut it = cmd.split_whitespace();
        let (Some(verb), Some(Ok(i))) = (it.next(), it.next().map(str::parse::<usize>)) else { return };
        let Some(p) = self.paths.get(i) else { return };
        let Some(c) = p.io.read().unwrap().clone() else { return };
        match verb {
            "kill" => {
                log::warn!("fault injection: killing path {i} ({})", p.addr);
                c.kill();
            }
            "stall" => {
                log::warn!("fault injection: path {i} ({}) goes silent", p.addr);
                c.stall.store(true, Ordering::Release);
            }
            _ => {}
        }
    }

    /// Per-path reconnect (with backoff) and keep-alive.
    fn supervise(self: &Arc<Self>, i: usize) {
        let p = &self.paths[i];
        let mut last_ka = Instant::now();
        while !self.stop.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(100));
            let up = p.io.read().unwrap().as_ref().is_some_and(|c| !c.is_dead());
            if !up {
                if p.io.read().unwrap().is_some() {
                    continue; // receiver still draining the old connection
                }
                let now = Instant::now();
                if now < *p.next_retry.lock().unwrap() {
                    continue;
                }
                match self.connect_path(i) {
                    Ok(()) => {
                        self.stats.reconnects.fetch_add(1, Ordering::Relaxed);
                        last_ka = Instant::now();
                    }
                    Err(e) => {
                        let mut b = p.backoff.lock().unwrap();
                        log::debug!("path {i} ({}) reconnect failed: {e:#}; retry in {:?}", p.addr, *b);
                        *p.next_retry.lock().unwrap() = now + *b;
                        *b = (*b * 2).min(Duration::from_secs(2));
                    }
                }
                continue;
            }
            if last_ka.elapsed() >= self.cfg.kato / 3 {
                last_ka = Instant::now();
                let failed = {
                    let mut a = p.admin.lock().unwrap();
                    match a.as_mut() {
                        Some(ad) => ad.keep_alive().is_err(),
                        None => false,
                    }
                };
                if failed {
                    log::warn!("path {i} ({}): keep-alive failed; killing path", p.addr);
                    if let Some(c) = p.io.read().unwrap().clone() {
                        c.kill();
                    }
                }
            }
        }
    }

    pub fn path_states(&self) -> Vec<(SocketAddr, bool, usize)> {
        self.paths
            .iter()
            .map(|p| {
                let io = p.io.read().unwrap().clone();
                let up = io.as_ref().is_some_and(|c| !c.is_dead());
                (p.addr, up, io.map(|c| c.outstanding()).unwrap_or(0))
            })
            .collect()
    }

    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Release);
        for r in self.parked.lock().unwrap().drain(..) {
            r.done.complete(-libc::EIO);
        }
        for p in &self.paths {
            if let Some(c) = p.io.read().unwrap().clone() {
                c.kill();
            }
        }
    }
}
