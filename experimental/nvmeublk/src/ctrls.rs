//! Per-path admin controllers for the io_uring engine: connect, enable,
//! identify, keep-alive and reconnect, each on its own supervisor thread.
//! I/O queues belong to the ublk queue threads (see qengine.rs); this layer
//! only tells them which controller to attach to and when it changed.

use crate::conn::{AdminConn, Ident, NsInfo};
use anyhow::{bail, Result};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct CtrlPath {
    pub addr: SocketAddr,
    /// Controller id of the live admin queue, 0 while down.
    cntlid: Mutex<u16>,
    /// Bumped every time the controller is lost or replaced. A queue whose
    /// I/O connection was made under an older epoch must reconnect: its
    /// queue belongs to a controller that no longer exists.
    pub epoch: AtomicU64,
    pub max_qsize: Mutex<u16>,
    /// A queue thread saw this path fail under epoch N and asks for the whole
    /// controller to be torn down (0 = no request). Honoured only while the
    /// epoch is still N, so a late report cannot kill a fresh controller.
    fence_req: AtomicU64,
}

impl CtrlPath {
    pub fn cntlid(&self) -> Option<u16> {
        let c = *self.cntlid.lock().unwrap();
        (c != 0).then_some(c)
    }

    /// cntlid and the epoch it belongs to, read so that the pair is
    /// consistent: an I/O queue connected with this cntlid is valid only
    /// while the epoch is unchanged.
    pub fn snapshot(&self) -> Option<(u16, u64)> {
        let e1 = self.epoch.load(Ordering::Acquire);
        let c = self.cntlid()?;
        let e2 = self.epoch.load(Ordering::Acquire);
        (e1 == e2).then_some((c, e1))
    }

    /// Ask the supervisor to tear down this path's controller, as the kernel
    /// resets the whole controller on any queue error. Every queue's I/O
    /// connection to it then dies on the epoch bump, so the target sees the
    /// controller go away instead of one queue quietly disappearing.
    pub fn fence(&self, epoch: u64) {
        self.fence_req.store(epoch, Ordering::Release);
    }
}

pub struct Ctrls {
    pub paths: Vec<Arc<CtrlPath>>,
    pub id: Arc<Ident>,
    pub info: NsInfo,
    kato: Duration,
    stop: AtomicBool,
}

impl Ctrls {
    pub fn new(addrs: Vec<SocketAddr>, id: Ident, kato: Duration) -> Result<Arc<Self>> {
        let id = Arc::new(id);
        let mut info = None;
        let paths: Vec<Arc<CtrlPath>> = addrs
            .iter()
            .map(|a| Arc::new(CtrlPath { addr: *a, cntlid: Mutex::new(0), epoch: AtomicU64::new(1), max_qsize: Mutex::new(128), fence_req: AtomicU64::new(0) }))
            .collect();
        let mut admins = Vec::new();
        for p in &paths {
            match Self::bring_up(p, &id, kato) {
                Ok((a, i)) => {
                    if info.is_none() {
                        info = Some(i);
                    }
                    admins.push(Some(a));
                }
                Err(e) => {
                    log::warn!("{}: {e:#}", p.addr);
                    admins.push(None);
                }
            }
        }
        let Some(info) = info else { bail!("no path reachable") };
        let me = Arc::new(Ctrls { paths, id, info, kato, stop: AtomicBool::new(false) });
        for (i, admin) in admins.into_iter().enumerate() {
            let m = me.clone();
            std::thread::Builder::new().name(format!("nvme-ctrl-{i}")).spawn(move || m.supervise(i, admin))?;
        }
        Ok(me)
    }

    fn bring_up(p: &CtrlPath, id: &Ident, kato: Duration) -> Result<(AdminConn, NsInfo)> {
        let mut a = AdminConn::connect(p.addr, id, kato.as_millis() as u32)?;
        let (info, mqes) = a.enable_and_identify()?;
        *p.cntlid.lock().unwrap() = a.cntlid;
        *p.max_qsize.lock().unwrap() = mqes.saturating_add(1).clamp(2, 128);
        log::info!("path {} admin up: cntlid {}", p.addr, a.cntlid);
        Ok((a, info))
    }

    fn lose(p: &CtrlPath) {
        *p.cntlid.lock().unwrap() = 0;
        p.epoch.fetch_add(1, Ordering::AcqRel);
    }

    fn supervise(self: &Arc<Self>, i: usize, mut admin: Option<AdminConn>) {
        let p = self.paths[i].clone();
        let mut backoff = Duration::from_millis(250);
        let mut next_try = Instant::now();
        let mut last_ka = Instant::now();
        while !self.stop.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(100));
            let req = p.fence_req.swap(0, Ordering::AcqRel);
            if req != 0 && req == p.epoch.load(Ordering::Acquire) {
                if let Some(a) = admin.take() {
                    log::warn!("path {i} ({}): I/O failure under epoch {req}; tearing down controller {}", p.addr, a.cntlid);
                    a.shutdown();
                    Self::lose(&p);
                    // Give the target a moment to see every queue close before
                    // a new controller is created on this path.
                    next_try = Instant::now() + Duration::from_millis(200);
                    continue;
                }
            }
            match admin.as_mut() {
                Some(a) => {
                    if last_ka.elapsed() < self.kato / 3 {
                        continue;
                    }
                    last_ka = Instant::now();
                    if let Err(e) = a.keep_alive() {
                        log::warn!("path {i} ({}): keep-alive failed ({e:#}); dropping controller", p.addr);
                        a.shutdown();
                        admin = None;
                        Self::lose(&p);
                        next_try = Instant::now();
                    }
                }
                None => {
                    if Instant::now() < next_try {
                        continue;
                    }
                    match Self::bring_up(&p, &self.id, self.kato) {
                        Ok((a, info)) if info.nsze == self.info.nsze && info.lba_shift == self.info.lba_shift => {
                            // New controller: queues attached to the old one are dead.
                            p.epoch.fetch_add(1, Ordering::AcqRel);
                            admin = Some(a);
                            backoff = Duration::from_millis(250);
                            last_ka = Instant::now();
                        }
                        Ok(_) => {
                            log::error!("path {i}: namespace geometry changed; refusing this path");
                            Self::lose(&p);
                            next_try = Instant::now() + Duration::from_secs(5);
                        }
                        Err(e) => {
                            log::debug!("path {i} reconnect failed: {e:#}");
                            next_try = Instant::now() + backoff;
                            backoff = (backoff * 2).min(Duration::from_secs(2));
                        }
                    }
                }
            }
        }
        if let Some(a) = admin {
            a.shutdown();
        }
    }

    /// A queue's I/O connection failed on a live-looking controller: the
    /// controller may be wedged. Force the supervisor to replace it only if
    /// keep-alive agrees; here we just record nothing and let keep-alive decide.
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Release);
    }
}
