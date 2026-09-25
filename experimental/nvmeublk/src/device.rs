//! One ublk device served by this process: its controllers, its ublk queues
//! and its lifecycle. `nvmeublk run` serves exactly one; the per-node daemon
//! (daemon.rs) serves many, each on its own thread.

use crate::conn::Ident;
use crate::{ctrls, host_ident, qengine, queue_fn};
use anyhow::{bail, Context, Result};
use libublk::ctrl::UblkCtrlBuilder;
use libublk::io::UblkDev;
use libublk::UblkFlags;
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

fn d_queues() -> u16 {
    4
}
fn d_depth() -> u16 {
    64
}
fn d_io_timeout() -> u64 {
    5000
}
fn d_no_path() -> u64 {
    30000
}
fn d_one() -> usize {
    1
}
fn d_threads() -> u16 {
    1
}
fn d_rx_chunk() -> usize {
    32 * 1024
}

/// Everything needed to (re)create a device. Stored in the daemon's state
/// file, so a restarted daemon can reattach with the same configuration.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DeviceSpec {
    /// Caller's name for the volume (scale-csi: the volume id). Unique per node.
    pub volume: String,
    pub subnqn: String,
    /// Target portals, "addr:port".
    pub addrs: Vec<String>,
    /// Host identity to connect with; the node's own if omitted.
    #[serde(default)]
    pub hostnqn: Option<String>,
    #[serde(default)]
    pub hostid: Option<String>,
    #[serde(default = "d_queues")]
    pub queues: u16,
    #[serde(default = "d_depth")]
    pub depth: u16,
    #[serde(default)]
    pub zero_copy: bool,
    /// NAPI busy-poll budget while I/O is in flight (us); 0 = off.
    #[serde(default)]
    pub napi_us: u32,
    #[serde(default = "d_io_timeout")]
    pub io_timeout_ms: u64,
    #[serde(default = "d_no_path")]
    pub no_path_timeout_ms: u64,
    /// Hold for writes orphaned by a failed path; KATO + 5 s if omitted.
    #[serde(default)]
    pub write_fence_ms: Option<u64>,
    /// TCP connections per path per queue (tuning; default 1).
    #[serde(default = "d_one")]
    pub conns_per_path: usize,
    /// Most bytes one header receive may take into the copy buffer. Payload
    /// that arrives inside it is copied; the rest of a read's payload goes
    /// zero copy. Smaller = less copying, more receives (tuning; default 32 KiB).
    #[serde(default = "d_rx_chunk")]
    pub rx_chunk: usize,
    /// ublk I/O threads per queue (UBLK_F_PER_IO_DAEMON; tuning, default 1).
    /// Each thread serves its own partition of the queue's tags with its own
    /// engine and connections.
    #[serde(default = "d_threads")]
    pub threads_per_queue: u16,
    /// Contiguous tag partitions instead of interleaved ones (tuning): blk-mq
    /// hands a submitter sequential tags, so one stream stays on one thread.
    #[serde(default)]
    pub seq_tags: bool,
}

/// A device being served by a thread of this process.
pub struct Running {
    pub spec: DeviceSpec,
    pub dev_id: i32,
    pub stats: Arc<qengine::Stats>,
    pub ctrls: Arc<ctrls::Ctrls>,
    draining: Arc<AtomicBool>,
    pub quiesce: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<()>>>,
}

impl Running {
    pub fn path(&self) -> String {
        format!("/dev/ublkb{}", self.dev_id)
    }

    /// Stop serving and delete the device. Parked and fenced I/O fails with
    /// EIO, so this never waits on paths that are gone.
    pub fn detach(mut self) -> Result<()> {
        self.draining.store(true, Ordering::Release);
        libublk::ctrl::UblkCtrl::new_simple(self.dev_id)
            .and_then(|c| c.kill_dev())
            .with_context(|| format!("stop ublk device {}", self.dev_id))?;
        match self.thread.take().map(|t| t.join()) {
            Some(Ok(r)) => r,
            Some(Err(_)) => bail!("device {} thread panicked", self.dev_id),
            None => Ok(()),
        }
    }

    /// Wait for the serving thread (for `nvmeublk run`).
    pub fn wait(mut self) -> Result<()> {
        match self.thread.take().map(|t| t.join()) {
            Some(Ok(r)) => r,
            Some(Err(_)) => bail!("device {} thread panicked", self.dev_id),
            None => Ok(()),
        }
    }

    /// Stop sending new I/O; true once nothing is on the wire.
    pub fn drain(&self) -> bool {
        self.quiesce.store(true, Ordering::Release);
        self.stats.inflight.load(Ordering::Acquire) <= 0
    }
}

fn ident(spec: &DeviceSpec) -> Ident {
    match (&spec.hostnqn, &spec.hostid) {
        (Some(nqn), Some(id)) => {
            let hex: String = id.chars().filter(|c| c.is_ascii_hexdigit()).collect();
            let mut hostid = [0u8; 16];
            for (i, b) in hex.as_bytes().chunks(2).take(16).enumerate() {
                hostid[i] = u8::from_str_radix(std::str::from_utf8(b).unwrap_or("0"), 16).unwrap_or(0);
            }
            Ident { hostnqn: nqn.trim().to_string(), hostid, subnqn: spec.subnqn.clone() }
        }
        _ => host_ident(&spec.subnqn),
    }
}

/// Start serving `spec`. `recover` = Some((dev_id, hold_writes)) reattaches
/// an existing ublk device after this process (or a predecessor) exited;
/// hold_writes is false only when the predecessor drained cleanly.
pub fn start(spec: DeviceSpec, recover: Option<(i32, bool)>) -> Result<Running> {
    let addrs: Vec<SocketAddr> = spec
        .addrs
        .iter()
        .map(|a| a.to_socket_addrs().with_context(|| format!("bad address {a}"))?.next().context("unresolvable"))
        .collect::<Result<_>>()?;
    let kato = Duration::from_secs(15);
    let ctrls = loop {
        match ctrls::Ctrls::new(addrs.clone(), ident(&spec), kato) {
            Ok(c) => break c,
            // A recovering device holds its I/O until a server reattaches;
            // giving up here would leave it frozen, so keep trying.
            Err(e) if recover.is_some() => {
                log::warn!("{}: recovery: no path reachable yet ({e:#}); retrying", spec.volume);
                std::thread::sleep(Duration::from_secs(1));
            }
            Err(e) => return Err(e),
        }
    };
    let info = ctrls.info.clone();
    let write_fence = Duration::from_millis(spec.write_fence_ms.unwrap_or(kato.as_millis() as u64 + 5000));
    let hold = recover.is_some_and(|(_, hold)| hold);
    if spec.zero_copy {
        let feats = libublk::ctrl::UblkCtrl::get_features().unwrap_or(0);
        let need = (libublk::sys::UBLK_F_AUTO_BUF_REG | libublk::sys::UBLK_F_USER_COPY) as u64;
        if feats & need != need {
            bail!("zero copy requested but this kernel's ublk lacks AUTO_BUF_REG/USER_COPY (features {feats:#x})");
        }
    }
    let stats = Arc::new(qengine::Stats::default());
    let stop = Arc::new(AtomicBool::new(false));
    let draining = Arc::new(AtomicBool::new(false));
    let quiesce = Arc::new(AtomicBool::new(false));
    let (tx, rx) = mpsc::channel::<Result<i32>>();
    let (s2, st2, c2, dr2, q2) = (spec.clone(), stats.clone(), ctrls.clone(), draining.clone(), quiesce.clone());
    let thread = std::thread::Builder::new().name(format!("ublk-{}", short(&spec.volume))).spawn(move || {
        let r = serve(s2, recover, hold, write_fence, info, st2, c2.clone(), stop, dr2, q2, &tx);
        if let Err(e) = &r {
            let _ = tx.send(Err(anyhow::anyhow!("{e:#}")));
        }
        c2.shutdown();
        r
    })?;
    let dev_id = match rx.recv_timeout(Duration::from_secs(60)) {
        Ok(Ok(id)) => id,
        Ok(Err(e)) => return Err(e),
        Err(_) => bail!("{}: device did not come up within 60 s", spec.volume),
    };
    Ok(Running { spec, dev_id, stats, ctrls, draining, quiesce, thread: Some(thread) })
}

fn short(v: &str) -> String {
    v.chars().rev().take(8).collect::<Vec<_>>().into_iter().rev().collect()
}

#[allow(clippy::too_many_arguments)]
fn serve(
    spec: DeviceSpec,
    recover: Option<(i32, bool)>,
    hold: bool,
    write_fence: Duration,
    info: crate::conn::NsInfo,
    stats: Arc<qengine::Stats>,
    ctrls: Arc<ctrls::Ctrls>,
    stop: Arc<AtomicBool>,
    draining: Arc<AtomicBool>,
    quiesce: Arc<AtomicBool>,
    ready: &mpsc::Sender<Result<i32>>,
) -> Result<()> {
    let (queues, depth) = (spec.queues.max(1), spec.depth.max(2));
    // Largest request the device takes. Zero copy moves data straight
    // between the socket and the request pages, so a larger cap costs no
    // buffer memory there (tag buffers are in-capsule sized) and keeps a 1M
    // request as one NVMe command; the copying mode allocates io_buf per tag.
    // NVMEUBLK_MAX_IO_KB (tuning): zero-copy default 1024, copying 512.
    let max_io = if spec.zero_copy { std::env::var("NVMEUBLK_MAX_IO_KB").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(1024) } else { 512 };
    let io_buf = (max_io.clamp(4, 32 * 1024) * 1024).min(info.mdts_bytes) as u32;
    let size = info.nsze << info.lba_shift;
    let lba_shift = info.lba_shift as u8;
    let flags = (libublk::sys::UBLK_F_USER_RECOVERY | libublk::sys::UBLK_F_USER_RECOVERY_REISSUE) as u64
        | if spec.zero_copy { (libublk::sys::UBLK_F_USER_COPY | libublk::sys::UBLK_F_AUTO_BUF_REG) as u64 } else { 0 };
    let threads = spec.threads_per_queue.clamp(1, depth);
    // Several threads per queue need UBLK_F_PER_IO_DAEMON, which the driver
    // advertises by itself (6.16+) and libublk checks after the device is
    // added; it is not a flag the server may request.
    let tag_flags = if threads > 1 && spec.seq_tags { UblkFlags::UBLK_DEV_F_SEQ_TAG_PARTITION } else { UblkFlags::empty() };
    let builder = UblkCtrlBuilder::default().name("nvmeublk").nr_queues(queues).depth(depth).io_buf_bytes(io_buf).ctrl_flags(flags).io_threads_per_queue(threads);
    let builder = match recover {
        Some((id, _)) => {
            libublk::ctrl::UblkCtrl::new_simple(id)?.start_user_recover().context("start user recovery")?;
            log::info!("{}: recovering ublk device {id}{}", spec.volume, if hold { " (writes held for one fence)" } else { " (clean handover)" });
            builder.id(id).dev_flags(UblkFlags::UBLK_DEV_F_RECOVER_DEV | tag_flags)
        }
        None => builder.dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV | tag_flags),
    };
    let ctrl = builder.build().context("create ublk device (is ublk_drv loaded?)")?;
    let dev_id = ctrl.dev_info().dev_id as i32;
    let fault_dir = format!("/run/nvmeublk/dev{dev_id}");
    let _ = std::fs::create_dir_all(&fault_dir);
    let _ = std::fs::write(format!("{fault_dir}/queues"), queues.to_string());
    let cfg = qengine::QConfig {
        io_timeout: Duration::from_millis(spec.io_timeout_ms),
        no_path_timeout: Duration::from_millis(spec.no_path_timeout_ms),
        max_attempts: 8,
        write_fence,
        // After a crash the dead server's writes may still be running on the
        // target, and REISSUE hands them to us: hold writes for one fence.
        hold_writes_until: hold.then(|| Instant::now() + write_fence),
        rx_offload: 0,
        cdev_fd: -1,
        conns_per_path: spec.conns_per_path.max(1),
        rx_chunk: spec.rx_chunk.max(64),
        napi_us: spec.napi_us,
        fault_dir,
        quiesce,
    };
    log::info!(
        "{}: {} blocks of {} B, {} queues x {} ({} threads/queue{}), zero_copy={} napi_us={} write fence {} ms",
        spec.volume,
        info.nsze,
        1u64 << info.lba_shift,
        queues,
        depth,
        threads,
        if spec.seq_tags { ", contiguous tags" } else { "" },
        spec.zero_copy,
        spec.napi_us,
        write_fence.as_millis()
    );
    let (sq, stq, drq) = (stats.clone(), stop.clone(), draining.clone());
    let vol = spec.volume.clone();
    let ready = ready.clone();
    ctrl.run_target(
        move |dev: &mut UblkDev| {
            dev.set_default_params(size);
            dev.tgt.params.basic.logical_bs_shift = lba_shift;
            dev.tgt.params.basic.physical_bs_shift = lba_shift.max(12);
            // Room on each queue ring for the network SQEs (recv + writev per
            // path, timer, reconnect wakeup) next to the ublk commands.
            dev.tgt.sq_depth = depth * 2 + 64;
            dev.tgt.cq_depth = depth * 2 + 64;
            Ok(())
        },
        move |qid, dev: &_| queue_fn(qid, dev, ctrls.clone(), sq.clone(), stq.clone(), drq.clone(), cfg.clone()),
        move |c| {
            log::info!("{vol}: serving /dev/ublkb{}", c.dev_info().dev_id);
            let _ = ready.send(Ok(c.dev_info().dev_id as i32));
        },
    )?;
    stop.store(true, Ordering::Release);
    ctrl.del_dev()?;
    let _ = std::fs::remove_dir_all(format!("/run/nvmeublk/dev{dev_id}"));
    Ok(())
}
