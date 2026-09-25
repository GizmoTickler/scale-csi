//! One ublk device served by this process: its controllers, its ublk queues
//! and its lifecycle. `nvmeublk run` serves exactly one; the per-node daemon
//! (daemon.rs) serves many.
//!
//! A device has no thread of its own for its lifetime, only its queue
//! threads. It is brought up on a short-lived thread (`start_connected`), and
//! stopped and deleted on the thread that detaches it or waits for it.

use crate::conn::Ident;
use crate::{ctrls, host_ident, qengine, queue_fn};
use anyhow::{bail, Context, Result};
use libublk::ctrl::{UblkCtrl, UblkCtrlBuilder, UblkTargetThreads};
use libublk::io::UblkDev;
use libublk::UblkFlags;
use serde::{Deserialize, Serialize};
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

/// NVMe keep-alive timeout of the admin queues.
const KATO: Duration = Duration::from_secs(15);

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
fn d_chunk() -> u16 {
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
    /// Chunked tag partition with several threads per queue (tuning): each
    /// thread owns runs of this many consecutive tags, dealt round-robin, so
    /// a low-depth stream stays on one thread for that many requests while a
    /// deep one spans all threads. 1 = plain interleave; clamped to
    /// depth / threads; ignored with seq_tags.
    #[serde(default = "d_chunk")]
    pub tag_chunk: u16,
}

/// A device being served by this process.
pub struct Running {
    pub spec: DeviceSpec,
    pub dev_id: i32,
    pub stats: Arc<qengine::Stats>,
    pub ctrls: Arc<ctrls::Ctrls>,
    draining: Arc<AtomicBool>,
    pub quiesce: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    /// Taken by `detach`/`wait`, which end the device.
    served: Option<Served>,
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
        self.finish()
    }

    /// Wait until the device is stopped (for `nvmeublk run`, whose SIGINT
    /// stops it), then delete it.
    pub fn wait(mut self) -> Result<()> {
        self.finish()
    }

    /// The end of the device's life, on the calling thread: what the tail of
    /// the device's own thread used to do.
    fn finish(&mut self) -> Result<()> {
        let Some(served) = self.served.take() else { return Ok(()) };
        let r = retire(served, &self.stop);
        self.ctrls.shutdown();
        r
    }

    /// Stop sending new I/O; true once nothing is on the wire.
    pub fn drain(&self) -> bool {
        self.quiesce.store(true, Ordering::Release);
        self.stats.inflight.load(Ordering::Acquire) <= 0
    }
}

/// A device that is up: the control that added or recovered it (and later
/// deletes it) and the queue threads serving it.
///
/// Dropped without `retire` (a Running dropped without detach, or a start
/// abandoned after its deadline), it leaves the device alone, as dropping the
/// old per-device thread's handle did: the queue threads keep serving it.
/// The control must not delete it then: DEL_DEV stops the device and waits
/// for its release, and the dropping thread may not even have a control ring.
struct Served {
    ctrl: UblkCtrl,
    threads: Option<UblkTargetThreads>,
}

impl Drop for Served {
    fn drop(&mut self) {
        if self.threads.is_some() {
            self.ctrl.disown();
        }
    }
}

/// Wait for a stopped device's queue threads to return, then delete it.
fn retire(mut s: Served, stop: &AtomicBool) -> Result<()> {
    let dev_id = s.ctrl.dev_info().dev_id;
    // Only creating this thread's control ring fails in wait_target; without
    // one, the control could not delete the device when dropped.
    if let Some(threads) = s.threads.take()
        && let Err(e) = s.ctrl.wait_target(threads)
    {
        s.ctrl.disown();
        return Err(e.into());
    }
    stop.store(true, Ordering::Release);
    s.ctrl.del_dev()?;
    let _ = std::fs::remove_dir_all(format!("/run/nvmeublk/dev{dev_id}"));
    Ok(())
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
    let ctrls = connect(&spec, recover.is_some())?;
    start_connected(spec, ctrls, recover)
}

/// Bring up the admin queue of every reachable path of `spec`. With `retry`
/// (a device being recovered) this keeps trying every second while no path
/// is reachable: a recovering device holds its I/O until a server
/// reattaches, so giving up here would leave it frozen.
pub fn connect(spec: &DeviceSpec, retry: bool) -> Result<Arc<ctrls::Ctrls>> {
    let addrs: Vec<SocketAddr> = spec
        .addrs
        .iter()
        .map(|a| a.to_socket_addrs().with_context(|| format!("bad address {a}"))?.next().context("unresolvable"))
        .collect::<Result<_>>()?;
    loop {
        match ctrls::Ctrls::new(addrs.clone(), ident(spec), KATO) {
            Ok(c) => return Ok(c),
            Err(e) if retry => {
                log::warn!("{}: recovery: no path reachable yet ({e:#}); retrying", spec.volume);
                std::thread::sleep(Duration::from_secs(1));
            }
            Err(e) => return Err(e),
        }
    }
}

/// `start` once `connect` has brought the paths up.
pub fn start_connected(spec: DeviceSpec, ctrls: Arc<ctrls::Ctrls>, recover: Option<(i32, bool)>) -> Result<Running> {
    let info = ctrls.info.clone();
    let write_fence = Duration::from_millis(spec.write_fence_ms.unwrap_or(KATO.as_millis() as u64 + 5000));
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
    // Bring-up runs on a short-lived thread. Its control commands (ADD or
    // START_USER_RECOVERY, then START or END_USER_RECOVERY) go through that
    // thread's own control ring, and the io-wq worker the driver punts them
    // to exits with the thread instead of living as long as the device. The
    // rendezvous channel hands the device over only if this side is still
    // waiting; after the deadline the thread serves the device to its end
    // itself, as the old per-device thread did.
    let (tx, rx) = mpsc::sync_channel::<Result<Served>>(0);
    let (s2, st2, c2, stop2, dr2, q2) = (spec.clone(), stats.clone(), ctrls.clone(), stop.clone(), draining.clone(), quiesce.clone());
    std::thread::Builder::new().name(format!("ublk-{}", short(&spec.volume))).spawn(move || {
        match bring_up(s2, recover, hold, write_fence, info, st2, c2.clone(), stop2.clone(), dr2, q2) {
            Ok(served) => {
                if let Err(mpsc::SendError(Ok(served))) = tx.send(Ok(served)) {
                    if let Err(e) = retire(served, &stop2) {
                        log::error!("ublk device abandoned after its start deadline: {e:#}");
                    }
                    c2.shutdown();
                }
            }
            Err(e) => {
                let _ = tx.send(Err(e));
                c2.shutdown();
            }
        }
    })?;
    let served = match rx.recv_timeout(Duration::from_secs(60)) {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(e),
        Err(_) => bail!("{}: device did not come up within 60 s", spec.volume),
    };
    let dev_id = served.ctrl.dev_info().dev_id as i32;
    Ok(Running { spec, dev_id, stats, ctrls, draining, quiesce, stop, served: Some(served) })
}

pub fn short(v: &str) -> String {
    v.chars().rev().take(8).collect::<Vec<_>>().into_iter().rev().collect()
}

/// ublk feature flags a new device is added with. A recovered device keeps
/// the flags it was added with: libublk replaces these with the driver's
/// copy when it opens the device for recovery.
fn ublk_flags(zero_copy: bool) -> u64 {
    use libublk::sys::*;
    // USER_RECOVERY + REISSUE: a restarted daemon reattaches the device and
    // gets the I/O that was in flight back. QUIESCE: QUIESCE_DEV, the only
    // per-device cancel that keeps the device, for moving one device between
    // servers in-process later. The driver requires USER_RECOVERY for it and
    // uses it nowhere else, so it changes nothing today; but flags are fixed
    // at ADD, so a device added without it can never get it.
    let mut flags = (UBLK_F_USER_RECOVERY | UBLK_F_USER_RECOVERY_REISSUE | UBLK_F_QUIESCE) as u64;
    if zero_copy {
        // SUPPORT_ZERO_COPY on top of USER_COPY + AUTO_BUF_REG only enables
        // UBLK_IO_(UN)REGISTER_IO_BUF, which nothing here sends yet: every
        // other test of it in the driver is ORed with USER_COPY/AUTO_BUF_REG
        // (need_map_io, need_req_ref, dropping NEED_GET_DATA). Never in the
        // copying mode, where it would switch the driver's data copy off.
        flags |= (UBLK_F_USER_COPY | UBLK_F_AUTO_BUF_REG | UBLK_F_SUPPORT_ZERO_COPY) as u64;
    }
    flags
}

/// Counts a queue thread out when it returns or unwinds. The last one out
/// sets the stop flag and shuts the admin paths down, as the device's own
/// thread did once every queue thread had returned, so a device that stops
/// on its own (not by detach) does not keep its paths up until a detach.
/// `retire` and `finish` repeat both; they are idempotent.
struct QueueExit {
    exited: Arc<AtomicUsize>,
    total: usize,
    stop: Arc<AtomicBool>,
    ctrls: Arc<ctrls::Ctrls>,
}

impl Drop for QueueExit {
    fn drop(&mut self) {
        if self.exited.fetch_add(1, Ordering::AcqRel) + 1 == self.total {
            self.stop.store(true, Ordering::Release);
            self.ctrls.shutdown();
        }
    }
}

/// Create (or reopen for recovery) the ublk device and start its queue
/// threads; returns once the device is up.
#[allow(clippy::too_many_arguments)]
fn bring_up(
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
) -> Result<Served> {
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
    let flags = ublk_flags(spec.zero_copy);
    let threads = spec.threads_per_queue.clamp(1, depth);
    let tag_chunk = spec.tag_chunk.max(1);
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
    // Fault injection fans a command out to one file per engine, and there
    // is one engine per io thread (engine id = queue * threads + thread).
    let _ = std::fs::write(format!("{fault_dir}/queues"), (queues * threads).to_string());
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
        "{}: {} blocks of {} B, {} queues x {} ({} threads/queue{}, tag chunk {}), zero_copy={} napi_us={} write fence {} ms",
        spec.volume,
        info.nsze,
        1u64 << info.lba_shift,
        queues,
        depth,
        threads,
        if spec.seq_tags { ", contiguous tags" } else { "" },
        tag_chunk,
        spec.zero_copy,
        spec.napi_us,
        write_fence.as_millis()
    );
    let (sq, stq, drq) = (stats.clone(), stop.clone(), draining.clone());
    let exited = Arc::new(AtomicUsize::new(0));
    let target = ctrl.start_target(
        move |dev: &mut UblkDev| {
            dev.set_default_params(size);
            dev.set_io_tag_chunk(tag_chunk);
            dev.tgt.params.basic.logical_bs_shift = lba_shift;
            dev.tgt.params.basic.physical_bs_shift = lba_shift.max(12);
            // Room on each queue ring for the network SQEs (recv + writev per
            // path, timer, reconnect wakeup) next to the ublk commands.
            dev.tgt.sq_depth = depth * 2 + 64;
            dev.tgt.cq_depth = depth * 2 + 64;
            Ok(())
        },
        move |qid, dev: &_| {
            // libublk runs one thread per (queue, io thread); the kernel may
            // have trimmed the queue count, so count from the device.
            let total = dev.dev_info.nr_hw_queues as usize * dev.io_threads_per_queue() as usize;
            let _out = QueueExit { exited: exited.clone(), total, stop: stq.clone(), ctrls: ctrls.clone() };
            queue_fn(qid, dev, ctrls.clone(), sq.clone(), stq.clone(), drq.clone(), cfg.clone())
        },
    )?;
    log::info!("{}: serving /dev/ublkb{dev_id}", spec.volume);
    Ok(Served { ctrl, threads: Some(target) })
}

#[cfg(test)]
mod tests {
    use libublk::sys::*;

    #[test]
    fn new_devices_can_be_quiesced() {
        for zero_copy in [false, true] {
            let f = super::ublk_flags(zero_copy);
            assert_ne!(f & UBLK_F_QUIESCE as u64, 0, "zero_copy={zero_copy}");
            // ADD_DEV refuses QUIESCE without USER_RECOVERY.
            assert_ne!(f & UBLK_F_USER_RECOVERY as u64, 0, "zero_copy={zero_copy}");
            assert_ne!(f & UBLK_F_USER_RECOVERY_REISSUE as u64, 0, "zero_copy={zero_copy}");
        }
    }

    #[test]
    fn zero_copy_flag_only_with_user_copy_and_auto_buf_reg() {
        let zc = (UBLK_F_SUPPORT_ZERO_COPY | UBLK_F_USER_COPY | UBLK_F_AUTO_BUF_REG) as u64;
        // Copying mode: SUPPORT_ZERO_COPY alone would turn the driver's copy off.
        assert_eq!(super::ublk_flags(false) & zc, 0);
        assert_eq!(super::ublk_flags(true) & zc, zc);
    }

    /// libublk refuses any flag outside its UBLK_DRV_F_ALL with InvalidVal
    /// before it touches the driver (the device open fails with EACCES
    /// without root, and with root id -1 adds nothing).
    #[test]
    fn libublk_accepts_the_flags() {
        for zero_copy in [false, true] {
            let r = libublk::ctrl::UblkCtrl::new(None, -1, 1, 64, 4096, super::ublk_flags(zero_copy), 0, libublk::UblkFlags::empty());
            assert!(!matches!(r, Err(libublk::UblkError::InvalidVal)), "zero_copy={zero_copy}");
        }
    }
}
