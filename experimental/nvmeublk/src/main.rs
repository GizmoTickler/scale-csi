//! nvmeublk: a userspace NVMe/TCP initiator with its own multipath, exposed as
//! a Linux block device through ublk.
//!
//!   nvmeublk probe <nqn> <addr:port>...      protocol smoke test, no ublk
//!   nvmeublk run   <nqn> <addr:port>...      serve /dev/ublkbN until Ctrl-C
//!
//! Environment: NVMEUBLK_QUEUES (2), NVMEUBLK_DEPTH (64),
//! NVMEUBLK_IO_TIMEOUT_MS (5000), NVMEUBLK_NO_PATH_TIMEOUT_MS (30000, 0=forever).

mod conn;
mod ctrls;
mod mpath;
mod pdu;
mod qengine;

use anyhow::{bail, Context, Result};
use conn::{Done, Ident, Op, Req};
use libublk::ctrl::UblkCtrlBuilder;
use libublk::helpers::IoBuf;
use libublk::io::{UblkDev, UblkQueue};
use libublk::{BufDesc, UblkError, UblkFlags};
use mpath::{Config, Mpath};
use std::net::{SocketAddr, ToSocketAddrs};
use std::rc::Rc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn host_ident(subnqn: &str) -> Ident {
    // On a cluster node the target's allowlist (strict fencing) admits only the
    // node's own NVMe identity, so use it when asked to.
    if let (Ok(nqn), Ok(id)) = (std::env::var("NVMEUBLK_HOSTNQN"), std::env::var("NVMEUBLK_HOSTID")) {
        let hex: String = id.trim().chars().filter(|c| c.is_ascii_hexdigit()).collect();
        let mut hostid = [0u8; 16];
        for (i, b) in hex.as_bytes().chunks(2).take(16).enumerate() {
            hostid[i] = u8::from_str_radix(std::str::from_utf8(b).unwrap_or("0"), 16).unwrap_or(0);
        }
        return Ident { hostnqn: nqn.trim().to_string(), hostid, subnqn: subnqn.to_string() };
    }
    // Stable per machine: derive the host identity from /etc/machine-id.
    let mid = std::fs::read_to_string("/etc/machine-id").unwrap_or_default();
    let mid = mid.trim();
    let mut hostid = [0u8; 16];
    for (i, b) in mid.as_bytes().chunks(2).take(16).enumerate() {
        hostid[i] = u8::from_str_radix(std::str::from_utf8(b).unwrap_or("0"), 16).unwrap_or(0);
    }
    let uuid = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        hostid[0], hostid[1], hostid[2], hostid[3], hostid[4], hostid[5], hostid[6], hostid[7],
        hostid[8], hostid[9], hostid[10], hostid[11], hostid[12], hostid[13], hostid[14], hostid[15]
    );
    Ident { hostnqn: format!("nqn.2014-08.org.nvmexpress:uuid:{uuid}"), hostid, subnqn: subnqn.to_string() }
}

fn eventfd() -> Result<i32> {
    // Non-blocking, so io_uring arms a poll on it instead of punting each
    // waiting read to an io-wq worker thread.
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        bail!("eventfd: {}", std::io::Error::last_os_error());
    }
    Ok(fd)
}

fn config() -> Config {
    Config {
        kato: Duration::from_secs(15),
        io_timeout: Duration::from_millis(env_u64("NVMEUBLK_IO_TIMEOUT_MS", 5000)),
        no_path_timeout: Duration::from_millis(env_u64("NVMEUBLK_NO_PATH_TIMEOUT_MS", 30000)),
        qsize: 128,
        max_attempts: 8,
    }
}

fn start(nqn: &str, addrs: &[String]) -> Result<Arc<Mpath>> {
    let addrs: Vec<SocketAddr> = addrs
        .iter()
        .map(|a| a.to_socket_addrs().with_context(|| format!("bad address {a}"))?.next().context("unresolvable"))
        .collect::<Result<_>>()?;
    let m = Mpath::new(addrs, host_ident(nqn), config())?;
    let mm = m.clone();
    std::thread::Builder::new().name("nvme-maint".into()).spawn(move || mm.maintain())?;
    Ok(m)
}

/// Blocking submit used by `probe`.
fn sync_io(m: &Mpath, op: Op, slba: u64, buf: &mut [u8]) -> Result<i32> {
    let efd = eventfd()?;
    let done = Arc::new(Done { res: AtomicI32::new(0), efd });
    let nlb = (buf.len() >> m.info.lba_shift) as u32;
    m.submit(Req { op, slba, nlb, buf: buf.as_mut_ptr(), len: buf.len(), done: done.clone(), first_submit: Instant::now(), attempts: 0 });
    let mut v = 0u64;
    let mut pfd = libc::pollfd { fd: efd, events: libc::POLLIN, revents: 0 };
    while unsafe { libc::read(efd, &mut v as *mut u64 as *mut libc::c_void, 8) } != 8 {
        unsafe { libc::poll(&mut pfd, 1, -1) };
    }
    unsafe { libc::close(efd) };
    Ok(done.res.load(Ordering::Acquire))
}

fn probe(nqn: &str, addrs: &[String]) -> Result<()> {
    let m = start(nqn, addrs)?;
    std::thread::sleep(Duration::from_millis(300));
    for (a, up, _) in m.path_states() {
        println!("path {a}: {}", if up { "up" } else { "down" });
    }
    let bs = 1usize << m.info.lba_shift;
    // Small write (in-capsule), large write (R2T/H2CData), reads back, flush.
    for &len in &[bs, 128 * 1024, 1024 * 1024] {
        let slba = 2048;
        let mut w: Vec<u8> = (0..len).map(|i| (i * 31 + len) as u8).collect();
        let r1 = sync_io(&m, Op::Write, slba, &mut w)?;
        let mut rbuf = vec![0u8; len];
        let r2 = sync_io(&m, Op::Read, slba, &mut rbuf)?;
        let ok = r1 == len as i32 && r2 == len as i32 && rbuf == w;
        println!("{len:>8} B write={r1} read={r2} verify={}", if ok { "OK" } else { "MISMATCH" });
        if !ok {
            bail!("probe failed at {len} bytes");
        }
    }
    let f = sync_io(&m, Op::Flush, 0, &mut [])?;
    println!("flush={f}");
    m.shutdown();
    Ok(())
}

/// Engine-only latency: sequential 4K reads with no ublk in the path.
fn lat(nqn: &str, addrs: &[String]) -> Result<()> {
    let m = start(nqn, addrs)?;
    std::thread::sleep(Duration::from_millis(300));
    let mut buf = vec![0u8; 4096];
    for _ in 0..200 {
        sync_io(&m, Op::Read, 0, &mut buf)?;
    }
    let n = 5000;
    let t = Instant::now();
    for i in 0..n {
        sync_io(&m, Op::Read, (i * 7919) % 100_000, &mut buf)?;
    }
    let per = t.elapsed() / n as u32;
    println!("engine-only 4K read QD1: {per:?} per op ({:.0} IOPS)", 1.0 / per.as_secs_f64());
    m.shutdown();
    Ok(())
}

async fn io_task(q: &UblkQueue<'_>, tag: u16, e: &qengine::QEngine, shift: u32, cdev_fd: i32, zc: bool) -> Result<(), UblkError> {
    let buf = IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize);
    // Per-tag completion channel, reused for every request on this tag.
    let (done_tx, done_rx) = smol::channel::bounded::<i32>(1);
    // USER_COPY: the kernel moves no data at fetch/commit; `buf` is only our
    // scratch space, and data crosses into the request with pread/pwrite.
    let ucopy = (cdev_fd >= 0).then(|| libublk::io::UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0));
    // Zero copy: the kernel registers each request's pages in this ring's
    // buffer table at index `tag` when it hands us the request, and drops
    // the registration when we commit.
    let auto_reg = libublk::sys::ublk_auto_buf_reg { index: tag, flags: 0, reserved0: 0, reserved1: 0 };
    let ublk_buf = if zc { BufDesc::AutoReg(auto_reg) } else if ucopy.is_some() { BufDesc::Slice(&[]) } else { BufDesc::Slice(buf.as_slice()) };
    q.submit_io_prep_cmd(tag, ublk_buf, 0, if ucopy.is_some() || zc { None } else { Some(&buf) }).await?;
    loop {
        let iod = q.get_iod(tag);
        let op = match iod.op_flags & 0xff {
            libublk::sys::UBLK_IO_OP_READ => Some(qengine::Op::Read),
            libublk::sys::UBLK_IO_OP_WRITE => Some(qengine::Op::Write),
            libublk::sys::UBLK_IO_OP_FLUSH => Some(qengine::Op::Flush),
            _ => None,
        };
        let res = match op {
            None => -libc::EOPNOTSUPP,
            Some(op) => 'io: {
                let bytes = (iod.nr_sectors as usize) << 9;
                if let (Some(pos), qengine::Op::Write) = (ucopy, op) {
                    // Pull the write data out of the request into our buffer.
                    let mut got = 0usize;
                    while got < bytes {
                        let n = unsafe { libc::pread(cdev_fd, buf.as_slice().as_ptr().add(got) as *mut libc::c_void, bytes - got, (pos + got as u64) as libc::off_t) };
                        if n <= 0 {
                            if n < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                                continue;
                            }
                            log::error!("q{} tag {tag}: pread of write data failed", q.get_qid());
                            break 'io -libc::EIO;
                        }
                        got += n as usize;
                    }
                }
                e.submit(qengine::Pending::new(
                    op,
                    (iod.start_sector << 9) >> shift,
                    (bytes >> shift) as u32,
                    buf.as_slice().as_ptr() as *mut u8,
                    if op == qengine::Op::Flush { 0 } else { bytes },
                    done_tx.clone(),
                    ucopy,
                    (zc && op == qengine::Op::Read).then_some(tag),
                ));
                done_rx.recv().await.unwrap_or(-libc::EIO)
            }
        };
        let ublk_buf = if zc { BufDesc::AutoReg(auto_reg) } else if ucopy.is_some() { BufDesc::Slice(&[]) } else { BufDesc::Slice(buf.as_slice()) };
        q.submit_io_commit_cmd(tag, ublk_buf, res).await?;
    }
}

fn queue_fn(
    qid: u16,
    dev: &UblkDev,
    ctrls: Arc<ctrls::Ctrls>,
    stats: Arc<qengine::Stats>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    draining: Arc<std::sync::atomic::AtomicBool>,
    cfg: qengine::QConfig,
) {
    // NAPI busy poll (NVMEUBLK_NAPI_US): while this queue thread waits for
    // events, the kernel polls the NIC queues of the sockets on its ring for
    // up to N us instead of sleeping until an interrupt. Trades a slice of a
    // core for latency. (DEFER_TASKRUN was tried: no gain, and the queue
    // threads never exit after the device is deleted.)
    let q_rc = Rc::new(UblkQueue::new(qid, dev).unwrap());
    let napi_us = env_u64("NVMEUBLK_NAPI_US", 0) as u32;
    if napi_us > 0 {
        let mut napi = io_uring::types::Napi::new().set_busy_poll_timeout(napi_us).set_prefer_busy_poll(true);
        match libublk::with_task_io_ring_mut(|ring| ring.submitter().register_napi(&mut napi)) {
            Ok(()) => log::info!("queue {qid}: NAPI busy poll {napi_us} us"),
            Err(e) => log::warn!("queue {qid}: NAPI busy poll not available: {e}"),
        }
    }
    let shift = ctrls.info.lba_shift;
    // Engine tasks are 'static (they own Rc<QEngine>); tag tasks borrow the
    // queue. Two local executors, ticked together from the same event loop.
    let net_exe: Rc<smol::LocalExecutor<'static>> = Rc::new(smol::LocalExecutor::new());
    let st2 = stats.clone();
    let mut cfg = cfg;
    let user_copy = dev.dev_info.flags & libublk::sys::UBLK_F_USER_COPY as u64 != 0;
    cfg.cdev_fd = if user_copy { dev.tgt.fds[0] } else { -1 };
    let cdev_fd = cfg.cdev_fd;
    let zc = dev.dev_info.flags & libublk::sys::UBLK_F_AUTO_BUF_REG as u64 != 0;
    if zc {
        cfg.rx_offload = 0; // payload goes to the request pages on this ring
    }
    let engine = qengine::QEngine::new(qid, ctrls, cfg, net_exe.clone(), stats, stop, draining);
    engine.start();
    let exe_rc = Rc::new(smol::LocalExecutor::new());
    let exe = exe_rc.clone();
    let mut tasks = Vec::new();
    for tag in q_rc.tags() {
        let q = q_rc.clone();
        let e = engine.clone();
        tasks.push(exe.spawn(async move {
            match io_task(&q, tag, &e, shift, cdev_fd, zc).await {
                Err(UblkError::QueueIsDown) | Ok(_) => {}
                Err(err) => log::error!("io_task {tag} failed: {err}"),
            }
        }));
    }
    smol::block_on(exe_rc.run(async move {
        let run_ops = || {
            let t0 = Instant::now();
            st2.loops.fetch_add(1, Ordering::Relaxed);
            let mut progress = true;
            while progress {
                progress = false;
                while exe.try_tick() {
                    progress = true;
                }
                while net_exe.try_tick() {
                    progress = true;
                }
            }
            st2.loop_ns.fetch_add(t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
        };
        let done = || tasks.iter().all(|t| t.is_finished());
        if let Err(e) = libublk::wait_and_handle_io_events(&q_rc, Some(20), run_ops, done).await {
            log::error!("queue {qid}: event loop failed: {e}");
        }
    }));
    // Deliberately not setting the shared stop flag here: one queue's loop
    // ending must not stop the other queues' timers (reconnect, expiry).
}

/// A block driver in userspace must not need memory to make progress on the
/// writeback that would free memory. Done before any thread starts, so every
/// thread inherits it:
/// - PR_SET_IO_FLUSHER: this task's allocations never recurse into I/O
///   (PF_MEMALLOC_NOIO), and it is not throttled as a dirtier of its own device.
/// - mlockall: no page of the daemon can be swapped or reclaimed away.
/// - oom_score_adj -1000: the OOM killer never picks the storage daemon.
// linux/prctl.h (not exported by the libc crate for this target).
const PR_SET_IO_FLUSHER: libc::c_int = 57;
const PR_GET_IO_FLUSHER: libc::c_int = 58;

fn harden_for_writeback() {
    if unsafe { libc::prctl(PR_SET_IO_FLUSHER, 1, 0, 0, 0) } != 0 {
        log::warn!("PR_SET_IO_FLUSHER failed: {} (needs CAP_SYS_RESOURCE)", std::io::Error::last_os_error());
    }
    if unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) } != 0 {
        log::warn!("mlockall failed: {}", std::io::Error::last_os_error());
    }
    if let Err(e) = std::fs::write("/proc/self/oom_score_adj", "-1000") {
        log::warn!("oom_score_adj -1000 failed: {e}");
    }
    let flusher = unsafe { libc::prctl(PR_GET_IO_FLUSHER, 0, 0, 0, 0) } == 1;
    log::info!("writeback hardening: io_flusher={flusher} memory locked, oom_score_adj=-1000");
}

fn run(nqn: &str, addrs: &[String]) -> Result<()> {
    harden_for_writeback();
    let addrs: Vec<SocketAddr> = addrs
        .iter()
        .map(|a| a.to_socket_addrs().with_context(|| format!("bad address {a}"))?.next().context("unresolvable"))
        .collect::<Result<_>>()?;
    let recover_id = std::env::var("NVMEUBLK_RECOVER_ID").ok().and_then(|v| v.parse::<i32>().ok());
    let kato = Duration::from_secs(15);
    let ctrls = loop {
        match ctrls::Ctrls::new(addrs.clone(), host_ident(nqn), kato) {
            Ok(c) => break c,
            // A recovering device holds its I/O until a daemon reattaches;
            // giving up here would leave it frozen, so keep trying.
            Err(e) if recover_id.is_some() => {
                log::warn!("recovery: no path reachable yet ({e:#}); retrying");
                std::thread::sleep(Duration::from_secs(1));
            }
            Err(e) => return Err(e),
        }
    };
    let info = ctrls.info.clone();
    log::info!("namespace: {} blocks of {} bytes ({} MiB), in-capsule {} B", info.nsze, 1u64 << info.lba_shift, (info.nsze << info.lba_shift) >> 20, info.incapsule_bytes);
    let queues = env_u64("NVMEUBLK_QUEUES", 4) as u16;
    let depth = env_u64("NVMEUBLK_DEPTH", 64) as u16;
    let io_buf = (512 * 1024usize).min(info.mdts_bytes) as u32;
    let size = info.nsze << info.lba_shift;
    let lba_shift = info.lba_shift as u8;
    // Fence for orphaned writes: the target may keep a controller, and run
    // its commands, for up to KATO after losing us; plus a quiesce margin.
    let write_fence = Duration::from_millis(env_u64("NVMEUBLK_WRITE_FENCE_MS", kato.as_millis() as u64 + 5000));
    let cfg = qengine::QConfig {
        io_timeout: Duration::from_millis(env_u64("NVMEUBLK_IO_TIMEOUT_MS", 5000)),
        no_path_timeout: Duration::from_millis(env_u64("NVMEUBLK_NO_PATH_TIMEOUT_MS", 30000)),
        max_attempts: 8,
        write_fence,
        // After a crash the dead daemon's writes may still be running on the
        // target, and REISSUE hands them to us: hold writes for one fence.
        hold_writes_until: recover_id.map(|_| Instant::now() + write_fence),
        rx_offload: env_u64("NVMEUBLK_RX_OFFLOAD", 0) as usize,
        cdev_fd: -1,
        conns_per_path: env_u64("NVMEUBLK_CONNS_PER_PATH", 1).clamp(1, 8) as usize,
        rx_chunk: env_u64("NVMEUBLK_RX_CHUNK", 32 * 1024).clamp(4096, 1 << 20) as usize,
    };
    let mut user_copy = env_u64("NVMEUBLK_USER_COPY", 0) != 0;
    // Zero copy needs AUTO_BUF_REG (kernel >= 6.16); it also uses USER_COPY
    // for the few payload bytes that arrive with a PDU header.
    let zero_copy = env_u64("NVMEUBLK_ZERO_COPY", 0) != 0;
    if zero_copy {
        let feats = libublk::ctrl::UblkCtrl::get_features().unwrap_or(0);
        let need = (libublk::sys::UBLK_F_AUTO_BUF_REG | libublk::sys::UBLK_F_USER_COPY) as u64;
        if feats & need != need {
            bail!("NVMEUBLK_ZERO_COPY=1 but this kernel's ublk lacks AUTO_BUF_REG/USER_COPY (features {feats:#x})");
        }
        user_copy = true;
    }
    log::info!("write fence {} ms{}", write_fence.as_millis(), if recover_id.is_some() { " (writes held for one fence: recovering)" } else { "" });
    let stats = Arc::new(qengine::Stats::default());
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let draining = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let _ = std::fs::write("/run/nvmeublk-queues", queues.to_string());

    // USER_RECOVERY: if this process dies, the kernel quiesces the device and
    // holds I/O; a successor started with NVMEUBLK_RECOVER_ID reattaches.
    // REISSUE re-sends the requests that were in flight at the crash.
    let mut builder = UblkCtrlBuilder::default();
    builder = builder
        .name("nvmeublk")
        .nr_queues(queues)
        .depth(depth)
        .io_buf_bytes(io_buf)
        .ctrl_flags(
            (libublk::sys::UBLK_F_USER_RECOVERY | libublk::sys::UBLK_F_USER_RECOVERY_REISSUE) as u64
                | if user_copy { libublk::sys::UBLK_F_USER_COPY as u64 } else { 0 }
                | if zero_copy { libublk::sys::UBLK_F_AUTO_BUF_REG as u64 } else { 0 },
        );
    let builder = match recover_id {
        Some(id) => {
            libublk::ctrl::UblkCtrl::new_simple(id)?.start_user_recover().context("start user recovery")?;
            log::info!("recovering ublk device {id}");
            builder.id(id).dev_flags(UblkFlags::UBLK_DEV_F_RECOVER_DEV)
        }
        None => builder.dev_flags(UblkFlags::UBLK_DEV_F_ADD_DEV),
    };
    let ctrl = Arc::new(builder.build().context("create ublk device (is ublk_drv loaded?)")?);
    let f = ctrl.dev_info().flags;
    log::info!(
        "ublk device flags {f:#x}: zero_copy={} user_copy={} user_recovery={} reissue={}",
        f & libublk::sys::UBLK_F_AUTO_BUF_REG as u64 != 0,
        f & libublk::sys::UBLK_F_USER_COPY as u64 != 0,
        f & libublk::sys::UBLK_F_USER_RECOVERY as u64 != 0,
        f & libublk::sys::UBLK_F_USER_RECOVERY_REISSUE as u64 != 0
    );

    // libublk's control ring is thread-local: stop through a handler-owned handle.
    let dev_id = ctrl.dev_info().dev_id as i32;
    let dr = draining.clone();
    ctrlc::set_handler(move || {
        // Parked and fenced I/O would otherwise hold del_gendisk hostage.
        dr.store(true, Ordering::Release);
        match libublk::ctrl::UblkCtrl::new_simple(dev_id) {
        Ok(c) => {
            if let Err(e) = c.kill_dev() {
                log::error!("stop ublk device {dev_id}: {e}");
            }
        }
        Err(e) => log::error!("open ublk device {dev_id} to stop it: {e}"),
        }
    })?;
    let st = stats.clone();
    let cstat = ctrls.clone();
    std::thread::spawn(move || {
      let mut last = (0u64, 0u64, 0u64);
      loop {
        std::thread::sleep(Duration::from_secs(5));
        let (n, w, t) = (st.done.load(Ordering::Relaxed), st.wire_ns.load(Ordering::Relaxed), st.total_ns.load(Ordering::Relaxed));
        let dn = (n - last.0).max(1);
        let g = |a: &std::sync::atomic::AtomicU64| a.swap(0, Ordering::Relaxed);
        let (qw, qn, wd, dc, rn, lp, ln) = (g(&st.q2w_ns), g(&st.q2w_n).max(1), g(&st.w2d_ns), g(&st.d2c_ns), g(&st.rd_n).max(1), g(&st.loops).max(1), g(&st.loop_ns));
        let lat = format!(
            "zc_MiB={} io={} wire_avg={}us total_avg={}us | queued->wired={}us wired->1stdata={}us 1stdata->done={}us | loops/s={} run_ops_avg={}us",
            st.zc_bytes.load(Ordering::Relaxed) >> 20, n - last.0, (w - last.1) / dn / 1000, (t - last.2) / dn / 1000, qw / qn / 1000, wd / rn / 1000, dc / rn / 1000, lp / 5, ln / lp / 1000
        );
        last = (n, w, t);
        let ups: Vec<String> = cstat.paths.iter().map(|p| format!("{}={}", p.addr.ip(), if p.cntlid().is_some() { "up" } else { "DOWN" })).collect();
        log::info!(
            "ctrls [{}] failovers={} resubmits={} parked={} fenced={} path_errors={} protocol_errors={} no_path_eio={} reconnects={} stall_kills={} epoch_kills={} {}",
            ups.join(" "),
            st.failovers.load(Ordering::Relaxed),
            st.resubmits.load(Ordering::Relaxed),
            st.parked.load(Ordering::Relaxed),
            st.fenced.load(Ordering::Relaxed),
            st.path_errors.load(Ordering::Relaxed),
            st.protocol_errors.load(Ordering::Relaxed),
            st.no_path_eio.load(Ordering::Relaxed),
            st.reconnects.load(Ordering::Relaxed),
            st.stall_kills.load(Ordering::Relaxed),
            st.epoch_kills.load(Ordering::Relaxed),
            lat
        );
      }
    });

    let (cq, sq, stq, drq) = (ctrls.clone(), stats.clone(), stop.clone(), draining.clone());
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
        move |qid, dev: &_| queue_fn(qid, dev, cq.clone(), sq.clone(), stq.clone(), drq.clone(), cfg.clone()),
        |c| log::info!("serving /dev/ublkb{}", c.dev_info().dev_id),
    )?;
    stop.store(true, Ordering::Release);
    ctrls.shutdown();
    ctrl.del_dev()?;
    Ok(())
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 3 && args[1] == "del" {
        let id: i32 = args[2].parse().context("device id")?;
        libublk::ctrl::UblkCtrl::new_simple(id)?.del_dev()?;
        println!("deleted ublk device {id}");
        return Ok(());
    }
    if args.len() < 4 {
        bail!("usage: nvmeublk probe|run <subnqn> <addr:port>...");
    }
    match args[1].as_str() {
        "probe" => probe(&args[2], &args[3..]),
        "lat" => lat(&args[2], &args[3..]),
        "run" => run(&args[2], &args[3..]),
        c => bail!("unknown command {c}"),
    }
}
