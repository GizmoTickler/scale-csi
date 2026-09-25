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
mod daemon;
mod device;
mod mpath;
mod pdu;
mod qengine;

use anyhow::{bail, Context, Result};
use conn::{Done, Ident, Op, Req};
use libublk::helpers::IoBuf;
use libublk::io::{UblkDev, UblkQueue};
use libublk::{BufDesc, UblkError};
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
    // Zero copy moves bulk data straight between the socket and the request's
    // own pages, so this buffer only ever holds in-capsule write data: size it
    // to that instead of the max I/O size. It is mlock()ed, per tag, per queue,
    // per volume (64 x 512K x 8 queues = 256 MiB per volume otherwise).
    // Zero copy needs no per-tag buffer at all: reads land in the request
    // pages and every write, in-capsule or R2T, is sent from them. Only the
    // copying mode stages data here (it is mlock()ed, per tag, per queue,
    // per volume).
    let buf = (!zc).then(|| IoBuf::<u8>::new(q.dev.dev_info.max_io_buf_bytes as usize));
    let buf_ptr = buf.as_ref().map_or(std::ptr::null_mut(), |b| b.as_slice().as_ptr() as *mut u8);
    // Per-tag completion channel, reused for every request on this tag.
    let (done_tx, done_rx) = smol::channel::bounded::<i32>(1);
    // USER_COPY: the kernel moves no data at fetch/commit; `buf` is only our
    // scratch space, and data crosses into the request with pread/pwrite.
    let ucopy = (cdev_fd >= 0).then(|| libublk::io::UblkIOCtx::ublk_user_copy_pos(q.get_qid(), tag, 0));
    // Zero copy: the kernel registers each request's pages in this ring's
    // buffer table at index `tag` when it hands us the request, and drops
    // the registration when we commit.
    let auto_reg = libublk::sys::ublk_auto_buf_reg { index: tag, flags: 0, reserved0: 0, reserved1: 0 };
    let ublk_buf = if zc { BufDesc::AutoReg(auto_reg) } else if ucopy.is_some() { BufDesc::Slice(&[]) } else { BufDesc::Slice(buf.as_ref().expect("copying mode has a tag buffer").as_slice()) };
    q.submit_io_prep_cmd(tag, ublk_buf, 0, if ucopy.is_some() || zc { None } else { buf.as_ref() }).await?;
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
                // Zero copy: a write too large for the capsule goes out via
                // R2T straight from the request's registered pages.
                // Zero copy: every write, in-capsule or R2T, is sent from the request pages.
                let zc_write = zc && op == qengine::Op::Write;
                if let (Some(pos), qengine::Op::Write, false) = (ucopy, op, zc_write) {
                    // Pull the write data out of the request into our buffer.
                    let mut got = 0usize;
                    while got < bytes {
                        let n = unsafe { libc::pread(cdev_fd, buf_ptr.add(got) as *mut libc::c_void, bytes - got, (pos + got as u64) as libc::off_t) };
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
                    buf_ptr,
                    if op == qengine::Op::Flush { 0 } else { bytes },
                    done_tx.clone(),
                    ucopy,
                    (zc && (op == qengine::Op::Read || zc_write)).then_some(tag),
                ));
                done_rx.recv().await.unwrap_or(-libc::EIO)
            }
        };
        let ublk_buf = if zc { BufDesc::AutoReg(auto_reg) } else if ucopy.is_some() { BufDesc::Slice(&[]) } else { BufDesc::Slice(buf.as_ref().expect("copying mode has a tag buffer").as_slice()) };
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
    // Queue-thread CPU placement (NVMEUBLK_QUEUE_CPUS, tuning). libublk pins
    // each queue thread to its blk-mq CPU group, which at one queue per CPU is
    // exactly the submitting CPU: at QD1 the submitter and the queue thread
    // then take turns on one core. "all" lets the thread run anywhere.
    // A CPU list ("14-15", "6,7,14,15") confines every queue thread to
    // those CPUs: dedicated storage cores the workload does not run on.
    let ncpu = (unsafe { libc::sysconf(libc::_SC_NPROCESSORS_CONF) }.max(1) as usize).min(libc::CPU_SETSIZE as usize);
    let cpus: Option<Vec<usize>> = match std::env::var("NVMEUBLK_QUEUE_CPUS").as_deref() {
        Ok("all") => Some((0..ncpu).collect()),
        Ok(list) if list.chars().next().is_some_and(|ch| ch.is_ascii_digit()) => Some(parse_cpu_list(list).into_iter().filter(|&c| c < ncpu).collect()),
        _ => None,
    };
    if let Some(cpus) = cpus.filter(|c| !c.is_empty()) {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            for cpu in cpus {
                libc::CPU_SET(cpu, &mut set);
            }
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        }
    }
    // NAPI busy poll (NVMEUBLK_NAPI_US): while this queue thread waits for
    // events, the kernel polls the NIC queues of the sockets on its ring for
    // up to N us instead of sleeping until an interrupt. Trades a slice of a
    // core for latency. (DEFER_TASKRUN was tried: no gain, and the queue
    // threads never exit after the device is deleted.)
    // Queue ring setup flags (NVMEUBLK_RING_MODE, tuning). libublk builds
    // this thread's ring with COOP_TASKRUN unless one already exists, so
    // create it first when another mode is asked for:
    //   coop  (default) COOP_TASKRUN, as libublk does
    //   plain           no task-run flags
    //   defer           SINGLE_ISSUER + DEFER_TASKRUN
    let mode = std::env::var("NVMEUBLK_RING_MODE").unwrap_or_default();
    if mode == "plain" || mode == "defer" {
        let (sq, cq) = (dev.tgt.sq_depth as u32, dev.tgt.cq_depth as u32);
        let r = libublk::io::ublk_init_task_ring(|cell| {
            if cell.get().is_none() {
                let mut b = io_uring::IoUring::<io_uring::squeue::Entry, io_uring::cqueue::Entry>::builder();
                b.setup_cqsize(cq);
                if mode == "defer" {
                    b.setup_single_issuer().setup_defer_taskrun();
                }
                let ring = b.build(sq).map_err(libublk::UblkError::IOError)?;
                cell.set(std::cell::RefCell::new(ring)).map_err(|_| libublk::UblkError::OtherError(-libc::EEXIST))?;
            }
            Ok(())
        });
        if let Err(e) = r {
            log::warn!("q{qid}: ring mode {mode} failed ({e}); using libublk's default");
        }
    }
    let q_rc = Rc::new(UblkQueue::new(qid, dev).unwrap());
    let shift = ctrls.info.lba_shift;
    // Engine tasks are 'static (they own Rc<QEngine>); tag tasks borrow the
    // queue. Two local executors, ticked together from the same event loop.
    let net_exe: Rc<smol::LocalExecutor<'static>> = Rc::new(smol::LocalExecutor::new());
    let st2 = stats.clone();
    let st3 = stats.clone();
    let mut cfg = cfg;
    let user_copy = dev.dev_info.flags & libublk::sys::UBLK_F_USER_COPY as u64 != 0;
    cfg.cdev_fd = if user_copy { dev.tgt.fds[0] } else { -1 };
    let cdev_fd = cfg.cdev_fd;
    let zc = dev.dev_info.flags & libublk::sys::UBLK_F_AUTO_BUF_REG as u64 != 0;
    if zc {
        cfg.rx_offload = 0; // payload goes to the request pages on this ring
    }
    // One engine (connections, target I/O queue ids) per io thread: with
    // several threads per ublk queue each serves its own tag partition.
    let eid = qid * dev.io_threads_per_queue() + libublk::io::io_thread_idx();
    let engine = qengine::QEngine::new(eid, ctrls, cfg, net_exe.clone(), stats, stop, draining);
    engine.start();
    let spin_engine = engine.clone();
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
        // The queue's event loop (libublk's wait_and_handle_io_events, plus
        // adaptive polling). NVMEUBLK_SPIN_US (tuning, 0 = off): while this
        // queue has commands in flight and saw an event within the budget,
        // check the ring without blocking instead of sleeping in it. A queue
        // carrying a thin share of a stream otherwise sleeps between events
        // and pays a wakeup for each; an idle queue still sleeps.
        let spin = Duration::from_micros(env_u64("NVMEUBLK_SPIN_US", 0));
        let timeout = io_uring::types::Timespec::new().sec(20);
        let mut last_event = Instant::now();
        run_ops();
        loop {
            let hot = !spin.is_zero() && spin_engine.inflight_here() > 0 && last_event.elapsed() < spin;
            let (poll_timeout, failed) = match libublk::uring_async::uring_poll_io_fn::<io_uring::squeue::Entry>(&q_rc, Some(timeout), if hot { 0 } else { 1 }) {
                Ok(t) => (t, false),
                Err(_) => (false, true),
            };
            let mut events = 0u32;
            let aborted = match libublk::uring_async::ublk_reap_io_events_with_update_queue(&q_rc, poll_timeout, None, |cqe| {
                events += 1;
                // A SEND_ZC buffer-release notification carries the send's
                // user_data, whose future already completed on the first
                // CQE; libublk recycles future keys, so it must not be woken.
                if io_uring::cqueue::notif(cqe.flags()) {
                    st3.zc_notif.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                libublk::uring_async::ublk_wake_task(cqe.user_data(), cqe)
            }) {
                Ok(a) => a,
                Err(e) => {
                    log::error!("queue {qid}: event loop failed: {e}");
                    break;
                }
            };
            if events > 0 {
                last_event = Instant::now();
            }
            run_ops();
            if (aborted || failed) && done() {
                break;
            }
        }
    }));
    // Deliberately not setting the shared stop flag here: one queue's loop
    // ending must not stop the other queues' timers (reconnect, expiry).
}

/// "0-3,8,10-11" -> [0, 1, 2, 3, 8, 10, 11]. Malformed parts are skipped.
fn parse_cpu_list(list: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for part in list.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        match part.split_once('-') {
            Some((a, b)) => {
                if let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>()) {
                    out.extend(a..=b);
                }
            }
            None => out.extend(part.parse::<usize>().ok()),
        }
    }
    out
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

/// `nvmeublk run`: serve one device in the foreground, configured from the
/// environment (the test scripts' interface). SIGINT deletes the device.
fn run(nqn: &str, addrs: &[String]) -> Result<()> {
    harden_for_writeback();
    qengine::set_zc_recv_preference(env_u64("NVMEUBLK_ZC_RECV", 0) != 0);
    let spec = device::DeviceSpec {
        volume: std::env::var("NVMEUBLK_VOLUME").unwrap_or_else(|_| "run".into()),
        subnqn: nqn.to_string(),
        addrs: addrs.to_vec(),
        hostnqn: std::env::var("NVMEUBLK_HOSTNQN").ok(),
        hostid: std::env::var("NVMEUBLK_HOSTID").ok(),
        queues: env_u64("NVMEUBLK_QUEUES", 4) as u16,
        depth: env_u64("NVMEUBLK_DEPTH", 64) as u16,
        zero_copy: env_u64("NVMEUBLK_ZERO_COPY", 0) != 0,
        napi_us: env_u64("NVMEUBLK_NAPI_US", 0) as u32,
        conns_per_path: env_u64("NVMEUBLK_CONNS_PER_PATH", 1) as usize,
        rx_chunk: env_u64("NVMEUBLK_RX_CHUNK", 32 * 1024) as usize,
        threads_per_queue: env_u64("NVMEUBLK_THREADS_PER_QUEUE", 1) as u16,
        seq_tags: env_u64("NVMEUBLK_SEQ_TAGS", 0) != 0,
        tag_chunk: env_u64("NVMEUBLK_TAG_CHUNK", 1) as u16,
        io_timeout_ms: env_u64("NVMEUBLK_IO_TIMEOUT_MS", 5000),
        no_path_timeout_ms: env_u64("NVMEUBLK_NO_PATH_TIMEOUT_MS", 30000),
        write_fence_ms: std::env::var("NVMEUBLK_WRITE_FENCE_MS").ok().and_then(|v| v.parse().ok()),
    };
    // NVMEUBLK_RECOVER_ID after a crash of this command: writes are held.
    let recover = std::env::var("NVMEUBLK_RECOVER_ID").ok().and_then(|v| v.parse::<i32>().ok()).map(|id| (id, true));
    let r = device::start(spec, recover)?;
    let f = libublk::ctrl::UblkCtrl::new_simple(r.dev_id).map(|c| c.dev_info().flags).unwrap_or(0);
    log::info!(
        "ublk device flags {f:#x}: zero_copy={} user_copy={} user_recovery={} reissue={}",
        f & libublk::sys::UBLK_F_AUTO_BUF_REG as u64 != 0,
        f & libublk::sys::UBLK_F_USER_COPY as u64 != 0,
        f & libublk::sys::UBLK_F_USER_RECOVERY as u64 != 0,
        f & libublk::sys::UBLK_F_USER_RECOVERY_REISSUE as u64 != 0
    );
    // SIGINT: stop and delete the device (the daemon's SIGTERM is a handover instead).
    let dev_id = r.dev_id;
    ctrlc::set_handler(move || match libublk::ctrl::UblkCtrl::new_simple(dev_id) {
        Ok(c) => {
            if let Err(e) = c.kill_dev() {
                log::error!("stop ublk device {dev_id}: {e}");
            }
        }
        Err(e) => log::error!("open ublk device {dev_id} to stop it: {e}"),
    })?;
    let (st, cstat) = (r.stats.clone(), r.ctrls.clone());
    std::thread::spawn(move || {
      let mut last = (0u64, 0u64, 0u64);
      loop {
        std::thread::sleep(Duration::from_secs(5));
        let (n, w, t) = (st.done.load(Ordering::Relaxed), st.wire_ns.load(Ordering::Relaxed), st.total_ns.load(Ordering::Relaxed));
        let dn = (n - last.0).max(1);
        let g = |a: &std::sync::atomic::AtomicU64| a.swap(0, Ordering::Relaxed);
        let (qw, qn, wd, dc, rn, lp, ln) = (g(&st.q2w_ns), g(&st.q2w_n).max(1), g(&st.w2d_ns), g(&st.d2c_ns), g(&st.rd_n).max(1), g(&st.loops).max(1), g(&st.loop_ns));
        let qs = g(&st.q2s_ns);
        let (wv, wvn) = (g(&st.wv_ns), g(&st.wv_n).max(1));
        let (zr, zrn) = (g(&st.zc_rx_ns), g(&st.zc_rx_n).max(1));
        let lat = format!(
            "zc_MiB={} zc_tx_MiB={} linked_hdr={} zc_notif={} io={} wire_avg={}us total_avg={}us | queued->wired={}us (pickup {}us, writev {}us x{}) wired->1stdata={}us 1stdata->done={}us (zc payload rx {}us) | loops/s={} run_ops_avg={}us",
            st.zc_bytes.load(Ordering::Relaxed) >> 20, st.zc_tx_bytes.load(Ordering::Relaxed) >> 20, g(&st.linked_hdr), g(&st.zc_notif), n - last.0, (w - last.1) / dn / 1000, (t - last.2) / dn / 1000, qw / qn / 1000, qs / qn / 1000, wv / wvn / 1000, wvn / 5, wd / rn / 1000, dc / rn / 1000, zr / zrn / 1000, lp / 5, ln / lp / 1000
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
    r.wait()
}

fn main() -> Result<()> {
    // Thread stacks. mlockall (harden_for_writeback) faults in and locks every
    // mapping, so each thread's whole stack is resident and pinned: at Rust's
    // 2 MiB default that was ~17 x 2 MiB per volume. Queue threads are
    // spawned by libublk with the default size, which std takes from
    // RUST_MIN_STACK; set it before any thread exists. NVMEUBLK_STACK_KB
    // (default 256) overrides; an overflow aborts with a message, never
    // silently.
    if std::env::var_os("RUST_MIN_STACK").is_none() {
        let kb = std::env::var("NVMEUBLK_STACK_KB").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(256).max(64);
        // SAFETY: first thing in main, before any other thread is spawned.
        unsafe { std::env::set_var("RUST_MIN_STACK", (kb * 1024).to_string()) };
    }
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 3 && args[1] == "del" {
        let id: i32 = args[2].parse().context("device id")?;
        libublk::ctrl::UblkCtrl::new_simple(id)?.del_dev()?;
        println!("deleted ublk device {id}");
        return Ok(());
    }
    // Per-node daemon and its client.
    if args.len() >= 2 && args[1] == "daemon" {
        harden_for_writeback();
        let socket = args.get(2).map(String::as_str).unwrap_or(daemon::DEFAULT_SOCKET);
        let state = args.get(3).map(String::as_str).unwrap_or(daemon::DEFAULT_STATE);
        return daemon::run(socket, state);
    }
    if args.len() == 3 && args[1] == "ctl" {
        return daemon::ctl(daemon::DEFAULT_SOCKET, &args[2]);
    }
    if args.len() < 4 {
        bail!("usage: nvmeublk probe|lat|run <subnqn> <addr:port>... | daemon [socket] [state] | ctl '<json>' | del <id>");
    }
    match args[1].as_str() {
        "probe" => probe(&args[2], &args[3..]),
        "lat" => lat(&args[2], &args[3..]),
        "run" => run(&args[2], &args[3..]),
        c => bail!("unknown command {c}"),
    }
}

#[cfg(test)]
mod cpu_list_tests {
    #[test]
    fn cpu_list_parses_ranges_and_singles() {
        assert_eq!(super::parse_cpu_list("0-3,8, 10-11"), vec![0, 1, 2, 3, 8, 10, 11]);
        assert_eq!(super::parse_cpu_list("x,5,2-a"), vec![5]);
    }
}
