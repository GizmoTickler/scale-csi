# nvmeublk — userspace NVMe/TCP + multipath, exposed via ublk (prototype)

A Rust userspace NVMe/TCP initiator with its own multipath, serving a Linux
block device through [ublk](https://docs.kernel.org/block/ublk.html)
(`/dev/ublkbN`). Built to test whether scale-csi's node data path can leave the
kernel `nvme-tcp` + native multipath stack.

## Layout
- `src/pdu.rs`: NVMe/TCP PDU framing (ICReq/ICResp, CapsuleCmd/Resp, C2HData, R2T, H2CData) and SQE/CQE layout. No digests.
- `src/conn.rs`: one path's controller.
  - Synchronous admin queue: Connect, CC.EN, CSTS.RDY, Identify, Keep-Alive.
  - One pipelined I/O queue with a CID pool.
  - Receiver thread: C2HData goes straight into the ublk buffer; answers R2T.
  - Batching sender thread: vectored writes; payloads borrowed from the request buffer, with no copy.
- `src/ctrls.rs`: per-path admin controllers for `run`. Connect, enable, identify; a supervisor thread per path for keep-alive and reconnect with backoff. An epoch per path is bumped whenever its controller is lost or replaced, so queues can tell that their I/O connection belongs to a dead controller.
- `src/qengine.rs`: the `run` data path (v3). Every ublk queue drives its own NVMe/TCP I/O connections, one per path, as SQEs on the queue's own io_uring: batched `Writev` sends, `Recv` into a staging buffer, and large C2HData payloads received straight into the request buffer. There are no sender or receiver threads and no cross-thread wakeups. Also: least-outstanding path choice, failover, parking with a deadline, the stall watchdog (keyed on each attempt's send time), epoch checks, and fault injection.
- `src/mpath.rs`: the v2 thread-per-path engine, now used only by `probe` and `lat`.
  - Least-outstanding path selection; failover of in-flight I/O.
  - Queue-if-no-path with a deadline (`NVMEUBLK_NO_PATH_TIMEOUT_MS`).
  - Per-path supervisor thread for reconnect with backoff and keep-alive.
  - Watchdog that kills a path whose oldest request exceeds `NVMEUBLK_IO_TIMEOUT_MS`. This is the silent-link case a socket error never reports.
- `src/main.rs`: modes `probe` (protocol test), `lat` (engine-only latency), `run` (ublk device) and `del` (remove a stale device). There is one libublk async task per tag. The device is created with `UBLK_F_USER_RECOVERY | UBLK_F_USER_RECOVERY_REISSUE`: if the daemon dies, the kernel holds the device and its I/O, and `NVMEUBLK_RECOVER_ID=<dev id> nvmeublk run ...` reattaches a fresh daemon and reissues the held I/O.

## Test rig (all local on a dev VM; nothing touches the storage server or a cluster)
`nvmet-local.sh up` starts a kernel nvmet target on 127.0.0.11-14:4420, with two subsystems:
- `:proto`: a 2 GiB file.
- `:null`: `null_blk`, to measure initiator overhead.

`start.sh`/`stop.sh` run the daemon, `drill.sh` is the failover drill, and `cpubench.sh` is the fio matrix plus system-wide busy cores.

## Results (2026-09-24, dev VM: kernel 7.2.3, 16 vCPU, loopback)
Correctness, with fio crc32c verification running continuously:
- The protocol probe verifies byte-exact at 4K (in-capsule), 128K (R2T) and 1M (multi-PDU).
- ext4 on `/dev/ublkb0`: mkfs, mount, and 512 MiB of mixed-size random writes verify with `err=0`.
- Failover drill, 60 s and 7.4 GiB verified with `err=0`:
  - Paths removed one at a time down to one, then restored.
  - The last path killed.
  - All 4 down for 8 s: 37 requests parked, 0 EIO, resumed on return.
  - 7 failovers and 98 resubmits in total.
- Silent-drop drill (nftables drop, no RST): the watchdog killed the path at 5.1 s (= io_timeout), in-flight I/O failed over, `err=0`.

Performance on the `null_blk` target (4K random at 4 jobs × QD32; 128K sequential at QD16). The kernel uses native multipath with the `queue-depth` iopolicy:

| | kernel nvme-tcp | nvmeublk v1 | nvmeublk v2 (batching sender) |
|---|---|---|---|
| 4K randread IOPS | 38.0K | 12.2K | **64.8K** |
| 4K randwrite IOPS | 38.3K | 12.1K | **103.8K** |
| 128K seq read | 482 MiB/s | 962 MiB/s | **1,738 MiB/s** |
| K IOPS per busy core (4K read) | 8.4 | ~5 | **9.0** |
| QD1 4K read latency | ~23 µs clat | ~172 µs engine-only | ~205 µs engine-only (+ ~55 µs ublk) |

**Caveats. Read these before believing the table.**
- Loopback flatters batching. The sender executes the peer's TCP receive inline, so coalescing many PDUs into one `writev` amortizes the *target's* work too. On a real NIC the gap will be smaller.
- QD1 latency is much worse than the kernel's (about 9×): every I/O crosses 2 or 3 thread hops (ublk queue thread, sender, receiver). Latency-sensitive databases would feel this.
- One I/O queue per path; no digests, no TLS, no ANA; nsid is fixed at 1; `mdts` is honored only by capping the ublk buffer size.

## v3: io_uring-native engine + crash recovery (2026-09-24, same dev VM)
Correctness, with fio crc32c verification running continuously:
- Failover drill (the same script as v2): 60 s, 5.0 GiB verified, `err=0`. All 4 paths down for 8 s parks I/O and resumes it; 0 EIO.
- Fault injection: `stall 1`, then `kill 2`, then `stall 0`, during 40 s of verified writes. `err=0`. Only the stalled path is killed, at io_timeout (5 s); the worst-case I/O latency is 5.0 s.
- **Daemon crash:** `kill -9` of the daemon 8 s into a 30 s verified fio run on a *mounted* ext4, then restart with `NVMEUBLK_RECOVER_ID=0` 3 s later. The kernel held the I/O, the new daemon reattached and reissued it, fio finished with `err=0` (2.0 GiB), and the worst-case I/O stalled 3.3 s (the restart gap). `fsck.ext4 -fn` is clean afterwards.

Performance on the `null_blk` target, v2 vs v3 back to back:

| | v2 (threads) | v3 (io_uring) |
|---|---|---|
| QD1 4K randread, fio mean | 274 µs | **210 µs** (−23%) |
| 4K randread 4×QD32 | 57.7K IOPS, 8.2K/core | **92–106K IOPS, 22.4K/core** |
| 4K randwrite 4×QD32 | 84.3K IOPS, 11.3K/core | **101–107K IOPS, 24–26K/core** |
| 128K seq read, 4 jobs × QD16 | 1,536 MiB/s | **1,913 MiB/s** |
| 128K seq read, 1 job × QD16 | **1,723 MiB/s** | 510–540 MiB/s |

Known limitation: one ublk queue now does all of its paths' network work on one thread, so a *single* large-block stream is capped at about 550 MB/s per queue. v2 spread one queue's receives across 4 path threads. Aggregate throughput across queues is higher than v2. The cap is not the staging copy (receiving straight into the request buffer did not move it), not the socket receive buffer (4–16 MiB: no change), and not CPU (the queue thread runs at about 40%). It behaves like a serialized wait per queue and is still open. A likely next step is spreading one queue's paths over helper rings, or ublk zero-copy (kernel ≥ 6.15, unavailable on Flatcar's 6.12).

Bugs found in v3 while testing:
- The stall watchdog keyed on the request's *first* submit time. A request failed over from a stalled path is already old, so it condemned every healthy path it landed on: one stalled path killed all four. Fix: a per-attempt `sent` timestamp.
- The I/O sockets were `O_NONBLOCK`. io_uring honors that and returns `-EAGAIN` instead of arming a poll, so the receiver resubmitted in a spin. Fix: blocking fds; io_uring does the waiting.

## v3 real-fabric re-run (2026-09-24, same worker, same procedure as the v2 run below)
The operator approved it; the setup was the same non-persistent, single-node `insmod` with a throwaway 16 GiB PVC. Nothing touched shared links.
- **Failover drill** (the v2 script: kill 0, kill 1, stall 2, kill 3 during 45 s of verified writes): 8.2 GiB, `err=0`. The stall was killed at io_timeout on path 2 only, on each queue; 7 failovers; 0 EIO.
- **Daemon crash:** `kill -9` 8 s into 30 s of verified writes, then restart with `NVMEUBLK_RECOVER_ID=0` 3 s later. 5.2 GiB, `err=0`; the worst-case I/O stalled 3.1 s.

Same zvol, 8 s runs; the kernel uses native multipath with the queue-depth iopolicy:

| | kernel | nvmeublk v3 | Δ | (v2: Δ) |
|---|---|---|---|---|
| 4K randread 4×QD32 | 57.8K IOPS | **79.9K** | **+38%** | −7% |
| 4K randwrite 4×QD32 | 54.8K IOPS | **74.8K** | **+37%** | +20% |
| 128K seq read 1×QD16 | 955 MiB/s | 725 | −24% | +26% |
| 128K seq read 4×QD16 | 2,817 MiB/s | 1,552 | −45% | — |
| 128K seq write 1×QD16 | 621 MiB/s | 559 | −10% | — |
| 4K randread QD1 | 356 µs | 488 µs | +132 µs | +170 µs |
| 4K randwrite QD1 | 333 µs | 472 µs | +139 µs | — |

Reading it:
- v3 wins small-block throughput outright.
- The QD1 gap narrowed by only about 40 µs. Most of what is left is the ublk round trip plus userspace scheduling, not thread hops.
- Large-block reads regressed badly against both v2 and the kernel, even with 4 jobs, so the per-queue serialization seen on loopback (above) is real and matters on a real NIC. Next step: find that serialized wait. Candidates: one ring doing both network and ublk completion for a queue, and the ublk read copy happening at commit time on the same thread.

Cleanup: the daemon stopped and its device was removed; the namespace, PVC and PV were deleted; `rmmod ublk_drv`; files removed. No Released or terminating PVs.

## v3.1: safety review fixes, and why large reads regressed (2026-09-24)
### Safety fixes (from an independent review; each verified)
- **Write fencing.** A write orphaned by a failed path used to be re-sent on another path at once. If the old target still executed it later, it could overwrite a newer write to the same LBA that had been acknowledged. Reads still fail over at once. Writes and flushes now wait `NVMEUBLK_WRITE_FENCE_MS` (default KATO + 5 s = 20 s) before they are re-sent. Any data-path failure also tears down that path's whole controller on every queue, not one I/O socket. The request is tagged with the connection's epoch, so a late report cannot kill a fresh controller.
- **Crash recovery** (`NVMEUBLK_RECOVER_ID`) holds every write, including the kernel's reissued ones, for one fence after start. It keeps retrying its first connect instead of exiting and leaving the device frozen.
- **Hang: parked I/O never moved when a command slot freed** (depth > target MQES). The slot test: one path, depth 256, QD256. The old code hung (fio timed out, 130 stuck); now `err=0`.
- **Hang: shutdown waited on parked I/O forever** with every path down and `NO_PATH_TIMEOUT_MS=0`. Old code: SIGINT did not exit (16 s, until the paths came back). Now it exits in 0.65 s, and the parked read gets EIO. One queue's loop ending no longer stops the other queues' timers.
- **Untrusted target input.** Every PDU header is validated before use (`pdu::check_pdu_header`, unit-tested with 14 malformed cases), and the receiver no longer panics on a bad header:
  - hlen/pdo/plen bounds;
  - digest flags;
  - SUCCESS only with LAST;
  - C2H only for reads, R2T only for writes;
  - in-order data offsets.

  A read completes only when every byte has arrived. Command ids carry a generation counter, as the kernel's genctr does. An error response while write data is still queued tears the connection down, so a stale H2CData can never be sent under a reused transfer tag.
- **Path errors** (SCT 3 without DNR) fail over instead of returning EIO.

Verification, all with fio crc32c:
- failover drill 5.2 GiB `err=0`;
- stall + kill with mixed read/write 7.2 GiB `err=0`;
- crash recovery `err=0` (worst stall 23 s = restart gap + fence), fsck clean.

Cost: the worst-case latency of an orphaned write is now about the fence (20–25 s). A read on the raw device during the same failures stayed at p99.9 6.8 ms.

### Why v3 lost large reads to v2, and what did not fix it
Measured on the real worker, today, same zvol (128K reads, QD16):

| | kernel | v2 (threads) | v3 |
|---|---|---|---|
| 1 job, pinned | 1,322 MiB/s | 1,213 | 764–862 |
| 1 job | 976 | 1,255 | 658–867 |
| 4 jobs | 2,757 | 2,005 | 1,272–1,721 |
| 4K randread 4×QD32 | 59K IOPS | 54K | 68–83K |

- On 6.12, a read byte is copied twice in the userspace path: socket → buffer, then ublk's commit copy into the bio pages. The kernel initiator copies once.
- v3 does every step of a queue's I/O on one thread and one ring: capsule transmit (through netfilter), receive, the commit, and the NIC softirq that lands on that CPU. That comes to about 115 µs of serial kernel work per 128K read, and each read needs several turns of that loop.
- v2 ran send, receive and commit on separate threads, so they overlapped.

Tried on the fabric; none moved the ceiling, so all are kept only as off-by-default knobs:
- offloading the bulk payload receive to a per-connection helper (`NVMEUBLK_RX_OFFLOAD`), with its CPU affinity freed from the queue's CPU group;
- ublk `USER_COPY`, with both copies done on the helper while the data is cache-hot (`NVMEUBLK_USER_COPY`);
- 2 or 4 connections per path (`NVMEUBLK_CONNS_PER_PATH`);
- 8–32K staging chunks (`NVMEUBLK_RX_CHUNK`).

The loopback rig also misled for a while. The local target ran on the daemon's own CPU and preempted it mid-send, which made v3 look 3× worse than it is. It is now run with `lo` RPS steered to other CPUs and the daemon pinned away from them. Also, `splice` into `/dev/ublkcN` is refused by the 6.12 driver (`user_backed_iter`), so a single-copy path needs ublk zero-copy (kernel ≥ 6.15).

## Zero copy on kernels >= 6.16 (`NVMEUBLK_ZERO_COPY=1`, 2026-09-24)
The device is created with `UBLK_F_AUTO_BUF_REG` (plus `USER_COPY`). The kernel then registers each read request's own pages in the queue ring's fixed-buffer table at index = tag. The bulk of a C2HData payload is read from the socket with `IORING_OP_READ_FIXED` straight into those pages (a fixed-buffer `RECV` is refused with EINVAL on 6.19; the generic read path works on every kernel with AUTO_BUF_REG): one copy, as in the kernel initiator, and no copy at commit. Payload bytes that arrived with a PDU header in the staging buffer, and small reads, go in with `pwrite` on `/dev/ublkcN`. Writes are unchanged. The daemon refuses the flag on a kernel whose ublk lacks the features. Every Flatcar channel (Stable/Beta/Alpha) ships 6.12, so this cannot run on the current cluster nodes.

Dev VM (kernel 7.2.3, loopback with the target steered off the daemon's CPUs), same binary with the flag off/on:
- 128K reads, 4 jobs: 2,412–2,532 → 3,032–3,247 MiB/s (+25%).
- 128K reads, one stream: 1,675–1,876 → 1,690–2,572 MiB/s (noisy; the queue thread is saturated either way on loopback).
- 4K randread 4×QD32: 211–225K → 194–197K IOPS (the extra `pwrite` per small read).

Correctness: verified write/verify-only pass `err=0`; failover drill `err=0` with 4 GiB of verification reads received zero-copy while paths were killed; fsck clean. Not yet measured on the real fabric, where the commit copy was about 30% of the queue thread's time: that needs a >= 6.16 host on the storage network.

### Zero copy on the real fabric (2026-09-24)
Setup: a throwaway Fedora 44 VM (kernel 6.19.10, 16 vCPU) on the same hypervisor as the cluster workers, attached to the four storage networks exactly like a worker. A dedicated 16 GiB zvol was prefilled with data, exported by its own subsystem admitting only that VM's host NQN. All three data paths ran on the same VM and volume, in two passes in opposite order (ranges are the two passes). The kernel path is native multipath with the queue-depth iopolicy.

| | kernel nvme-tcp | nvmeublk copy | nvmeublk zero copy |
|---|---|---|---|
| 128K read, 1 job, pinned | 958–1,104 MiB/s | 703–721 | 746–805 |
| 128K read, 1 job | 781–797 | 781–879 | **889–955** |
| 128K read, 4 jobs | 2,586–2,602 | 1,933–2,038 | **2,373–2,471** |
| 128K write, 1 job | 620–621 | 646–796 | 676–696 |
| 4K randread 4×QD32 | 52.0–52.6K IOPS | **107–110K** | 99–101K |
| 4K randwrite 4×QD32 | 53–56K | **96–98K** | 87K |
| 4K randread QD1 | **267–288 µs** | 412–414 | 393–404 |
| CPU per GiB, 128K 4 jobs | 2.08–2.10 core-s | 2.41–2.46 | **2.08–2.16** |
| CPU per GiB, 4K randread | 23.6–23.8 core-s | **12.3–13.0** | 13.4–13.7 |

- Zero copy recovers most of the large-read gap: 4 jobs go from 76–78% to 92–95% of the kernel, it beats the kernel on an unpinned single stream, and CPU per GiB matches the kernel's.
- Small random I/O stays at about 2× the kernel's IOPS for about half the CPU per GiB.
- QD1 latency is still about 120 µs behind the kernel.

Safety on the fabric, zero copy, fio crc32c:
- failover drill (kill, kill, silent stall, kill; random read/write) 15 GiB, `err=0`, 8 GiB of it received zero-copy;
- `kill -9` + `NVMEUBLK_RECOVER_ID` 12.5 GiB, `err=0`.

### Tuning levers on the fabric (2026-09-24, same VM and zvol, two passes each)
Zero copy plus 8 ublk queues (on 16 vCPUs) plus NAPI busy poll (`NVMEUBLK_NAPI_US=50`), against the kernel initiator:

| | kernel nvme-tcp | zero copy, 8 queues, NAPI 50 µs |
|---|---|---|
| 128K read, 1 job, pinned | 950–998 MiB/s | 928–1,098 |
| 128K read, 1 job | 753–764 | **818–826** (+8%) |
| 128K read, 4 jobs | 2,537–2,551 | **2,812–2,872** (+11–13%) |
| 128K write, 1 job | **607–626** | 519–566 (4 queues: 662–766) |
| 4K randread 4×QD32 | 53.6–54.5K IOPS | **105–109K** (2×) |
| 4K randwrite 4×QD32 | 53.7–55.5K | **85–90K** (1.6×) |
| 4K randread QD1 | **284–315 µs** | 360–389 µs |
| CPU per GiB, 4K randread | 22.9–23.2 core-s | **14.9–15.3** |

- NAPI busy poll alone (4 queues) cut QD1 from 416 to 353–367 µs for about 1.5 more cores busy during the QD1 run.
- 16 queues (one per vCPU, the kernel's layout) hurt small I/O (83–91K) and QD1 (530 µs). A single high result from a pinned stream (1,189 MiB/s) did not reproduce, so it is discarded.
- `DEFER_TASKRUN` + `SINGLE_ISSUER` rings gave no gain, and the queue threads spun at 100% after `kill_dev` instead of exiting. The knob was removed.
- Rig lesson: stop checks must wait for the daemon process to exit, not only for `/dev/ublkbN` to vanish. A daemon that stays alive keeps its device number, the next device comes up as `ublkb1`, and fio pointed at a missing `/dev/ublkb0` silently benchmarks a RAM-backed regular file it created (24 GB/s "results"). `fab`-style scripts now refuse to run unless the target is a block device, and refuse to start when anything stale is present.

Still behind the kernel: QD1 latency (by about 75 µs) and single-stream large writes. The write path still does two copies (`pread` from the request, then send). A zero-copy send from the registered buffer is the next lever there.

### Beating the kernel on latency too: longer busy poll (2026-09-24)
The network round trip here is about 250 µs, so a 50 µs NAPI budget mostly still ends in sleep. With a longer budget the queue thread is still polling when the reply and the next ublk request arrive, so neither needs an interrupt plus scheduler wakeup. Same VM and zvol; zero copy, 8 queues, `NVMEUBLK_NAPI_US=200`; the kernel was measured before and after:

| | kernel nvme-tcp | nvmeublk |
|---|---|---|
| 4K randread QD1 | 285–298 µs | **241–257 µs** |
| 128K read, 1 job, pinned | 964 MiB/s | **1,118** (+16%) |
| 128K read, 1 job | 765 | **1,020** (+33%) |
| 128K read, 4 jobs | 2,583 | **3,000** (+16%) |
| 4K randread 4×QD32 | 54K IOPS | **104K** |
| 4K randwrite 4×QD32 | 47K | **90K** |
| 128K write, 1 job | **630 MiB/s** | 535 |

Budget sweep (8 queues): 50 µs gives QD1 334 µs; 200 µs gives 241 µs; 1,000 µs gives 234 µs. The cost is CPU: about 1.1 busy cores during the QD1 run, against 0.4 for the kernel. Throughput runs are unaffected.

Zero-copy writes: payloads of writes larger than the in-capsule size go out via R2T straight from the request's registered pages (`IORING_OP_WRITE_FIXED` on the socket; a fixed-buffer `SEND` is refused by 6.19). Verified: 3 GiB of 64K–1M writes, every byte sent zero-copy, `err=0`; mixed failover drill 30 GiB `err=0`. It does not change single-stream 128K write throughput: every path, kernel included, lands at 520–800 MiB/s there, so that test is bound by the target. With 8 queues nvmeublk sits at the low end (519–628); with 4 queues, at 662–766.

## Bugs found and fixed while building it
- A single maintenance thread ran keep-alive, and keep-alive blocked for 10 s on a silent path. That froze the stall watchdog, so failover took 14 s instead of 5 s. Fix: a supervisor thread per path; the watchdog is non-blocking.
- The receiver tore down the admin queue before resubmitting orphans; that could wait behind a blocked keep-alive. Fix: fail over first.
- Shutdown killed the paths before stopping ublk, so in-flight I/O parked and the device stop waited out the no-path timeout. Also, libublk's control ring is thread-local, so `kill_dev()` from the Ctrl-C thread panicked. Fix: stop the device first, from a handler-owned control handle; complete parked I/O with EIO on shutdown.
- The ublk queue thread did socket writes inline while holding the connection lock, which capped throughput near 12K IOPS. Fix: a per-connection batching sender.

## Toward the cluster
Flatcar 4593.2.5 ships `# CONFIG_BLK_DEV_UBLK is not set`. It does not enforce module signatures (`MODULE_SIG_FORCE` off, lockdown none, Secure Boot off), so `ublk_drv.ko` built against the exact kernel can ship as a Flatcar sysext (`usr/lib/modules/6.12.102-flatcar/`).

## Flatcar module status (2026-09-24)
- `ublk_drv.ko` was built (see `build-flatcar-module.sh`) from the v6.12.102 stable source with the Flatcar 4593.2.5 developer container's gcc 14.3.1, against the node's own build tree.
- vermagic is `6.12.102-flatcar SMP preempt mod_unload`; all 118 imported symbols are exported by the node kernel's `Module.symvers`.
- It has **not** been installed on any node. Packaging it as a sysext with a boot-time `insmod` unit was blocked by the auto-mode classifier as persistence, and needs an operator decision.

## Real-fabric test (2026-09-24, one Flatcar Kubernetes worker → TrueNAS 26 nvmet, 4 paths)
The operator approved a non-persistent, single-node test.
- `insmod` of the built `ublk_drv.ko` on one worker (Flatcar 4593.2.5).
- The `scale-csi-test` namespace: one 16 GiB raw-block PVC plus a consumer pod, so scale-csi provisioned and published a real volume.
- nvmeublk ran in a privileged pod with the node's own NVMe identity, taken from an existing controller's sysfs `hostnqn`/`hostid`; Flatcar has no `/etc/nvme/hostnqn`. With a foreign identity, strict fencing rejected the Connect with 0x8308 (Connect Invalid Host), as intended.

Failure drills used in-process fault injection (`echo "kill N" | "stall N" > /run/nvmeublk-fault`), so the storage links that production volumes share were never touched. Result: 12.4 GiB of crc32c-verified random writes, `err=0`, at 302 MiB/s, through:
- 2 hard path kills;
- 1 silent stall, killed by the watchdog at 5.005 s with 4 in-flight requests failed over;
- 1 more kill.

Every path reconnected in under 1 s; 0 EIO.

Same zvol, 8 s runs:

| | kernel nvme-tcp (queue-depth) | nvmeublk | Δ |
|---|---|---|---|
| 4K randread 4×QD32 | 59.7K IOPS | 55.6K | −7% |
| 4K randwrite 4×QD32 | 56.3K IOPS | 67.4K | +20% |
| 128K seq read QD16 | 976 MiB/s | 1,229 MiB/s | +26% |
| 4K randread QD1 | 337 µs | 507 µs | +170 µs |

Cleanup: nvmeublk stopped cleanly; the namespace, PVC and PV were deleted (the driver removed the zvol); `rmmod ublk_drv`; files removed. The cluster is back to its pre-test PVC and attachment counts. The worker keeps the kernel taint flag (O+E, 12288) until its next reboot.

## Assessment
- **Viable.** Userspace NVMe/TCP multipath over ublk is correct under path loss and silent stalls, and it matches or beats the kernel on throughput over the real fabric.
- **It cost about 170 µs at QD1 over the fabric (v2).** v3 collapses the thread hops onto the queue's own io_uring and cuts QD1 by 23% on loopback. It has not been re-measured on the fabric yet.
- **Operational cost.** An out-of-tree module has to be rebuilt per Flatcar kernel and shipped as a version-pinned sysext. v3 implements ublk user recovery: a daemon crash stalls I/O (without failing it) until a new daemon reattaches with `NVMEUBLK_RECOVER_ID`. In a cluster, a supervisor such as systemd or a DaemonSet restart has to do the relaunch; that is not built here.
