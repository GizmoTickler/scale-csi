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
- `src/mpath.rs`: multipath.
  - Least-outstanding path selection; failover of in-flight I/O.
  - Queue-if-no-path with a deadline (`NVMEUBLK_NO_PATH_TIMEOUT_MS`).
  - Per-path supervisor thread for reconnect with backoff and keep-alive.
  - Watchdog that kills a path whose oldest request exceeds `NVMEUBLK_IO_TIMEOUT_MS`. This is the silent-link case a socket error never reports.
- `src/main.rs`: modes `probe` (protocol test), `lat` (engine-only latency), `run` (ublk device) and `del` (remove a stale device). There is one libublk async task per tag; the engine wakes a tag through its own eventfd read on the queue's io_uring.

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
- **It costs about 170 µs at QD1.** Before this could replace the kernel path for databases, the thread hops need collapsing: drive the sockets from the ublk queue's own io_uring (one ring per queue, send/recv as SQEs) instead of separate sender and receiver threads.
- **Operational cost.** An out-of-tree module has to be rebuilt per Flatcar kernel and shipped as a version-pinned sysext. A crash or restart of the daemon stalls I/O on that node's volumes until ublk user recovery (`UBLK_F_USER_RECOVERY`) reattaches; that is not implemented here.
