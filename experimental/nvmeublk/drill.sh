#!/bin/bash
# Failover drill: continuous verified writes while paths are removed and restored.
set -u
T="$(dirname "$0")/nvmet-local.sh"
log(){ echo "$(date +%T) $*"; }
mountpoint -q /mnt/ublk || { echo "ABORT: /mnt/ublk is not mounted"; exit 1; }
sudo fio --name=drill --filename=/mnt/ublk/drill --size=768M --rw=randwrite --bsrange=4k-128k \
  --ioengine=io_uring --iodepth=32 --direct=1 --time_based --runtime=60 \
  --verify=crc32c --verify_backlog=512 --verify_fatal=1 --output=/var/tmp/drill-fio.txt &
FIO=$!
sleep 6;  log "port 1 down"; sudo $T port-down 1
sleep 4;  log "port 2 down"; sudo $T port-down 2
sleep 4;  log "port 3 down (only path 4 left)"; sudo $T port-down 3
sleep 5;  log "ports 1-3 up"; sudo $T port-up 1; sudo $T port-up 2; sudo $T port-up 3
sleep 6;  log "port 4 down"; sudo $T port-down 4
sleep 5;  log "ALL ports down for 8s (I/O must park, not fail)"; for p in 1 2 3; do sudo $T port-down $p; done
sleep 8;  log "all ports up"; for p in 1 2 3 4; do sudo $T port-up $p; done
wait $FIO; echo "fio exit=$?"
grep -E 'err=|WRITE:|verify|lat \(msec\).*max|clat.*max' /var/tmp/drill-fio.txt | head -8
