#!/bin/bash
# Local NVMe/TCP test target for the nvmeublk prototype. up|down|port-down N|port-up N
set -euo pipefail
NQN=nqn.2026-09.lab.nvmeublk:proto
CFG=/sys/kernel/config/nvmet
BACK=/var/tmp/nvmeublk-backing.img
case "${1:-}" in
up)
  modprobe nvmet nvmet-tcp
  [ -f "$BACK" ] || truncate -s 2G "$BACK"
  mkdir -p $CFG/subsystems/$NQN
  echo 1 > $CFG/subsystems/$NQN/attr_allow_any_host
  mkdir -p $CFG/subsystems/$NQN/namespaces/1
  echo -n "$BACK" > $CFG/subsystems/$NQN/namespaces/1/device_path
  echo 1 > $CFG/subsystems/$NQN/namespaces/1/enable
  for i in 1 2 3 4; do
    ip addr add 127.0.0.1$i/8 dev lo 2>/dev/null || true
    mkdir -p $CFG/ports/$i
    echo tcp > $CFG/ports/$i/addr_trtype; echo ipv4 > $CFG/ports/$i/addr_adrfam
    echo 127.0.0.1$i > $CFG/ports/$i/addr_traddr; echo 4420 > $CFG/ports/$i/addr_trsvcid
    ln -sfn $CFG/subsystems/$NQN $CFG/ports/$i/subsystems/$NQN
  done ;;
port-down) rm -f $CFG/ports/$2/subsystems/$NQN ;;
port-up) ln -sfn $CFG/subsystems/$NQN $CFG/ports/$2/subsystems/$NQN ;;
down)
  for i in 1 2 3 4; do rm -f $CFG/ports/$i/subsystems/$NQN; rmdir $CFG/ports/$i 2>/dev/null || true; done
  echo 0 > $CFG/subsystems/$NQN/namespaces/1/enable 2>/dev/null || true
  rmdir $CFG/subsystems/$NQN/namespaces/1 $CFG/subsystems/$NQN 2>/dev/null || true ;;
*) echo "usage: $0 up|down|port-down N|port-up N"; exit 2 ;;
esac
