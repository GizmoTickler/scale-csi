#!/bin/bash
# start.sh [subnqn]: start the ublk target in the background, PID in /var/tmp/nvmeublk.pid.
NQN=${1:-nqn.2026-09.lab.nvmeublk:proto}
cd "$(dirname "$0")"
sudo sh -c "NVMEUBLK_QUEUES=${QUEUES:-4} RUST_LOG=info exec ./target/release/nvmeublk run $NQN 127.0.0.11:4420 127.0.0.12:4420 127.0.0.13:4420 127.0.0.14:4420 > /var/tmp/nvmeublk.log 2>&1 & echo \$! > /var/tmp/nvmeublk.pid"
