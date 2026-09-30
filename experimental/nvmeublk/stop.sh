#!/bin/bash
# stop.sh: SIGINT the daemon and wait (as root) until it exits and its device is gone.
P=$(cat /var/tmp/nvmeublk.pid)
sudo kill -INT "$P"
for i in $(seq 1 60); do sudo kill -0 "$P" 2>/dev/null || { echo "stopped in $((i/2))s"; exit 0; }; sleep 0.5; done
echo "daemon $P did not exit"; exit 1
