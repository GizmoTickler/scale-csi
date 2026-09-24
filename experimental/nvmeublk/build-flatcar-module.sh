#!/bin/bash
# Build ublk_drv.ko for a Flatcar node whose kernel ships without CONFIG_BLK_DEV_UBLK.
#   build-flatcar-module.sh <node-ssh> <flatcar-version> <kernel-tag>
#   e.g. build-flatcar-module.sh core@node1 4593.2.5 v6.12.102
# Uses the node's own kernel build tree (/usr/lib/modules/<kver>/build) and the
# Flatcar developer container's toolchain, so the compiler matches
# CONFIG_CC_VERSION_TEXT. Needs root for systemd-nspawn. Output: ./out/ublk_drv.ko
set -euo pipefail
NODE=$1 VER=$2 TAG=$3
W=$(pwd)/flatcar-build; mkdir -p "$W/src" "$W/tree" out
ssh "$NODE" 'tar -C /usr/lib/modules/$(uname -r) -czf - build' | tar -C "$W/tree" -xzf -
curl -sfL "https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git/plain/drivers/block/ublk_drv.c?h=$TAG" -o "$W/src/ublk_drv.c"
printf 'obj-m := ublk_drv.o\n' > "$W/src/Makefile"
[ -f "$W/devc.bin" ] || curl -sfL "https://stable.release.flatcar-linux.net/amd64-usr/$VER/flatcar_developer_container.bin.bz2" | bunzip2 > "$W/devc.bin"
sudo systemd-nspawn --quiet --image="$W/devc.bin" --read-only --private-network \
  --bind="$W/tree/build:/mnt" --bind="$W/src:/tmp/src" --pipe \
  /bin/bash -c 'make -C /mnt M=/tmp/src modules'
cp "$W/src/ublk_drv.ko" out/
modinfo out/ublk_drv.ko | grep vermagic
# Every imported symbol must be exported by the node kernel (no CONFIG_MODVERSIONS CRCs involved).
for s in $(nm -u out/ublk_drv.ko | awk '{print $2}'); do
  grep -qP "\t$s\t" "$W/tree/build/Module.symvers" || { echo "unexported symbol: $s"; exit 1; }
done
echo "ok: out/ublk_drv.ko"
