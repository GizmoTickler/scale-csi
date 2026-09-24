#!/bin/bash
# cpubench.sh <device> <label>: bench matrix per test, plus system-wide busy cores
# (captures kernel kworkers and the userspace daemon alike).
D=$1; L=$2
for spec in "randread 4k 32 4" "randwrite 4k 32 4" "read 128k 16 1"; do
  set -- $spec
  read -r a b c d e f g h _ < <(grep '^cpu ' /proc/stat | cut -d' ' -f3-); s0=$((a+b+c+d+e+f+g+h)); i0=$((d+e))
  r=$(sudo fio --name=b --filename=$D --rw=$1 --bs=$2 --iodepth=$3 --numjobs=$4 --group_reporting \
    --ioengine=io_uring --direct=1 --time_based --runtime=12 --size=1G --output-format=json 2>/dev/null)
  read -r a b c d e f g h _ < <(grep '^cpu ' /proc/stat | cut -d' ' -f3-); s1=$((a+b+c+d+e+f+g+h)); i1=$((d+e))
  echo "$r" | python3 -c "
import json,sys; j=json.load(sys.stdin)['jobs'][0]; k='write' if 'write' in '$1' else 'read'; s=j[k]
p=s['clat_ns'].get('percentile',{}); busy=(($s1-$s0)-($i1-$i0))/($s1-$s0)*$(nproc)
print(f'$L {\"$1\":9} bs=$2 qd=$3x$4 iops={s[\"iops\"]:>8.0f} bw={s[\"bw\"]/1024:>6.0f}MiB/s p50={p.get(\"50.000000\",0)/1000:>6.0f}us p99={p.get(\"99.000000\",0)/1000:>6.0f}us sys-busy-cores={busy:4.1f} kiops/core={s[\"iops\"]/1000/max(busy,0.01):5.1f}')"
done
