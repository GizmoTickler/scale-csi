#!/bin/bash
# bench.sh <device> <label>: identical fio matrix, JSON summarized.
D=$1; L=$2
for spec in "randread 4k 32 4" "randwrite 4k 32 4" "read 128k 16 1"; do
  set -- $spec
  sudo fio --name=b --filename=$D --rw=$1 --bs=$2 --iodepth=$3 --numjobs=$4 --group_reporting \
    --ioengine=io_uring --direct=1 --time_based --runtime=12 --size=1G --output-format=json 2>/dev/null |
  python3 -c "
import json,sys; j=json.load(sys.stdin)['jobs'][0]; k='write' if 'write' in '$1' else 'read'; s=j[k]
p=s['clat_ns']['percentile']
print(f'$L {\"$1\":9} bs=$2 qd=$3x$4  iops={s[\"iops\"]:>9.0f}  bw={s[\"bw\"]/1024:>7.0f}MiB/s  p50={p[\"50.000000\"]/1000:>7.0f}us  p99={p[\"99.000000\"]/1000:>7.0f}us  cpu(usr+sys)={j[\"usr_cpu\"]+j[\"sys_cpu\"]:.0f}%')"
done
