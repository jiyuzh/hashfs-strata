#!/bin/bash -x
set -e
HOME=/home/iangneal
SRCROOT=${HOME}/workspace/strata-gefei
KERNFS=${SRCROOT}/kernfs/tests/kernfs
DB_BENCH=${SRCROOT}/bench/leveldb/build/db_bench
FILEB=${SRCROOT}/bench/filebench/filebench
MKFS=${SRCROOT}/libfs/bin/mkfs.mlfs
LIBSHIM=${SRCROOT}/shim/libshim/libshim.so
JEMALLOC=${SRCROOT}/libfs/lib/jemalloc-4.5.0/lib/libjemalloc.so.2
UTILS=${SRCROOT}/utils
function run() {
  LD_PRELOAD=${LIBSHIM}:${JEMALLOC} LD_LIBRARY_PATH=${SRC_ROOT}/libfs/lib/nvml/src/nondebug/:${SRC_ROOT}/libfs/build:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.4.0/:/lib64:/usr/local/lib64 MLFS=1 $@
}
function mkfs() {
	${MKFS} 1
	${MKFS} 4
}
# $1 config file
function filebench() {
  MLFS_PROFILE=1 run ${FILEB} -f $1
}
# $1 num $2 value_size
function db_bench() {
  run ${DB_BENCH} --db=/mlfs --num=$1 --value_size=$2 --max_file_size=1073741824 --write_buffer_size=1073741824
}
# $1 layout_score
function kernfs() {
  LD_PRELOAD=${JEMALLOC} MLFS_LAYOUT_SCORE=$1 LD_LIBRARY_PATH=${SRCROOT}/kernfs/build KERNFS=1 ${KERNFS} &
}
function ext() {
  ${UTILS}/ext.sh
}
function hash() {
  ${UTILS}/hash.sh
}

function run_db_bench() {
  for ((rep=1;rep<=1;rep++)) do
    for layout in 70; do #70 85 100; do
      for num in 4000000; do #1000000 3000000 5000000 7000000; do
        for vsize in 512; do
          for idx in ext hash; do
            ${idx}
            mkfs
            kernfs $layout
            PID=$!
            echo kernfs pid is $PID
            sleep 7
            db_bench $num $vsize > db_bench_${idx}_${layout}_${num}_${vsize}_${rep}
            kill -quit ${PID}
          done
        done
      done
    done
  done
}
function run_filebench() {
  for ((rep=1;rep<=1;rep++)) do
    for layout in 60 70 80 90 100; do
      for rtype in seq_4k rand_4k; do
        for idx in ext; do
          ${idx}
          NAME=${idx}_filebench_${rtype}_layout${layout}_${rep}
          mkfs
          kernfs $layout
          KPID=$!
          sleep 2
          filebench ${SRCROOT}/bench/filebench/filemicro_${rtype}.f | tee ${NAME}.stdout
          mv `grep -l sigusr /tmp/libfs_prof.*` $NAME.json
          rm /tmp/libfs_prof.*
          kill -quit ${KPID}
        done
      done
    done
  done
}
#run_db_bench
run_filebench
