#! /bin/bash
#set -x
PATH=$PATH:.
STRATA_ROOT=/home/iangneal/workspace/strata
#export LD_LIBRARY_PATH=../lib/nvml/src/nondebug/:../build:../../shim/glibc-build/:../lib/libspdk/libspdk/:/usr/local/lib64:../src/storage/spdk/:/usr/lib/:/usr/lib/x86_64-linux-gnu/:/lib/x86_64-linux-gnu/
export LD_LIBRARY_PATH=:${STRATA_ROOT}/libfs/build:${STRATA_ROOT}/shim/glibc-build-all/:${STRATA_ROOT}/libfs/lib/libspdk/libspdk/:/usr/local/lib64:${STRATA_ROOT}/libfs/src/storage/spdk/:/usr/lib/:/usr/lib/x86_64-linux-gnu/:/lib/x86_64-linux-gnu/

LD_PRELOAD=${STRATA_ROOT}/shim/libshim/libshim.so:${STRATA_ROOT}/libfs/lib/jemalloc-4.5.0/lib/libjemalloc.so.2 ${@}
#LD_PRELOAD=../../shim/libshim/libshim.so:../lib/jemalloc-4.5.0/lib/libjemalloc.so.2 MLFS_PROFILE=1 ${@}
