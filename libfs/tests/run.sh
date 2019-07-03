#! /bin/bash
set -x
PATH=$PATH:.
#export LD_LIBRARY_PATH=../lib/nvml/src/nondebug/:../build:../../shim/glibc-build/:../lib/libspdk/libspdk/:/usr/local/lib64:../src/storage/spdk/:/usr/lib/:/usr/lib/x86_64-linux-gnu/:/lib/x86_64-linux-gnu/
export LD_LIBRARY_PATH=:../build:../../shim/glibc-build/:../lib/libspdk/libspdk/:/usr/local/lib64:../src/storage/spdk/:/usr/lib/:/usr/lib/x86_64-linux-gnu/:/lib/x86_64-linux-gnu/

LD_PRELOAD=../../shim/libshim/libshim.so:../lib/jemalloc-4.5.0/lib/libjemalloc.so.2:../src/storage/spdk/libspdk.so ${@}
#LD_PRELOAD=../../shim/libshim/libshim.so:../lib/jemalloc-4.5.0/lib/libjemalloc.so.2 MLFS_PROFILE=1 ${@}
