#! /bin/bash

PATH=$PATH:.
SRC_ROOT=../../
export LD_LIBRARY_PATH=$SRC_ROOT/libfs/lib/nvml/src/nondebug/:$SRC_ROOT/libfs/build:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.4.0/:/lib64:/usr/local/lib64
LD_PRELOAD=$SRC_ROOT/shim/libshim/libshim.so MLFS=1 MLFS_DEBUG=1 $@
#LD_PRELOAD=$SRC_ROOT/shim/libshim/libshim.so MLFS=1 MLFS_PROFILE=1 taskset -c 0,7 $@
#LD_PRELOAD=$SRC_ROOT/shim/libshim/libshim.so MLFS=1 taskset -c 0,7 $@
#LD_PRELOAD=$SRC_ROOT/shim/libshim/libshim.so:../../deps/mutrace/.libs/libmutrace.so MUTRACE_HASH_SIZE=2000000 MLFS=1 taskset -c 0,7 $@
#LD_PRELOAD=$SRC_ROOT/shim/libshim/libshim.so MLFS=1 $@
