#! /usr/bin/env /bin/bash

PATH=$PATH:.
SRC_ROOT=../../
LIBSPDK_DIR=`realpath ../../libfs/src/storage/spdk/`
GLIBC_DIR=`realpath ../../shim/glibc-build/lib`
if [[ ! -d $GLIBC_DIR ]]; then
		GLIBC_DIR=`realpath ../../shim/glibc-build`
fi
export LD_LIBRARY_PATH=$GLIBC_DIR:$SRC_ROOT/libfs/lib/nvml/src/nondebug/:$SRC_ROOT/libfs/build:/usr/local/lib/gcc/x86_64-unknown-linux-gnu/5.4.0/:/lib64:/usr/local/lib64:$LIBSPDK_DIR:/lib/x86_64-linux-gnu
echo $LD_LIBRARY_PATH
LD_PRELOAD=$SRC_ROOT/shim/libshim/libshim.so MLFS=1 MLFS_DEBUG=1 $@
#LD_PRELOAD=$SRC_ROOT/shim/libshim/libshim.so MLFS=1 MLFS_PROFILE=1 taskset -c 0,7 $@
#LD_PRELOAD=$SRC_ROOT/shim/libshim/libshim.so MLFS=1 taskset -c 0,7 $@
#LD_PRELOAD=$SRC_ROOT/shim/libshim/libshim.so:../../deps/mutrace/.libs/libmutrace.so MUTRACE_HASH_SIZE=2000000 MLFS=1 taskset -c 0,7 $@
#LD_PRELOAD=$SRC_ROOT/shim/libshim/libshim.so MLFS=1 $@
