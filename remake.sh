#! /bin/bash
set -e
#export OPT_ARGS="-g -O3"
export OPT_ARGS="-g -O2 -Werror"
export DEBUG_ARGS=""
for arg in $@; do
  if [ "$arg" == "mirror" ]; then
    echo "debug"
    OPT_ARGS="-g -O0"
    DEBUG_ARGS="${DEBUG_ARGS} -DMIRROR_SYSCALL"
  fi
  if [ "$arg" == "debug" ]; then
    echo "debug"
    OPT_ARGS="-g -O0 -Werror"
    DEBUG_ARGS="${DEBUG_ARGS}"
  fi
  if [ "$arg" == "verbose" ]; then
    echo "verbose"
    DEBUG_ARGS="${DEBUG_ARGS} -DSYS_TRACE"
  fi
done
function remake() {
    make -C $1 $2 clean
    make -C $1 $2 -j8
}
for i in ebuild hbuild; do
    remake libfs PREFIX=${i}
    remake kernfs PREFIX=${i}
done
remake shim/libshim
remake kernfs/tests
remake libfs/tests
##! /bin/bash
#set -e
#for prefix in ebuild hbuild; do
#  make -C libfs PREFIX=${prefix} -j clean
#  make -C libfs PREFIX=${prefix} -j
#  make -C kernfs PREFIX=${prefix} -j clean
#  make -C kernfs PREFIX=${prefix} -j
#done
#make -C kernfs/tests -j clean
#make -C kernfs/tests -j
#make -C libfs/tests -j clean
#make -C libfs/tests -j
#make -C shim/libshim -j clean
#make -C shim/libshim -j
