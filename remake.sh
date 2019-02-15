#! /bin/bash
set -e
for prefix in ebuild hbuild; do
  make -C libfs PREFIX=${prefix} -j clean
  make -C libfs PREFIX=${prefix} -j
  make -C kernfs PREFIX=${prefix} -j clean
  make -C kernfs PREFIX=${prefix} -j
done
make -C kernfs/tests -j clean
make -C kernfs/tests -j
make -C libfs/tests -j clean
make -C libfs/tests -j
make -C shim/libshim -j clean
make -C shim/libshim -j
