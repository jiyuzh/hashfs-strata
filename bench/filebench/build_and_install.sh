#! /bin/bash
source build.env
set -x
set -e
libtoolize
aclocal
autoheader
automake --add-missing
autoconf
./configure --prefix $(realpath install/)
make -j$(nproc)
make install -j$(nproc)
