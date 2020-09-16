#! /bin/bash
source build.env
set -e
libtoolize
aclocal
autoheader
automake --add-missing
autoconf
./configure --prefix=$(realpath ./install/)
make clean
make -j$(nproc)
make install -j$(nproc)
