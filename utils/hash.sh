#!/bin/sh
SRCDIR=$(realpath $(dirname ${0})/../)
ln -sfT hbuild ${SRCDIR}/libfs/build
ln -sfT hbuild ${SRCDIR}/kernfs/build
echo "Changed to hbuild"
