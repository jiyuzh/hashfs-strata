#!/bin/sh
SRCDIR=$(realpath $(dirname ${0})/../)
ln -sfT ebuild ${SRCDIR}/libfs/build
ln -sfT ebuild ${SRCDIR}/kernfs/build
echo "Changed to ebuild"
