#!/usr/bin/env bash

set -euo pipefail

mkdir -p build
pushd build

cmake ..
make -j`nproc`

popd
