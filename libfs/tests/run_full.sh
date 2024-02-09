#!/usr/bin/env bash

set -euo pipefail

sudo killall -s 9 kernfs

pushd "../../kernfs/tests"

sudo MLFS_IDX_STRUCT=HASHFS ./mkfs.sh
sudo MLFS_IDX_STRUCT=HASHFS ./run.sh kernfs &
sleep 10

popd

sudo MLFS_IDX_STRUCT=HASHFS ./run.sh "${@}"

set -e

sleep 5
sudo killall -s 2 kernfs
sleep 10
sudo killall -s 9 kernfs
