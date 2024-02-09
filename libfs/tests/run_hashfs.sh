#!/usr/bin/env bash

set -euo pipefail

function stop_hashfs
{
	set +e

	sleep 5
	sudo killall -s 2 kernfs
	sleep 10
	sudo killall -s 9 kernfs

	set -e
}

function start_hashfs
{
	set +e
	sudo killall -s 9 kernfs
	set -e

	pushd "../../kernfs/tests"

	sudo MLFS_IDX_STRUCT=HASHFS ./mkfs.sh
	sudo MLFS_IDX_STRUCT=HASHFS ./run.sh kernfs &
	sleep 10

	popd
}

function run_exp
{
	name="$1"
	shift

	sudo MLFS_IDX_STRUCT=HASHFS ./run.sh ../../../WineFS/microbench/fstest -p /mlfs/ -S "${@}" |& tee "hashfs-micro/$name.log"
}

factor=10

# start_hashfs
# run_exp "Append" -a -G "$factor"
# stop_hashfs

# start_hashfs
# run_exp "SWE" -w -G $((factor / 10)) -n 10
# stop_hashfs

start_hashfs
run_exp "SW" -w -G "$factor"
run_exp "SR" -r -G "$factor"
stop_hashfs

start_hashfs
run_exp "RWprep" -R -w -G "$factor"
run_exp "RW" -R -w -G "$factor"
run_exp "RR" -R -r -G "$factor"
stop_hashfs

bell "done"
