#!/usr/bin/env bash

set -euo pipefail

PERF_PATH="perf"
FLAME_DIR="/home/jz/FlameGraph"

RESULT_DIR="hashfs-micro/"

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

	bin="../../../WineFS/microbench/fstest"
	sudo "$PERF_PATH" record -F 20000 -a -g -o "$RESULT_DIR/$name.perf" -- env MLFS_IDX_STRUCT=HASHFS ./run.sh "$bin" -p /mlfs/ -S "${@}" |& tee "$RESULT_DIR/$name.log"
	sudo "$PERF_PATH" script -i "$RESULT_DIR/$name.perf" --kallsyms=/proc/kallsyms | "$FLAME_DIR/stackcollapse-perf.pl" | rg --no-config 'fstest|thread-pool' | rg --no-config -v ';__random' | perl -pe 's/(\[unknown\]\s*;)+/\[unknown\];/gm' > "$RESULT_DIR/$name.perf-folded"
	"$FLAME_DIR/flamegraph.pl" "$RESULT_DIR/$name.perf-folded" > "$RESULT_DIR/$name.svg"
}

function run_exp_512k
{
	name="$1"
	shift

	bin="../../../WineFS/microbench/fstest_512k"
	sudo "$PERF_PATH" record -F 20000 -a -g -o "$RESULT_DIR/$name.perf" -- env MLFS_IDX_STRUCT=HASHFS ./run.sh "$bin" -p /mlfs/ -S "${@}" |& tee "$RESULT_DIR/$name.log"
	sudo "$PERF_PATH" script -i "$RESULT_DIR/$name.perf" --kallsyms=/proc/kallsyms | "$FLAME_DIR/stackcollapse-perf.pl" | rg --no-config 'fstest|thread-pool' | rg --no-config -v ';__random' | perl -pe 's/(\[unknown\]\s*;)+/\[unknown\];/gm' > "$RESULT_DIR/$name.perf-folded"
	"$FLAME_DIR/flamegraph.pl" "$RESULT_DIR/$name.perf-folded" > "$RESULT_DIR/$name.svg"
}


factor=10

mkdir -p "$RESULT_DIR"

start_hashfs
run_exp "Append" -a -G "$factor"
stop_hashfs

start_hashfs
run_exp "SWE" -w -G $((factor / 10)) -n 10
stop_hashfs

start_hashfs
run_exp "SW" -w -G "$factor"
run_exp_512k "SR" -r -G "$factor"
stop_hashfs

start_hashfs
run_exp "RWprep" -R -w -G "$factor"
run_exp "RW" -R -w -G "$factor"
run_exp "RR" -R -r -G "$factor"
stop_hashfs

bell "done"
