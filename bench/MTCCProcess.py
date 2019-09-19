from argparse import ArgumentParser, Namespace
from datetime import datetime
import itertools
from IPython import embed
import json
import glob
import os
from pathlib import Path
from pprint import pprint 
import re
import shlex
import subprocess
from subprocess import DEVNULL, PIPE, STDOUT, TimeoutExpired
from multiprocessing import Process, Queue
import time 
from warnings import warn

import progressbar
from progressbar import ProgressBar

from Utils import *
from BenchRunner import BenchRunner

class MTCCRunner(BenchRunner):

    def __init__(self, args):
        super().__init__(args)
        self.update_bar_proc = None
        self.skip_insert = args.skip_insert

    def __del__(self):
        ''' Avoid having the asynchronous refresh thread become a zombie. '''
        super().__del__()
        if self.update_bar_proc is not None and self.update_bar_proc is None:
            self.update_bar_proc.terminate()
            self.update_bar_proc.join()
            assert self.update_bar_proc is not None


    def _parse_readfile_time(self, stdout):
        ''' Parse the 'elapsed time' field from the readfile output. '''
        lines = stdout.decode().splitlines()
        for line in lines:
            if 'elapsed time:' in line:
                fields = line.split(':')
                time = fields[1]
                return float(time)
    
        # Fall through, display the output that didn't have the elapsed time.
        pprint(lines)
        raise Exception('Could not find throughput numbers!')

    def _run_mtcc_trial(self, cwd, setup_args, trial_args, labels):
        self.env['MLFS_CACHE_PERF'] = '0'

        if setup_args:
            self.env['MLFS_PROFILE'] = '0'
            self._run_trial_continue(setup_args, cwd, None)
            self.env['MLFS_PROFILE'] = '1'

            total_time = self._run_trial_end(trial_args, cwd, self._parse_readfile_time)

        else:
            self.env['MLFS_PROFILE'] = '1'

            total_time = self._run_trial(trial_args, cwd, self._parse_readfile_time)

        # Get the stats.
        stat_obj = self._parse_trial_stat_files(total_time, labels)
        stat_obj['kernfs'] = self._get_kernfs_stats()

        if self.args.measure_cache_perf:
            self.env['MLFS_PROFILE'] = '1'
            self.env['MLFS_CACHE_PERF'] = '1'
            if setup_args:
                self._run_trial_continue(setup_args, cwd, None, timeout=(10*60))

                total_time = self._run_trial_end(
                    trial_args, cwd, self._parse_readfile_time, timeout=(10*60))

            else:

                total_time = self._run_trial(
                    trial_args, cwd, self._parse_readfile_time, timeout=(10*60))

            # Get the stats.
            cache_stat_obj = self._parse_trial_stat_files(total_time, labels)

            try:
                stat_obj['cache'] = cache_stat_obj['idx_cache']
                stat_obj['cache']['kernfs'] = self._get_kernfs_stats()['idx_cache']
            except:
                pprint(cache_stat_obj)
                pprint(self._get_kernfs_stats())
                raise

        return [stat_obj]


    def _run_mtcc(self):
        print('Running MTCC profiles.')
        mtcc_path = (self.root_path / 'libfs' / 'tests').resolve()
        assert mtcc_path.exists()
       
        workloads = self._get_workloads()

        old_stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        for old_file in old_stats_files:
            old_file.unlink()

        widgets = [
                    progressbar.Percentage(),
                    ' (', progressbar.Counter(), ' of {})'.format(len(workloads)),
                    ' ', progressbar.Bar(left='[', right=']'),
                    ' ', progressbar.Timer(),
                    ' ', progressbar.ETA(),
                  ]

        with ProgressBar(widgets=widgets, max_value=len(workloads)) as bar:
            bar.start()
            counter = 0

            def update_bar(shared_q):
                import time
                counter = 0
                while True:
                    time.sleep(0.5)
                    try:
                        counter = shared_q.get_nowait()
                    except:
                        pass
                    bar.update(counter)
            
            shared_q = Queue()
            self.update_bar_proc = Process(target=update_bar, args=(shared_q,))
            self.update_bar_proc.start()

            numa_node = self.args.numa_node
            dir_str   = str(mtcc_path)

            stat_objs = []
            current = ''
            prev_idx = None
            try:
                for workload in workloads:
                    try:

                        idx_struct, layout_score, start_size, io_size, reps, \
                                nfiles, trial_num = workload

                        self.env['MLFS_CACHE_PERF'] = '0'

                        if prev_idx is None or prev_idx != idx_struct:
                            assert self.kernfs is not None
                            self.kernfs.mkfs()

                        prev_idx = idx_struct

                        self.env['MLFS_IDX_STRUCT'] = idx_struct
                        self.env['MLFS_LAYOUT_SCORE'] = layout_score

                        labels = {}
                        labels['struct'] = idx_struct
                        labels['layout'] = layout_score
                        labels['start size'] = start_size
                        labels['io size'] = io_size
                        labels['repetitions'] = reps
                        labels['num files'] = nfiles
                        labels['trial num'] = trial_num

                        end_size = int(start_size + ((io_size * reps) / nfiles))

                        mtcc_insert_arg_str = \
                            f'''taskset -c 0
                                numactl -N {numa_node} -m {numa_node} {dir_str}/run.sh
                                {dir_str}/MTCC -b {io_size} -s 1 -j 1 -n {nfiles}
                                -S {start_size} -M {end_size} 
                                -w {io_size * reps} -r 0'''

                        setup_size = 1024 * 4096 if start_size > (1024 * 4096) else start_size

                        readtest_setup_arg_str = \
                            f'''taskset -c 0
                                numactl -N {numa_node} -m {numa_node} {dir_str}/run.sh
                                {dir_str}/MTCC -b {setup_size} -s 1 -j 1 -n {nfiles}
                                -M {start_size} -w {start_size} -r 0 -S 0'''

                        mtcc_seq_arg_str = \
                            f'''taskset -c 0
                                numactl -N {numa_node} -m {numa_node} {dir_str}/run.sh
                                {dir_str}/readfile -b {io_size} -s 1 -j 1 -n {nfiles}
                                -r {io_size * reps}'''

                        mtcc_rand_arg_str = \
                            f'''taskset -c 0
                                numactl -N {numa_node} -m {numa_node} {dir_str}/run.sh
                                {dir_str}/readfile -b {io_size} -s 0 -j 1 -n {nfiles}
                                -r {io_size * reps} -x'''

                        insert_trial_args = shlex.split(mtcc_insert_arg_str)

                        seq_setup_args = shlex.split(readtest_setup_arg_str)
                        seq_trial_args = shlex.split(mtcc_seq_arg_str)

                        rand_setup_args = seq_setup_args
                        rand_trial_args = shlex.split(mtcc_rand_arg_str)

                        # Run the benchmarks
                        # 1) Insert test
                        if not self.skip_insert:
                            current = 'Insert'
                            insert_labels = {}
                            insert_labels.update(labels)
                            insert_labels['test'] = 'Insert'
                            stat_objs += self._run_mtcc_trial(
                                mtcc_path, None, insert_trial_args, insert_labels)

                        # 2) Sequential read test
                        current = 'Sequential'
                        seq_labels = {}
                        seq_labels.update(labels)
                        seq_labels['test'] = 'Sequential Read'
                        stat_objs += self._run_mtcc_trial(
                            mtcc_path, seq_setup_args, seq_trial_args, seq_labels)

                        # 3) Random read test
                        current = 'Random'
                        rand_labels = {}
                        rand_labels.update(labels)
                        rand_labels['test'] = 'Random Read'
                        stat_objs += self._run_mtcc_trial(
                            mtcc_path, rand_setup_args, rand_trial_args, rand_labels)

                        counter += 1
                        shared_q.put(counter)

                    except:
                        print(current)
                        pprint(workload)
                        raise

            finally:
                # Output all the results
                timestamp_str = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
                fname = f'mtcc_{timestamp_str}.json'
                self._write_bench_output(stat_objs, fname)
                # also write a summarized version
                keys = ['layout', 'total_time', 'struct', 'test', 'io size',
                        'repetitions', 'num files', 'trial num', 'start size']

                ntotal = 3 * len(workloads) if not self.skip_insert else 2 * len(workloads)
                if len(stat_objs) != ntotal:
                    print(f'What? Should have {3 * len(workloads)}, only have {len(stat_objs)}!')

                stat_summary = []
                for stat_obj in stat_objs:
                    small_obj = {k: stat_obj[k] for k in keys if k in stat_obj}
                    stat_summary += [small_obj]
                sname = f'mtcc_summary_{timestamp_str}.json'
                self._write_bench_output(stat_summary, sname)

                # Shutdown async tasks
                self.update_bar_proc.terminate()
                self.update_bar_proc.join()
                assert self.update_bar_proc is not None


    def _get_workloads(self):
        io_sizes    = resolve_units(self.args.io_sizes)
        repetitions = self.args.repetitions
        start_sizes = resolve_units(self.args.start_sizes)
        idx_structs = self.args.data_structures
        layouts     = self.args.layout_scores
        ntrials     = self.args.trials
        nfiles      = self.args.num_files_per_test

        # The order here is important. We want idx_structs to be the external-most
        # variable, because when it changes we re-run mkfs, which we want to do
        # pretty infrequently.
        workloads = itertools.product(
            idx_structs, layouts, start_sizes, io_sizes,
            repetitions, nfiles, range(ntrials))

        return list(workloads)

    @classmethod
    def add_arguments(cls, parser):
        parser.set_defaults(fn=cls._run_mtcc)
        #parser.add_argument('thread_nums', nargs='+', type=int,
        #        help='Workloads of numbers of threads to run')
        #parser.add_argument('--sequential', '-s', action='store_true',
        #                    help='Run reads sequentially rather than randomly')

        # Requirements
        parser.add_argument('--io-sizes', '-i', nargs='+', type=str,
                            help='The data read/write units to use.')
        parser.add_argument('--repetitions', '-r', nargs='+', type=int,
                help=('The number of times to do reads/writes in an experiment.'
                      ' Used for locality tests'))

        parser.add_argument('--start-sizes', '-s', nargs='+', type=str,
                help=('The starting file sizes to use. Used to measure the '
                      'effects of indexing structure size.'))
        parser.add_argument('--num-files-per-test', '-f', nargs='+', type=int,
                help='The number of files per test.')

        # Options
        parser.add_argument('--measure-cache-perf', '-c', action='store_true',
                            help='Measure cache perf as well.')

        parser.add_argument('--skip-insert', action='store_true',
                            help='Skip the insert test')

        cls._add_common_arguments(parser)
