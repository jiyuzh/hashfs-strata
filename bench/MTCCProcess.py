from argparse import ArgumentParser, Namespace
import copy
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
from BenchmarkProcesses import BenchRunner

class MTCCRunner(BenchRunner):

    def __init__(self, args):
        super().__init__(args)
        self.update_bar_proc = None

    def __del__(self):
        super().__del__()
        if self.update_bar_proc is not None and self.update_bar_proc is None:
            self.update_bar_proc.terminate()
            self.update_bar_proc.join()
            assert self.update_bar_proc is not None

    def _parse_readfile_time(self, stdout):
        lines = stdout.decode().splitlines()
        for line in lines:
            if 'elapsed time:' in line:
                fields = line.split(':')
                time = fields[1]
                return float(time)
     
        pprint(lines)
        raise Exception('Could not find throughput numbers!')

    def _parse_trial_stat_files(self, time_elapsed, labels):
        stat_objs = []

        stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        assert stats_files
        
        for stat_file in stats_files:
            with stat_file.open() as f:
                file_data = f.read()
                stats_arr = []
                stats_arr = json.loads(file_data)
                # data_objs = [ x.strip() for x in file_data.split(os.linesep) ]
                for obj in stats_arr:
                    # if len(data) < 2:
                    #     continue
                    if 'lsm' not in obj or 'nr' not in obj['lsm']:
                        continue
                    obj['bench'] = 'MTCC (readfile)'
                    obj['total_time'] = time_elapsed
                    obj.update(labels)
                    stat_objs += [obj]

        if len(stat_objs) > 1:
            stat_objs = [s for s in stat_objs if s['lsm']['nr'] > 0]

        if not stat_objs:
            pprint(stat_objs)
            print(time_elapsed)
            pprint(labels)
            raise Exception('No valid statistics from trial run!')

        for stat_file in stats_files:
            subprocess.run(shlex.split('rm -f {}'.format(
                str(stat_file))), check=True)

        return stat_objs[0]

    def _parse_trial_stat_files_cache(self, workload_name, layout, struct):
        stat_objs = []

        stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        assert stats_files
   
        for stat_file in stats_files:
            with stat_file.open() as f:
                file_data = f.read()
                stats_arr = []
                stats_arr = json.loads(file_data)
                # data_objs = [ x.strip() for x in file_data.split(os.linesep) ]
                for obj in stats_arr:
                    # data = data.strip('\x00')
                    # if len(data) < 2:
                    #     continue
                    if 'lsm' not in obj or 'nr' not in obj['lsm'] or obj['lsm']['nr'] <= 0:
                        continue
                    obj['bench'] = 'MTCC (readfile)'
                    obj['workload'] = workload_name
                    obj['layout'] = float(layout) / 100.0
                    obj['struct'] = struct.lower()
                    stat_objs += [obj]

        for stat_file in stats_files:
            subprocess.run(shlex.split('sudo rm -f {}'.format(
                str(stat_file))), check=True)

        pprint(stat_objs)
        assert len(stat_objs) == 1
        return stat_objs[0]


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
            rm_args = shlex.split('sudo rm -f {}'.format(str(old_file)))
            subprocess.run(rm_args, check=True)

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
            try:
                for workload in workloads:
                    try:

                        idx_struct, layout_score, start_size, io_size, reps, \
                                nfiles, trial_num = workload

                        self.env['MLFS_IDX_STRUCT'] = idx_struct
                        self.env['MLFS_LAYOUT_SCORE'] = layout_score
                        self.env['MLFS_CACHE_PERF'] = '0'

                        labels = {}
                        labels['struct'] = idx_struct
                        labels['layout'] = layout_score
                        labels['start size'] = start_size
                        labels['io size'] = io_size
                        labels['repetitions'] = reps
                        labels['num files'] = nfiles
                        labels['trial num'] = trial_num

                        mtcc_insert_arg_str = \
                            f'''taskset -c 0
                                numactl -N {numa_node} -m {numa_node} {dir_str}/run.sh
                                {dir_str}/MTCC -b {io_size} -s 1 -j 1 -n {nfiles}
                                -S {start_size} -M {start_size + (io_size * reps)} 
                                -w {io_size * reps} -r 0'''

                        setup_size = 1024 * 4096 if start_size > (1024 * 4096) else start_size

                        readtest_setup_arg_str = \
                            f'''taskset -c 0
                                numactl -N {numa_node} -m {numa_node} {dir_str}/run.sh
                                {dir_str}/MTCC -b {setup_size} -s 1 -j 1 -n {nfiles}
                                -M {start_size} -w {setup_size} -r 0'''

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

                if len(stat_objs) != 3 * len(workloads):
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

        cls._add_common_arguments(parser)
