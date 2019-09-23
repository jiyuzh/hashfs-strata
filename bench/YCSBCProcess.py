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
from AEPWatchThread import AEPWatchThread

class YCSBCRunner(BenchRunner):

    def __init__(self, args):
        super().__init__(args)
        self.update_bar_proc = None
        self.bench_path = (self.root_path / 'bench').resolve()
        self.dbfilename = "/mlfs/db"
        assert self.bench_path.exists()

    def __del__(self):
        ''' Avoid having the asynchronous refresh thread become a zombie. '''
        super().__del__()
        if self.update_bar_proc is not None and self.update_bar_proc is None:
            self.update_bar_proc.terminate()
            self.update_bar_proc.join()
            assert self.update_bar_proc is not None


    def _parse_ycsbc_KTPS(self, stdout):
        ''' Parse the 'elapsed time' field from the readfile output. '''
        lines = stdout.decode().splitlines()
        labels = {}
        for line in lines:
            cols = line.split()
            if 'leveldb' in line:
                labels['KTPS'] = cols[-1]
            elif line.startswith("READ "):
                labels['READ'] = {'cnt': cols[1], 'cycles': cols[2]}
            elif line.startswith("UPDATE "):
                labels['UPDATE'] = {'cnt': cols[1], 'cycles': cols[2]}
        if len(labels) == 0:
            # Fall through, display the output that didn't have the elapsed time.
            pprint(lines)
            raise Exception('Could not find throughput numbers!')
        else:
            return labels


    def _run_ycsbc_trial(self, cwd, setup_args, trial_args, labels):
        assert (setup_args is not None)

        self.env['MLFS_PROFILE'] = '1'
        self.env['MLFS_CACHE_PERF'] = '0'
        
        self._run_trial_continue(setup_args, cwd, None, timeout=(10*60))

        stdout_labels = self._run_trial_end(trial_args, cwd, 
                self._parse_ycsbc_KTPS, timeout=(10*60))

        # Get the stats.
        labels.update(stdout_labels)
        stat_obj = self._parse_trial_stat_files('N/A', labels)
        stat_obj['kernfs'] = self._get_kernfs_stats()

        if self.args.measure_cache_perf:
            self._run_trial_continue(setup_args, cwd, None, timeout=(10*60))

            self.aep.start()
            stdout_labels = self._run_trial_end(
                trial_args, cwd, self._parse_ycsbc_KTPS, timeout=(10*60))
            aep_stats = self.aep.stop()

            # Get the stats.
            labels.update(stdout_labels)
            cache_stat_obj = self._parse_trial_stat_files('N/A', labels)

            try:
                #stat_obj['cache'] = cache_stat_obj['idx_cache']
                #stat_obj['cache']['kernfs'] = self._get_kernfs_stats()['idx_cache']
                stat_obj['cache'] = aep_stats
            except:
                pprint(cache_stat_obj)
                pprint(self._get_kernfs_stats())
                raise

        return [stat_obj]

    def _run_yscbc_workload(self, workload):
        idx_struct, layout_score, ycsb_workload, trial_num = workload

        numa_node = self.args.numa_node
        dir_str   = str(self.bench_path)
        libfs_tests_str = str(self.libfs_tests_path)

        self.env['MLFS_CACHE_PERF'] = '0'

        self.env['MLFS_IDX_STRUCT'] = idx_struct
        self.env['MLFS_LAYOUT_SCORE'] = layout_score

        labels = {}
        labels['struct'] = idx_struct
        labels['layout'] = layout_score
        labels['ycsb_workload'] = ycsb_workload
        labels['trial num'] = trial_num

        rmrf_setup_str = \
            f'''taskset -c 0 numactl -N {numa_node} -m {numa_node}
                {libfs_tests_str}/run.sh {libfs_tests_str}/rmrf {self.dbfilename}
            '''

        ycsbc_arg_str = \
            f'''taskset -c 0
                numactl -N {numa_node} -m {numa_node} {dir_str}/run.sh
                {dir_str}/YCSB-C/ycsbc -db leveldb
                -dbfilename {self.dbfilename}
                -P {dir_str}/YCSB-C/workloads/{ycsb_workload}'''

        rmrf_setup_args = shlex.split(rmrf_setup_str)
        ycsbc_trial_args = shlex.split(ycsbc_arg_str)

        self.remove_old_libfs_stats()

        # Run the benchmarks
        return self._run_ycsbc_trial(
            self.bench_path, rmrf_setup_args, ycsbc_trial_args, labels)

    def _run_ycsbc(self):
        print('Running YCSB-C profiles.')
       
        workloads = self._get_workloads()

        self.remove_old_libfs_stats()

        widgets = self.get_standard_progress_bar(len(workloads))

        with ProgressBar(widgets=widgets, max_value=len(workloads)) as bar:
            bar.start()
            counter = 0

            def update_bar(shared_q):
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

            stat_objs = []
            prev_idx = None
            try:
                for workload in workloads:
                    try:
                        idx_struct = workload[0]

                        self.env['MLFS_CACHE_PERF'] = '0'
                        self.env['MLFS_IDX_STRUCT'] = idx_struct

                        if self.args.always_mkfs or \
                                prev_idx is None or prev_idx != idx_struct:
                            assert self.kernfs is not None
                            self.kernfs.mkfs()

                        prev_idx = idx_struct

                        stat_objs += self._run_yscbc_workload(workload)

                        counter += 1
                        shared_q.put(counter)

                    except:
                        pprint(workload)
                        raise

            finally:
                # Output all the results
                timestamp_str = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
                fname = f'ycsbc_{timestamp_str}.json'
                self._write_bench_output(stat_objs, fname)
                # also write a summarized version
                keys = ['layout', 'total_time', 'struct', 'test', 'io size',
                        'repetitions', 'num files', 'trial num', 'start size']

#                if len(stat_objs) != 3 * len(workloads):
#                    import pdb
#                    pdb.set_trace()
#                    print(f'What? Should have {3 * len(workloads)}, only have {len(stat_objs)}!')

                stat_summary = []
                for stat_obj in stat_objs:
                    small_obj = {k: stat_obj[k] for k in keys if k in stat_obj}
                    stat_summary += [small_obj]
                sname = f'ycsbc_summary_{timestamp_str}.json'
                self._write_bench_output(stat_summary, sname)

                # Shutdown async tasks
                self.update_bar_proc.terminate()
                self.update_bar_proc.join()
                assert self.update_bar_proc is not None


    def _get_workloads(self):
        idx_structs   = self.args.data_structures
        layouts       = self.args.layout_scores
        ntrials       = self.args.trials
        ycsb_workload = self.args.ycsb_workload

        # The order here is important. We want idx_structs to be the external-most
        # variable, because when it changes we re-run mkfs, which we want to do
        # pretty infrequently.
        workloads = itertools.product(
            idx_structs, layouts, ycsb_workload, range(ntrials))

        return list(workloads)

    @classmethod
    def add_arguments(cls, parser):
        parser.set_defaults(fn=cls._run_ycsbc)
        #parser.add_argument('thread_nums', nargs='+', type=int,
        #        help='Workloads of numbers of threads to run')
        #parser.add_argument('--sequential', '-s', action='store_true',
        #                    help='Run reads sequentially rather than randomly')

        # Requirements
        parser.add_argument('--ycsb-workload', '-P', nargs='+', type=str,
                            default=[
                                'workloada.strata.spec',
                                'workloadb.strata.spec',
                                'workloadc.strata.spec',
                                'workloadd.strata.spec',
                                'workloade.strata.spec',
                                'workloadf.strata.spec',
                                ],
                            help='The workload name to use')

        # Options
        parser.add_argument('--measure-cache-perf', '-c', action='store_true',
                            help='Measure cache perf as well.')
        parser.add_argument('--always-mkfs', action='store_true',
                            help='Always rerun mkfs between trials.')

        cls._add_common_arguments(parser)
