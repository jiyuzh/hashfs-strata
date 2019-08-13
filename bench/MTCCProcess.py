from argparse import ArgumentParser, Namespace
import copy
from datetime import datetime
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

    def _parse_trial_stat_files(self, workload_name, layout, struct, time_elapsed):
        stat_objs = []

        stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        assert len(stats_files)
        
        for stat_file in stats_files:
            with stat_file.open() as f:
                file_data = f.read()
                stats_arr = []
                stats_arr = json.loads(file_data)
                # data_objs = [ x.strip() for x in file_data.split(os.linesep) ]
                for obj in stats_arr:
                    # if len(data) < 2:
                    #     continue
                    if 'lsm' not in obj or 'nr' not in obj['lsm'] or obj['lsm']['nr'] <= 0:
                        continue
                    obj['bench'] = 'MTCC (readfile)'
                    obj['workload'] = workload_name
                    obj['layout'] = float(layout) / 100.0
                    obj['total_time'] = time_elapsed
                    obj['struct'] = struct.lower()
                    stat_objs += [obj]

        if len(stat_objs) != 1:
            pprint(stat_objs)
            print(workload_name, layout, struct, time_elapsed)
            raise Exception('No valid statistics from trial run!')

        for stat_file in stats_files:
            subprocess.run(shlex.split('rm -f {}'.format(
                str(stat_file))), check=True)

        return stat_objs[0]

    def _parse_trial_stat_files_cache(self, workload_name, layout, struct):
        stat_objs = []

        stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        assert len(stats_files)
        
        for stat_file in stats_files:
            with stat_file.open() as f:
                file_data = f.read()
                data_objs = [ x.strip() for x in file_data.split(os.linesep) ]
                for data in data_objs:
                    data = data.strip('\x00')
                    if len(data) < 2:
                        continue
                    obj = json.loads(data)
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

    def _run_mtcc(self):
        print('Running MTCC profiles.')
        mtcc_path = (self.root_path / 'libfs' / 'tests').resolve()
        assert mtcc_path.exists()
       
        workloads = self.args.thread_nums

        old_stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        for old_file in old_stats_files:
            rm_args = shlex.split('sudo rm -f {}'.format(str(old_file)))
            subprocess.run(rm_args, check=True)

        num_trials = len(workloads) * len(self.layout_scores) * \
                     len(self.structs) * self.args.trials
        widgets = [
                    progressbar.Percentage(),
                    ' (', progressbar.Counter(), ' of {})'.format(num_trials),
                    ' ', progressbar.Bar(left='[', right=']'),
                    ' ', progressbar.Timer(),
                    ' ', progressbar.ETA(),
                  ]

        with ProgressBar(widgets=widgets, max_value=num_trials) as bar:
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

            mtcc_arg_str = ('{0}/run.sh numactl -N 1 -m 1 {0}/MTCC -b 4k -s 0 '
                            '-j {1} -n {1} -M 1G -w 4k -r 0k')
            readfile_arg_str = ('{0}/run.sh numactl -N 1 -m 1 {0}/readfile '
            '-b 4k -s %d -j {1} -n {1} -r {1}G') % (1 if self.args.sequential else 0)

            try:
                for workload in workloads:
                    stat_objs = []
                    workload_name = '{}_threads'.format(workload)
                    for layout in self.layout_scores:
                        for struct in self.structs:
                            self.env['MLFS_IDX_STRUCT'] = struct
                            self.env['MLFS_LAYOUT_SCORE'] = layout
                            self.env['MLFS_CACHE_PERF'] = '0'

                            for trialno in range(self.args.trials):

                                setup_args = shlex.split(
                                        mtcc_arg_str.format(mtcc_path, workload))
                                readfile_args = shlex.split(
                                        readfile_arg_str.format(mtcc_path, workload))

                                self.env['MLFS_PROFILE'] = '0'
                                pr = lambda stdout: print(stdout.decode())
                                self._run_trial_continue(setup_args, mtcc_path, None)
                                self.env['MLFS_PROFILE'] = '1'

                                total_time = self._run_trial_end(readfile_args, 
                                        mtcc_path, self._parse_readfile_time)

                                # Get the stats.
                                stat_obj = self._parse_trial_stat_files(
                                        workload_name, layout, struct, total_time)

                                stat_obj['kernfs'] = self._get_kernfs_stats()

                                if self.args.measure_cache_perf:
                                    self.env['MLFS_PROFILE'] = '0'
                                    pprint(setup_args)
                                    self._run_trial_continue(setup_args, mtcc_path, None, timeout=(10*60))
                                    self.env['MLFS_PROFILE'] = '1'

                                    self.env['MLFS_CACHE_PERF'] = '1'
                                    pprint(readfile_args)
                                    self._run_trial_end(readfile_args, mtcc_path, pr, no_warn=True, timeout=(10*60))

                                    cache_stat_obj = self._parse_trial_stat_files_cache(
                                            workload_name, layout, struct)
                                    stat_obj['cache'] = cache_stat_obj['cache']
                                    stat_obj['cache']['kernfs'] = self._get_kernfs_stats()

                                stat_objs += [stat_obj]

                                counter += 1
                                shared_q.put(counter)

                    timestamp_str = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
                    fname = 'mtcc_{}_{}.json'.format(workload_name, timestamp_str)
                    self._write_bench_output(stat_objs, fname)
                    # also write a summarized version
                    keys = ['layout', 'total_time', 'struct', 'bench', 'workload', 'cache']
                    stat_summary = []
                    for stat_obj in stat_objs:
                        small_obj = { k: stat_obj[k] for k in keys if k in stat_obj}
                        stat_summary += [small_obj]
                    sname = 'mtcc_summary_{}_{}.json'.format(workload_name, 
                                                             timestamp_str)
                    self._write_bench_output(stat_summary, sname)
            except:
                raise
            finally:
                self.update_bar_proc.terminate()
                self.update_bar_proc.join()
                assert self.update_bar_proc is not None

   
    @classmethod
    def add_arguments(cls, parser):
        parser.set_defaults(fn=cls._run_mtcc)
        parser.add_argument('thread_nums', nargs='+', type=int,
                help='Workloads of numbers of threads to run')
        parser.add_argument('--measure-cache-perf', '-c', action='store_true',
                            help='Measure cache perf as well.')
        parser.add_argument('--sequential', '-s', action='store_true',
                            help='Run reads sequentially rather than randomly')
        cls._add_common_arguments(parser)
