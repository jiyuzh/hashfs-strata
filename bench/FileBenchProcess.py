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

from BenchRunner import BenchRunner

class FileBenchRunner(BenchRunner):

    def __init__(self, args):
        super().__init__(args)
        self.update_bar_proc = None

    def __del__(self):
        super().__del__()
        if self.update_bar_proc is not None and self.update_bar_proc is None:
            self.update_bar_proc.terminate()
            self.update_bar_proc.join()
            assert self.update_bar_proc is not None

    def _parse_filebench_throughput(self, stdout):
        lines = stdout.decode().splitlines()
        for line in lines:
            if 'mb/s' in line:
                fields = line.split(' ')
                for field in fields:
                    if 'mb/s' in field:
                        return float(field.replace('mb/s', ''))
      
        pprint(lines)
        raise Exception('Could not find throughput numbers!')

    def _parse_workloads(self, filebench_path):
        workloads = []
        for pattern in self.args.workloads:
            pattern_path = str(filebench_path / pattern)
            print(pattern_path)
            matches = glob.glob(pattern_path, recursive=True)
            if len(matches) == 0:
                warn('Pattern "{}" resulted in no workload file matches'.format(
                        pattern), UserWarning)
            else:
                workloads += matches

        if len(workloads) == 0:
            raise Exception('No valid workloads found!')
        
        return workloads

    def _parse_trial_stat_files(self, workload_name, layout, struct, throughput):
        stat_objs = []

        stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        assert len(stats_files)
        
        for stat_file in stats_files:
            with stat_file.open() as f:
                file_data = f.read()
                if not file_data:
                    continue
                stats_arr = []
                try:
                    stats_arr = json.loads(file_data)
                    assert isinstance(stats_arr, list)
                except json.decoder.JSONDecodeError as e:
                    print(e)
                    print(f'Could not decode {str(stat_file)} ({f.read()})!')
                    raise

                for obj in stats_arr:
                    print(f'STAT OBJ: {stat_file}')
                    # data = data.strip('\x00')
                    if 'master' in obj or 'shutdown' in obj:
                        continue
                    # if len(data) < 2:
                    #     continue
                    obj['bench'] = 'filebench'
                    obj['workload'] = workload_name
                    obj['layout'] = float(layout) / 100.0
                    obj['throughput'] = throughput
                    obj['struct'] = struct.lower()
                    stat_objs += [obj]

        for stat_file in stats_files:
            subprocess.run(shlex.split('rm -f {}'.format(
                str(stat_file))), check=True)

        if len(stat_objs) > 1:
            warn(f'{len(stat_objs)} instead of just one!')
            for so in stat_objs:
                print(f'{so}')

        if not stat_objs:
            raise Exception(f'len(stat_objs) == {len(stat_objs)}')

        # assert len(stat_objs) >= 1, f'len(stat_objs) == {len(stat_objs)}'
        return stat_objs[0]

    def _parse_trial_stat_files_cache(self, workload_name, layout, struct):
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
                    # data = data.strip('\x00')
                    if 'master' in data or 'shutdown' in data:
                        continue
                    # if len(data) < 2:
                    #     continue
                    obj['bench'] = 'filebench'
                    obj['workload'] = workload_name
                    obj['layout'] = float(layout) / 100.0
                    obj['struct'] = struct.lower()
                    stat_objs += [obj]

        for stat_file in stats_files:
            subprocess.run(shlex.split('rm -f {}'.format(
                str(stat_file))), check=True)

        assert len(stat_objs) == 1
        return stat_objs[0]

    def _run_filebench(self):
        print('Running FileBench profiles.')
        filebench_path = (self.root_path / 'bench' / 'filebench').resolve()
        assert filebench_path.exists()
       
        workloads = self._parse_workloads(filebench_path)

        old_stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        for old_file in old_stats_files:
            rm_args = shlex.split('rm -f {}'.format(str(old_file)))
            subprocess.run(rm_args, check=True)

        num_trials = len(workloads) * len(self.layout_scores) * \
                     len(self.structs) * self.args.trials

        numa_node = self.args.numa_node
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

            try:
                for workload in workloads:
                    stat_objs = []
                    workload_name = Path(workload).name.split('.')[0]
                    for layout in self.layout_scores:
                        for struct in self.structs:
                            self.env['MLFS_IDX_STRUCT'] = struct
                            self.env['MLFS_LAYOUT_SCORE'] = layout
                            self.env['MLFS_CACHE_PERF'] = '0'
                            self.env['MLFS_PROFILE'] = '1'

                            if struct == 'LEVEL_HASH_TABLES' and layout != '100':
                                warn(f'{struct} breaks with layout {layout}, swap!')
                                self.env['MLFS_LAYOUT_SCORE'] = '100'

                            for trialno in range(self.args.trials):
                                argstr = 'numactl -N {2} -m {2} {0}/run.sh {0}/filebench.mlfs -f {1}'.format(
                                            str(filebench_path), workload, numa_node)
                                filebench_args = shlex.split(argstr)

                                ntries = 0
                                mb_s = None
                                stat_obj = None
                                while True:
                                    try:
                                        self.remove_old_libfs_stats()
                                        self.kernfs.mkfs()
                                        for shm in [Path(x) for x in glob.glob('/tmp/filebench-shm-*')]:
                                            shm.unlink()

                                        mb_s = self._run_trial(filebench_args, filebench_path,
                                                            self._parse_filebench_throughput,
                                                            timeout=(2*60), try_parse=True)

                                        # if mb_s is None or not mb_s:
                                        #     warn('Could not parse filebench results!',
                                        #             UserWarning)
                                        #     continue

                                        # Get the stats.
                                        stat_obj = self._parse_trial_stat_files(
                                                workload_name, layout, struct, mb_s)

                                        stat_obj['kernfs'] = self._get_kernfs_stats()
                                        stat_obj['trial num'] = trialno

                                        break
                                    except Exception as e:
                                        print(e)
                                        ntries += 1
                                        if ntries > 2:
                                            raise e
                                        else:
                                            print(f'RETRY #{ntries}')

                                if self.args.measure_cache_perf:
                                    self.env['MLFS_CACHE_PERF'] = '1'
                                    mb_s_cache = self._run_trial(filebench_args, filebench_path,
                                                           self._parse_filebench_throughput, timeout=(10*60))

                                    if mb_s_cache is None or not mb_s_cache:
                                        warn('Could not parse filebench results!',
                                                UserWarning)
                                        continue

                                    cache_stat_obj = self._parse_trial_stat_files_cache(
                                            workload_name, layout, struct)
                                    stat_obj['cache'] = cache_stat_obj['cache']
                                    stat_obj['cache']['kernfs'] = self._get_kernfs_stats()

                                stat_objs += [stat_obj]

                                counter += 1
                                shared_q.put(counter)

                    timestamp_str = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
                    fname = 'filebench_{}_{}.json'.format(workload_name, timestamp_str)
                    self._write_bench_output(stat_objs, fname)
                    # also write a summarized version
                    keys = ['layout', 'throughput', 'struct', 'bench', 'workload', 'cache']
                    stat_summary = []
                    for stat_obj in stat_objs:
                        small_obj = { k: stat_obj[k] for k in keys if k in stat_obj}
                        stat_summary += [small_obj]
                    sname = 'filebench_summary_{}_{}.json'.format(workload_name, 
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
        parser.set_defaults(fn=cls._run_filebench)
        parser.add_argument('workloads', type=str, nargs='+',
                            help=('Workload filters on what workloads to '
                                  'run. Will run all that match.'))
        parser.add_argument('--measure-cache-perf', '-c', action='store_true',
                            help='Measure cache perf as well.')
        cls._add_common_arguments(parser)
