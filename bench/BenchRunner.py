from argparse import ArgumentParser, Namespace
import copy
from datetime import datetime
from IPython import embed
import json
import glob
import os
from pathlib import Path
from pprint import pprint
import psutil
import re
import signal
import shlex
import subprocess
from subprocess import DEVNULL, PIPE, STDOUT, TimeoutExpired
from multiprocessing import Process
import time
from warnings import warn

from KernFSThread import KernFSThread 
from AEPWatchThread import AEPWatchThread

class BenchRunner:

    IDX_STRUCTS   = [ 'EXTENT_TREES', 'EXTENT_TREES_TOP_CACHED', 
                      'GLOBAL_HASH_TABLE', 'GLOBAL_CUCKOO_HASH',
                      'GLOBAL_HASH_TABLE_COMPACT',
                      'GLOBAL_CUCKOO_HASH_COMPACT',
                      'LEVEL_HASH_TABLES', 'RADIX_TREES', 'NONE', 'HASHFS' ]
    IDX_DEFAULT   = [ 'EXTENT_TREES', 
                      'GLOBAL_HASH_TABLE', 'GLOBAL_CUCKOO_HASH',
                      'RADIX_TREES', 'NONE', 'LEVEL_HASH_TABLES' ]
    #LAYOUT_SCORES = ['100', '90', '80', '70', '60']
    LAYOUT_SCORES = ['100', '70']

    def __init__(self, args):
        assert isinstance(args, Namespace)
        self.args = args
        if 'fn' not in self.args:
            raise Exception('No valid benchmark selected!')

        self.root_path = Path(__file__).resolve().parent.parent
        self.libfs_tests_path = (self.root_path / 'libfs' / 'tests').resolve()
        assert self.libfs_tests_path.exists()

        self.kernfs = None
        self.outdir = Path(self.args.outdir)

        self.structs = []
        for struct in args.data_structures:
            struct = struct.upper()
            if struct not in self.__class__.IDX_STRUCTS:
                raise Exception('{} not a valid data structure!'.format(
                    struct))
            self.structs += [struct]
        
        self.layout_scores = args.layout_scores

        self.env = copy.deepcopy(os.environ)
        self.env['MLFS_PROFILE'] = '1'

        # The same instance is used, but the managed subprocess will change.
        self.kernfs = KernFSThread(self.root_path, self.env, 
                                   gather_stats=True,
                                   verbose=self.args.verbose)
        #self.aep = AEPWatchThread()


    def __del__(self):
        if self.kernfs is not None and self.kernfs.is_running():
            warn('Running cleanup due to failure', UserWarning, stacklevel=2)
            self.kernfs.stop()
        if hasattr(self, 'current_proc'):
            try:
                self._kill_process(self.current_proc)
            except:
                pass

    def _start_trial(self):
        if self.kernfs is not None and self.kernfs.is_running():
            warn('Starting new trial without proper completion of previous.',
                 UserWarning)
        self.kernfs.start()

    def _finish_trial(self):
        assert self.kernfs is not None
        if not self.kernfs.is_running():
            warn('KernFS has already stopped before official end.',
                 UserWarning)

        self.kernfs_stats = self.kernfs.stop()

    def _get_kernfs_stats(self):
        return self.kernfs_stats

    def _kill_process(self, proc):
        ' Make sure to start this process with a new session. '
        pgid = os.getpgid(proc.pid)
        kill_args = shlex.split(f'kill -{signal.SIGQUIT.value} -- -{str(pgid)}')
        subprocess.run(kill_args, check=True)
        proc.wait(timeout=10)

    def _write_bench_output(self, json_obj, name):
        if not self.outdir.exists():
            self.outdir.mkdir()
                
        filepath = self.outdir / name
        if json_obj:
            with filepath.open('w') as f:
                json.dump(json_obj, f, indent=4)

    def _run_trial(self, bench_args, bench_cwd, processing_fn, timeout=(2*60),
            no_warn=False):
        self._start_trial()

        proc = None
        try:
            proc = subprocess.Popen(bench_args, 
                                    cwd=bench_cwd, env=self.env, 
                                    stdout=PIPE, stderr=STDOUT,
                                    start_new_session=True)
            self.current_proc = proc

            stdout, stderr = proc.communicate(timeout=timeout)
            del self.current_proc

            if processing_fn is not None:
                return processing_fn(stdout)
            else:
                return None
        except (TimeoutExpired, PermissionError) as e: 
            if not no_warn:
                warn('Process "{}" hangs!'.format(' '.join(bench_args)),
                     UserWarning)
                print(e)
                pprint(self.env)
            self._kill_process(proc)
            embed()
            print("OUT")
            print(proc.stdout.read().decode())
            print("OUT")
            #proc.kill()
        finally:
            self._finish_trial()
        
        return None 

    def _run_trial_continue(self, bench_args, bench_cwd, processing_fn, timeout=(5*60),
            no_warn=False):
        self._start_trial()

        proc = None
        try:
            proc = subprocess.Popen(bench_args, 
                                    cwd=bench_cwd, env=self.env, 
                                    stdout=PIPE, stderr=STDOUT,
                                    start_new_session=True)

            stdout, stderr = proc.communicate(timeout=timeout)

            if processing_fn is not None:
                return processing_fn(stdout)
            else:
                return None
        except (TimeoutExpired, PermissionError) as e: 
            if not no_warn:
                warn('Process "{}" hangs!'.format(' '.join(bench_args)),
                     UserWarning)
                pprint(self.env)
            self._kill_process(proc)
            #proc.kill()
        
        return None 

    def _run_trial_passthrough(self, bench_args, bench_cwd, processing_fn, timeout=(5*60),
            no_warn=False):
        proc = None
        try:
            proc = subprocess.Popen(bench_args, 
                                    cwd=bench_cwd, env=self.env, 
                                    stdout=PIPE, stderr=STDOUT,
                                    start_new_session=True)
            self.current_proc = proc

            stdout, stderr = proc.communicate(timeout=timeout)
            del self.current_proc

            if processing_fn is not None:
                return processing_fn(stdout)
            else:
                return None
        except (TimeoutExpired, PermissionError) as e: 
            if not no_warn:
                warn('Process "{}" hangs!'.format(' '.join(bench_args)),
                     UserWarning)
                pprint(self.env)
            self._kill_process(proc)
            #proc.kill()
        
        return None 

    def _run_trial_end(self, bench_args, bench_cwd, processing_fn, timeout=(5*60),
            no_warn=False):

        proc = None
        try:
            proc = subprocess.Popen(bench_args, 
                                    cwd=bench_cwd, env=self.env, 
                                    stdout=PIPE, stderr=STDOUT,
                                    start_new_session=True)
            self.current_proc = proc

            stdout, stderr = proc.communicate(timeout=timeout)
            del self.current_proc

            if processing_fn is not None:
                return processing_fn(stdout)
            else:
                return None
        except (TimeoutExpired, PermissionError) as e: 
            if not no_warn:
                warn('Process "{}" hangs!'.format(' '.join(bench_args)),
                     UserWarning)
                pprint(self.env)
            self._kill_process(proc)
            #proc.kill()
        finally:
            self._finish_trial()
        
        return None 

    def _parse_trial_stat_files(self, time_elapsed, labels):
        stat_objs = []

        stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        assert stats_files

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
                    if 'lsm' not in obj or 'nr' not in obj['lsm']:
                        continue
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
            stat_file.unlink()

        return stat_objs[0]

    def run(self):
        self.args.fn(self)

    @staticmethod
    def get_standard_progress_bar(nitems):
        import progressbar
        widgets = [
                    progressbar.Percentage(),
                    ' (', progressbar.Counter(), ' of {})'.format(nitems),
                    ' ', progressbar.Bar(left='[', right=']'),
                    ' ', progressbar.Timer(),
                    ' ', progressbar.ETA(),
                  ]
        return widgets

    @staticmethod
    def remove_old_libfs_stats():
        import glob
        from pathlib import Path

        old_stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        for old_file in old_stats_files:
            old_file.unlink()

    @classmethod
    def _add_common_arguments(cls, parser):
        parser.add_argument('--data-structures', '-d', nargs='+', type=str,
                            default=cls.IDX_DEFAULT, 
                            help='List of structures to test with.')
        parser.add_argument('--layout-scores', '-l', nargs='+', type=str,
                            default=cls.LAYOUT_SCORES,
                            help='List of layout scores to use.')
        parser.add_argument('--trials', '-t', nargs='?', default=1, type=int,
                            help='Number of trials to run, default 1')

        parser.add_argument('--numa-node', '-M', default=0, type=int,
                            help='Which NUMA node to lock to.')

        parser.add_argument('--outdir', '-o', default='./benchout',
                            help='Where to output the results.')
        parser.add_argument('--verbose', '-v', action='store_true',
                            help='Output stdout/stderr of subprocesses.')
  
