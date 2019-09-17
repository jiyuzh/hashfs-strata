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
    LAYOUT_SCORES = ['100', '90', '80', '70']

    def __init__(self, args):
        assert isinstance(args, Namespace)
        self.args = args
        if 'fn' not in self.args:
            raise Exception('No valid benchmark selected!')

        self.root_path = Path(__file__).resolve().parent.parent
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


    def __del__(self):
        if self.kernfs is not None and self.kernfs.is_running():
            warn('Running cleanup due to failure', UserWarning, stacklevel=2)
            self.kernfs.stop()

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
        finally:
            self._finish_trial()
        
        return None 

    def _run_trial_continue(self, bench_args, bench_cwd, processing_fn, timeout=(2*60),
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

    def _run_trial_end(self, bench_args, bench_cwd, processing_fn, timeout=(2*60),
            no_warn=False):

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
        finally:
            self._finish_trial()
        
        return None 

    def run(self):
        self.args.fn(self)

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
  
