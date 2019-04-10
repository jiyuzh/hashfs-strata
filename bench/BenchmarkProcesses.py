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
import time
from warnings import warn

class KernFSThread:

    def __init__(self, root_path, env, gather_stats=False, verbose=False):
        'path argument must be to repo root directory.'
        assert isinstance(root_path, Path)
        assert isinstance(env, type(os.environ))
        assert isinstance(gather_stats, bool)
        self.gather_stats = gather_stats
        self.env          = env
        self.root         = root_path
        self.kernfs_path  = root_path / 'kernfs' / 'tests'
        self.proc         = None
        self.verbose      = verbose
        assert self.kernfs_path.exists()

    def __del__(self):
        if self.proc is not None and self.proc.returncode is None:
            self.stop()
        assert self.proc is None or self.proc.returncode is not None

    def start(self):
        'First we run mkfs, then start up the kernfs process.'
        rm_args = shlex.split('sudo umount /mlfs')
        subprocess.run(rm_args, check=True, stdout=DEVNULL, stderr=DEVNULL)

        mkdir_args = shlex.split('sudo mount -t tmpfs tmpfs /mlfs')
        subprocess.run(mkdir_args, check=True, stdout=DEVNULL, stderr=DEVNULL)

        mkfs_args = [ str(self.kernfs_path / 'mkfs.sh') ]
        proc = subprocess.run(mkfs_args, cwd=self.kernfs_path, check=True,
                              stdout=DEVNULL, stderr=DEVNULL)
        
        kernfs_args = shlex.split('{0}/run.sh {0}/kernfs'.format(
                                  str(self.kernfs_path)))

        opt_args = {}
        if not self.verbose:
            opt_args = { 'stdout': DEVNULL, 'stderr': DEVNULL }
        else:
            print('Running verbose KernFSThread.')
                       
        self.proc = subprocess.Popen(kernfs_args, cwd=self.kernfs_path,
                                     env=self.env, start_new_session=True,
                                     **opt_args)
        time.sleep(20)

    def stop(self):
        'Kill the kernfs process and potentially gather stats.'
        # kill whole process group
        pgid = os.getpgid(self.proc.pid)
        kill_args = [ 'sudo', 'kill', '-3', '--', '-'+str(pgid) ]
        subprocess.run(kill_args, check=True, stdout=DEVNULL, stderr=DEVNULL)
        self.proc.wait(timeout=10)
        assert self.proc.returncode is not None

    def is_running(self):
        return self.proc is not None and self.proc.returncode is None


class BenchRunner:

    IDX_STRUCTS   = [ 'EXTENT_TREES', 'GLOBAL_HASH_TABLE',
                      'LEVEL_HASH_TABLES', 'RADIX_TREES', 'NONE' ]
    LAYOUT_SCORES = ['100', '90', '80', '70', '60']

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


    def __del__(self):
        if self.kernfs is not None and self.kernfs.is_running():
            warn('Running cleanup due to failure', UserWarning, stacklevel=2)
            self.kernfs.stop()

    def _start_trial(self):
        if self.kernfs is not None and self.kernfs.is_running():
            warn('Starting new trial without proper completion of previous.',
                 UserWarning)
        self.kernfs = KernFSThread(self.root_path, self.env, 
                verbose=self.args.verbose)
        self.kernfs.start()

    def _finish_trial(self):
        assert self.kernfs is not None
        if not self.kernfs.is_running():
            warn('KernFS has already stopped before official end.',
                 UserWarning)
        else:
            self.kernfs.stop()

    def _kill_process(self, proc):
        ' Make sure to start this process with a new session. '
        pgid = os.getpgid(proc.pid)
        kill_args = [ 'sudo', 'kill', '-3', '--', '-'+str(pgid) ]
        subprocess.run(kill_args, check=True)
        proc.wait(timeout=10)


    def _write_bench_output(self, json_obj, name):
        if not self.outdir.exists():
            self.outdir.mkdir()
                
        filepath = self.outdir / name
        with filepath.open('w') as f:
            json.dump(json_obj, f, indent=4)

    def _run_trial(self, bench_args, bench_cwd, processing_fn, timeout=(2*60)):
        self._start_trial()

        proc = None
        try: 
            proc = subprocess.run(bench_args, 
                                  cwd=bench_cwd, env=self.env, 
                                  stdout=PIPE, stderr=STDOUT, 
                                  start_new_session=True,
                                  timeout=timeout,
                                  input=str.encode(2*os.linesep))

            return processing_fn(proc.stdout)
        except (TimeoutExpired, PermissionError) as e: 
            warn('Process "{}" hangs!'.format(' '.join(bench_args)),
                 UserWarning)
            self._kill_process(proc)
        finally:
            self._finish_trial()
        
        return None 

    def run(self):
        self.args.fn(self)

    @classmethod
    def _add_common_arguments(cls, parser):
        parser.add_argument('--data-structures', '-d', nargs='+', type=str,
                            default=cls.IDX_STRUCTS, 
                            help='List of structures to test with.')
        parser.add_argument('--layout-scores', '-l', nargs='+', type=str,
                            default=cls.LAYOUT_SCORES,
                            help='List of layout scores to use.')
        parser.add_argument('--trials', '-t', nargs='?', default=1, type=int,
                            help='Number of trials to run, default 1')
        parser.add_argument('--outdir', '-o', default='./benchout',
                            help='Where to output the results.')
        parser.add_argument('--verbose', '-v', action='store_true',
                            help='Output stdout/stderr of subprocesses.')
   
    @classmethod
    def add_arguments(cls, parser):
        subparsers = parser.add_subparsers()

        all_cmds = []
        filebench = subparsers.add_parser('filebench')
        filebench.set_defaults(fn=cls._run_filebench)
        filebench.add_argument('workloads', type=str, nargs='+',
                               help=('Workload filters on what workloads to '
                                     'run. Will run all that match.'))
        all_cmds += [filebench]

        leveldb = subparsers.add_parser('leveldb')
        leveldb.set_defaults(fn=cls._run_leveldb)
        leveldb.add_argument('--db_size', type=int, default=300000,
                             help='number of KV pairs in final DB.')
        leveldb.add_argument('values_sizes', type=int, nargs='+',
                             help='What value sizes to use')
        all_cmds += [leveldb]

        for sub in all_cmds:
            cls._add_common_arguments(sub)
