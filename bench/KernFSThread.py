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

from Utils import *

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
        self._cleanup_kernfs()
        if self.proc is not None and self.proc.returncode is None:
            self.stop()
        assert self.proc is None or self.proc.returncode is not None


    def _clear_stats(self):
        'Reset the stats after init.'
        # kill whole process group
        pgid = os.getpgid(self.proc.pid)
        kill_args = shlex.split(f'kill -{signal.SIGUSR2.value} -- -{pgid}')
        subprocess.run(kill_args, check=True, stdout=DEVNULL, stderr=DEVNULL)


    def mkfs(self):
        ''' 
            Reset all the DAX devices. Only should be necessary when we change
            indexing structures. Avoid using this often since it's really slow.
        '''
        mkfs_args = shlex.split(f'numactl -N 0 -m 0 {str(self.kernfs_path / "mkfs.sh")}')
        proc = None
        if self.verbose:
            proc = subprocess.run(mkfs_args, cwd=self.kernfs_path, check=True,
                                  start_new_session=True, env=self.env)
        else:
            proc = subprocess.run(mkfs_args, cwd=self.kernfs_path, check=True,
                                  start_new_session=True, env=self.env,
                                  stdout=DEVNULL, stderr=DEVNULL)
        assert proc.returncode == 0

    def start(self):
        ''' 
            Start up KernFS without running mkfs. That has to be done separately. 
        '''

        # Make sure there are no stats files from prior runs.
        self._cleanup_kernfs()
       
        kernfs_arg_str = '{0}/run.sh taskset -c 0 numactl -N {1} -m {1} {0}/kernfs'.format(
                                  str(self.kernfs_path), '0')
        kernfs_args = shlex.split(kernfs_arg_str)

        opt_args = {}
        self.proc = None
        if not self.verbose:
            self.proc = subprocess.Popen(kernfs_args, cwd=self.kernfs_path,
                                         env=self.env, start_new_session=True,
                                         stdout=DEVNULL, stderr=DEVNULL)
        else:
            print('Running verbose KernFSThread.')
            self.proc = subprocess.Popen(kernfs_args, cwd=self.kernfs_path,
                                         env=self.env, start_new_session=True)

        # Wait for the kernfs PID file to appear.
        start_time = time.time()
        while (time.time() - start_time) < 5*60:
            pid_files = [Path(x) for x in glob.glob('/tmp/kernfs*.pid')]
            res = [find_pid_in_file(self.proc.pid, p) for p in pid_files]
            if True in res:
                break
        else:
            # 'else' occurs when the while condition is false, but not if the
            # loop is broken.
            raise Exception('Timed out waiting for kernfs!')

        self._clear_stats()


    def _parse_kernfs_stats(self):
        stat_objs = []

        stats_files = [Path(x) for x in glob.glob('/tmp/kernfs_prof.*')]
        assert stats_files
        
        for stat_file in stats_files:
            with stat_file.open() as f:
                file_data = f.read()
                stats_arr = []
                stats_arr = json.loads(file_data)
                # data_objs = [ x.strip() for x in file_data.split(os.linesep) ]
                for data in stats_arr:
                    # data = data.strip('\x00')
                    # if len(data) < 2:
                    #     continue
                    try:
                        stat_objs += [data]
                    except:
                        continue

        assert len(stat_objs) >= 1
        return stat_objs[-1]

    def _cleanup_kernfs(self):
        stats_files = [Path(x) for x in glob.glob('/tmp/kernfs_prof.*')]
        for stat_file in stats_files:
            stat_file.unlink()

    def stop(self):
        'Kill the kernfs process and potentially gather stats.'
        pgid = os.getpgid(self.proc.pid)
        kill_args = shlex.split(f'kill -{signal.SIGQUIT.value} -- -{str(pgid)}')
        subprocess.run(kill_args, check=True, stdout=DEVNULL, stderr=DEVNULL)
        self.proc.wait(timeout=10)
        assert self.proc.returncode is not None

        stats = None
        if self.gather_stats:
            stats = self._parse_kernfs_stats()
        
        self._cleanup_kernfs()
        return stats

    def is_running(self):
        return self.proc is not None and self.proc.returncode is None
