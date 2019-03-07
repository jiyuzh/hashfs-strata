#! /usr/bin/env python3

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

    def __init__(self, root_path, env, gather_stats=False):
        'path argument must be to repo root directory.'
        assert isinstance(root_path, Path)
        assert isinstance(env, type(os.environ))
        assert isinstance(gather_stats, bool)
        self.gather_stats = gather_stats
        self.env          = env
        self.root         = root_path
        self.kernfs_path  = root_path / 'kernfs' / 'tests'
        self.proc         = None
        assert self.kernfs_path.exists()

    def start(self):
        'First we run mkfs, then start up the kernfs process.'
        mkfs_args = [ str(self.kernfs_path / 'mkfs.sh') ]
        proc = subprocess.run(mkfs_args, cwd=self.kernfs_path, check=True,
                              stdout=DEVNULL, stderr=DEVNULL)
        
        kernfs_args = shlex.split('sudo -E {0}/run.sh {0}/kernfs'.format(
                                  str(self.kernfs_path)))
                       
        self.proc = subprocess.Popen(kernfs_args, cwd=self.kernfs_path,
                                     env=self.env, start_new_session=True)
        time.sleep(20)

    def stop(self):
        'Kill the kernfs process and potentially gather stats.'
        # kill whole process group
        pgid = os.getpgid(self.proc.pid)
        kill_args = [ 'sudo', 'kill', '-3', '--', '-'+str(pgid) ]
        subprocess.run(kill_args, check=True)
        self.proc.wait(timeout=10)

    def is_running(self):
        return self.proc is not None and self.proc.returncode is None


class BenchRunner:

    IDX_STRUCTS   = [ 'EXTENT_TREES', 'GLOBAL_HASH_TABLE',
                      'LEVEL_HASH_TABLES', 'NONE' ]
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
        self.kernfs = KernFSThread(self.root_path, self.env)
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

    def _parse_filebench_throughput(self, stdout):
        lines = stdout.decode().splitlines()
        for line in lines:
            if 'mb/s' in line:
                print(line)
                fields = line.split(' ')
                for field in fields:
                    if 'mb/s' in field:
                        return float(field.replace('mb/s', ''))
       
        pprint(lines)
        raise Exception('Could not find throughput numbers!')


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
            warn('Process "{}" hangs.!'.format(' '.join(bench_args)),
                 UserWarning)
            self._kill_process(proc)
        finally:
            print('Workload cleanup.')
            self._finish_trial()
        
        return None 

    def _run_filebench(self):
        print('Running FileBench microbenchmark.')
        filebench_path = (self.root_path / 'bench' / 'filebench').resolve()
        assert filebench_path.exists()
       
        workloads = []
        for pattern in self.args.workloads:
            pattern_path = str(filebench_path / pattern)
            matches = glob.glob(pattern_path, recursive=True)
            if len(matches) == 0:
                warn('Pattern "{}" resulted in no workload file matches'.format(
                        pattern), UserWarning)
            else:
                workloads += matches

        if len(workloads) == 0:
            raise Exception('No valid workloads found!')

        old_stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        for old_file in old_stats_files:
            subprocess.run(shlex.split('sudo rm -f {}'.format(str(old_file))), 
                                                              check=True)


        for workload in workloads:
            stat_objs = []
            workload_name = Path(workload).name.split('.')[0]
            for layout in self.layout_scores:
                for struct in self.structs:
                    self.env['MLFS_IDX_STRUCT'] = struct
                    self.env['MLFS_LAYOUT_SCORE'] = layout

                    for trialno in range(self.args.trials):
                        print('Running workload "{}" with layout "{}", trial #{}.'.format(
                            workload_name, layout, trialno + 1))

                        filebench_args = shlex.split(
                            'sudo -E {0}/run.sh {0}/filebench -f {1}'.format(
                                str(filebench_path), workload))

                        mb_s = self._run_trial(filebench_args, filebench_path,
                                               self._parse_filebench_throughput)

                        if mb_s is None or not mb_s:
                            continue

                        # Get the stats.
                        stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
                        assert len(stats_files)
                        
                        for stat_file in stats_files:
                            with stat_file.open() as f:
                                file_data = f.read()
                                data_objs = [ x.strip() for x in file_data.split(os.linesep) ]
                                for data in data_objs:
                                    data = data.strip('\x00')
                                    if 'master' in data or 'shutdown' in data:
                                        continue
                                    if len(data) < 2:
                                        warn('Stat file {} is empty.'.format(
                                            stat_file.name), UserWarning)
                                        continue
                                    obj = json.loads(data)
                                    obj['bench'] = 'filebench'
                                    obj['workload'] = workload_name
                                    obj['layout'] = float(layout) / 100.0
                                    obj['throughput'] = mb_s 
                                    obj['struct'] = struct.lower()
                                    stat_objs += [obj]

                        for stat_file in stats_files:
                            subprocess.run(shlex.split('sudo rm -f {}'.format(
                                str(stat_file))), check=True)

            timestamp_str = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
            fname = 'filebench_{}_{}.json'.format(workload_name, timestamp_str)
            self._write_bench_output(stat_objs, fname)
            # also write a summarized version
            keys = ['layout', 'throughput', 'struct', 'bench', 'workload']
            stat_summary = []
            for stat_obj in stat_objs:
                small_obj = { k: stat_obj[k] for k in keys }
                stat_summary += [small_obj]
            sname = 'filebench_summary_{}_{}.json'.format(workload_name, 
                                                          timestamp_str)
            self._write_bench_output(stat_summary, sname)

    def _parse_leveldb_output(self, stdout):
        lines = stdout.decode().splitlines()
        stats = {}

        stat_format = r'^(\w+)\s+:\s+(\S+)\s+micros\/op\;'
        stat_re = re.compile(stat_format)
        for line in lines:
            matches = stat_re.search(line)
            if matches is not None:
                stats[matches.group(1)] = matches.group(2)
            elif 'readrand' in line:
                import IPython
                IPython.embed()

        return stats if len(stats) > 0 else None

    def _run_leveldb(self):
        print('Running LevelDB benchmark.')
        # disable stats files for LevelDB, since they aren't useful.
        self.env['MLFS_PROFILE'] = '0'
        leveldb_path = (self.root_path / 'bench' / 'leveldb'/'build').resolve()
        assert leveldb_path.exists()

        old_stats_files = [Path(x) for x in glob.glob('/tmp/libfs_prof.*')]
        for old_file in old_stats_files:
            subprocess.run(shlex.split('sudo rm -f {}'.format(str(old_file))), 
                                                              check=True)
        for value_size in self.args.values_sizes:
            stat_objs = []
            for layout in self.__class__.LAYOUT_SCORES:
                for struct in self.structs:
                    self.env['MLFS_IDX_STRUCT'] = struct
                    self.env['MLFS_LAYOUT_SCORE'] = layout

                    for trialno in range(self.args.trials):
                        print('Running with value size of {} bytes, trial #{}.'.format(
                              value_size, trialno + 1))

                        cmd_fmt = ('sudo -E numactl -N 1 -m 1 {0}/run.sh '
                                   '{0}/db_bench --db=/mlfs --num={1} --value_size={2}')
                        cmd_str = cmd_fmt.format(str(leveldb_path), 
                                      self.args.db_size, value_size)
                        leveldb_args = shlex.split(cmd_str)

                        stats = self._run_trial(leveldb_args, leveldb_path,
                                                self._parse_leveldb_output)

                        if stats is None or not stats:
                            continue

                        stats['bench'] = 'filebench'
                        stats['workload'] = 'value_size_{}'.format(value_size)
                        stats['layout'] = float(layout) / 100.0
                        stats['struct'] = struct.lower()
                        stat_objs += [stats]

            timestamp_str = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
            fname = 'leveldb_{}_{}.json'.format(value_size, timestamp_str)
            self._write_bench_output(stat_objs, fname)


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

################################################################################
# Main execution.
################################################################################

def main():
    parser = ArgumentParser(description='Automate running benchmarks.')
    BenchRunner.add_arguments(parser)

    args = parser.parse_args()

    runner = BenchRunner(args)
    runner.run()


if __name__ == '__main__':
    main()
