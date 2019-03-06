#! /usr/bin/env python3

from argparse import ArgumentParser, Namespace
import copy
from datetime import datetime
import json
import glob
import os
from pathlib import Path
from pprint import pprint 
import shlex
import subprocess
from subprocess import DEVNULL, PIPE, STDOUT
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
        assert self.kernfs_path.exists()

    def start(self):
        'First we run mkfs, then start up the kernfs process.'
        mkfs_args = [ str(self.kernfs_path / 'mkfs.sh') ]
        subprocess.run(mkfs_args, cwd=self.kernfs_path, check=True,
                       stdout=DEVNULL, stderr=DEVNULL)
        
        kernfs_args = shlex.split('sudo -E {0}/run.sh {0}/kernfs'.format(
                                  str(self.kernfs_path)))
                       
        self.proc = subprocess.Popen(kernfs_args, cwd=self.kernfs_path,
                                     env=self.env, start_new_session=True)
        time.sleep(10)

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

    IDX_STRUCTS   = ['EXTENT_TREES', 'GLOBAL_HASH_TABLE', 'NONE']
    LAYOUT_SCORES = ['100', '90', '80', '70', '60']

    def __init__(self, args):
        assert isinstance(args, Namespace)
        self.args = args
        if 'fn' not in self.args:
            raise Exception('No valid benchmark selected!')

        self.root_path = Path(__file__).resolve().parent.parent
        self.kernfs = None
        self.outdir = Path(self.args.outdir)

        self.env = copy.deepcopy(os.environ)
        self.env['MLFS_PROFILE'] = '1'
        self.env['MLFS_LAYOUT_SCORE'] = '100'


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


    def _write_bench_output(self, json_obj, name):
        if not self.outdir.exists():
            self.outdir.mkdir()
                
        filepath = self.outdir / name
        with filepath.open('w') as f:
            json.dump(json_obj, f, indent=4)

    def _parse_filebench_throughput(self, stdout):
        lines = stdout.decode().split(os.linesep)
        for line in lines:
            if 'mb/s' in line:
                print(line)
                fields = line.split(' ')
                for field in fields:
                    if 'mb/s' in field:
                        return float(field.replace('mb/s', ''))
        
        raise Exception('Could not find throughput numbers!')


    def _run_trial(self, bench_args, bench_cwd, processing_fn, timeout=(2*60)):
        self._start_trial()

        try: 
            proc = subprocess.run(bench_args, 
                                  cwd=bench_cwd, check=True, env=self.env, 
                                  stdout=PIPE, stderr=STDOUT, 
                                  timeout=timeout,
                                  input=str.encode(2*os.linesep))

            return processing_fn(proc.stdout)
        except TimeoutError as e: 
            warn('Process "{}" hangs.!'.format(' '.join(bench_args)),
                 UserWarning)
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
            for layout in self.__class__.LAYOUT_SCORES:
                for struct in self.__class__.IDX_STRUCTS:
                    self.env['MLFS_IDX_STRUCT'] = struct
                    self.env['MLFS_LAYOUT_SCORE'] = layout

                    for trialno in range(self.args.trials):
                        print('Running workload "{}", trial #{}.'.format(
                            workload_name, trialno + 1))

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
            stat_summary = { k: stat_obj[k] for k in keys }
            sname = 'filebench_summary_{}_{}.json'.format(workload_name, 
                                                          timestamp_str)
            self._write_bench_output(stat_summary, sname)


    def _run_leveldb(self):
        print('Running LevelDB benchmark.')
        leveldb_path = (self.root_path / 'bench' / 'leveldb'/'build').resolve()
        assert leveldb_path.exists()
       
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
            for layout in self.__class__.LAYOUT_SCORES:
                for struct in self.__class__.IDX_STRUCTS:
                    self.env['MLFS_IDX_STRUCT'] = struct
                    self.env['MLFS_LAYOUT_SCORE'] = layout

                    for trialno in range(self.args.trials):
                        print('Running workload "{}", trial #{}.'.format(
                            workload_name, trialno + 1))

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
            stat_summary = { k: stat_obj[k] for k in keys }
            sname = 'filebench_summary_{}_{}.json'.format(workload_name, 
                                                          timestamp_str)
            self._write_bench_output(stat_summary, sname)

    def run(self):
        self.args.fn(self)


    @classmethod
    def _add_common_arguments(cls, parser):
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
