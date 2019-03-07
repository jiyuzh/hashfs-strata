#! /usr/bin/env python3
from __future__ import print_function
from argparse import ArgumentParser
from collections import defaultdict
from IPython import embed
import itertools
import json
import enum
from pathlib import Path
from pprint import pprint
import re
import numpy as np
import pandas as pd

import typing

from DataframeGraphs import Grapher

def recursive_dict():
    return defaultdict(recursive_dict)

class ResultsParser:

    TEST_NAMES = { 'rand': 'Random Reads',
                   'seq': 'Sequential Reads',
                   'filemicro_rand_4k': 'Random Reads',
                   'filemicro_seq_4k': 'Sequential Reads' }
    DATA_STRUCT_NAMES = { 'extent_trees':      'ext (API)',
                          'none':              'ext (Strata)',
                          'global_hash_table': 'hash' }

    DATA_STRUCT_BASELINE = DATA_STRUCT_NAMES['none']

    def __init__(self, results_dir: Path, input_files: list):
        pd.set_option('display.float_format', lambda x: '%.3f' % x)
        self.results_dir = results_dir
        self.input_files = input_files
        assert self.results_dir.exists() or self.input_files is not None

    def _filter_raw_data(self, raw_data):
        if 'lsm' in raw_data and 'read_data' in raw_data and 'l0' in raw_data:
            return pd.Series({
                'indexing':   raw_data['lsm']['tsc'],
                'data':       raw_data['read_data']['tsc'],
                'other':      raw_data['l0']['tsc'],
                'throughput': raw_data['throughput']
            })
        else:
            return pd.Series(raw_data)

    @classmethod
    def _filter_test_name(cls, tname):
        if tname not in cls.TEST_NAMES:
            return 'UNKNOWN TEST'
        return cls.TEST_NAMES[tname]

    @classmethod
    def _filter_data_struct(cls, name):
        if name not in cls.DATA_STRUCT_NAMES:
            return name
        return cls.DATA_STRUCT_NAMES[name]

    def _get_data_objects(self, input_file):
        raw_data = list()
        with input_file.open() as json_file:
            raw_data = json.load(json_file)
            if not isinstance(raw_data, list):
                raw_data = [raw_data]

        return raw_data

    def _get_series(self, input_file):
        data = []
        for raw_data in self._get_data_objects(input_file):
            data += [self._filter_raw_data(raw_data)]

        experiments_df = pd.DataFrame(data)
        return experiments_df.mean()

    def _get_all_dataframes(self):
        data = recursive_dict()
        for dirent in self.results_dir.iterdir():
            if 'json' in dirent.name:
                print(dirent.name)
                parts = dirent.name.split('.')[0].split('_')
                df = self._get_series(dirent)
                datastruct   = df['struct'] #parts[0]
                benchmark    = df['bench'] #parts[1]
                testname     = df['workload']#parts[2]
                layout_score = df['layout'] #float(parts[3].replace('layout','')) / 100.0
                data[benchmark][testname][datastruct][layout_score] = df

        dfs = recursive_dict()
        dfs_total = recursive_dict()
        for bname, test_data in data.items():
            for tname, result_data in test_data.items():
                tname = self._filter_test_name(tname)
                ext = self.__class__.DATA_STRUCT_BASELINE
                ext_df  = pd.DataFrame(result_data[ext]).T
                hash_df = pd.DataFrame(result_data['hash']).T

                if hash_df.empty:
                    hash_df = ext_df.copy()
                    hash_df[:] = 0.0

                ext_norm  = ext_df.T.sum()
                hash_norm = hash_df.T.sum()

                # Normalize to extents to compare throughput overall.
                total = { ext: ext_df.T.sum() / ext_norm,
                         'hash': hash_df.T.sum() / ext_norm}
                dfs_total[bname][tname] = pd.DataFrame(total).fillna(0).sort_index()

                # Normalize to self for components, because these run for a
                # fixed period of time and we want to show lower indexing.
                for reason in ext_df.columns:
                    data = { ext: ext_df[reason] / ext_norm,
                             'hash': hash_df[reason] / hash_norm}
                    dfs[bname][tname][reason] = pd.DataFrame(data).fillna(0).sort_index()


        return dfs, dfs_total

    def _get_all_dataframes_files(self):
        data = recursive_dict()
        stats_objs = []

        for input_file in self.input_files:
            assert isinstance(input_file, Path)
            with input_file.open() as f:
                stats_objs = json.load(f)

            for data_obj in stats_objs:
                #parts = dirent.name.split('.')[0].split('_')
                df = self._filter_raw_data(data_obj)
                datastruct   = self._filter_data_struct(data_obj['struct'])
                benchmark    = data_obj['bench']
                testname     = data_obj['workload']
                layout_score = data_obj['layout']
                data[benchmark][testname][datastruct][layout_score] = df

        dfs = recursive_dict()
        dfs_total = recursive_dict()
        for bname, test_data in data.items():
            for tname, result_data in test_data.items():
                tname = self._filter_test_name(tname)
                ext = self.__class__.DATA_STRUCT_BASELINE
                ext_df  = pd.DataFrame(result_data[ext]).T

                ext_norm  = ext_df['throughput']
                total = { ext: ext_df['throughput'] / ext_norm }

                for data_struct, res in result_data.items():
                    if data_struct == ext:
                        continue
                    data_struct_df = pd.DataFrame(res).T

                    # Normalize to extents to compare throughput overall.
                    total[data_struct] = data_struct_df['throughput'] / ext_norm

                dfs_total[bname][tname] = pd.DataFrame(total).fillna(0).sort_index()

                # Normalize to self for components, because these run for a
                # fixed period of time and we want to show lower indexing.
                ext_df = ext_df.T.drop('throughput')
                for reason in ext_df.T.columns:
                    if 'throughput' in str(reason):
                        continue
                    data = { ext: ext_df.T[reason] / ext_df.sum() }
                    for data_struct, res in result_data.items():
                        if data_struct == ext:
                            continue
                        data_struct_df = pd.DataFrame(res).drop('throughput')
                        data_struct_norm = data_struct_df.sum()
                        data[data_struct] = data_struct_df.T[reason] / data_struct_norm

                    dfs[bname][tname][reason] = pd.DataFrame(data).fillna(0).sort_index()

        return dfs, dfs_total


    def do_graph(self, output_file, benchmark):
        grapher = Grapher(output_file)
        component_dfs, total_dfs = self._get_all_dataframes()

        for bench, test_data in total_dfs.items():
            if benchmark == bench:
                for testname, result_df in test_data.items():
                    grapher.graph_dataframes(result_df)


    def do_graph_stacked(self, output_file, benchmark):
        grapher = Grapher(output_file)
        component_dfs, total_dfs = self._get_all_dataframes_files()

        assert len(component_dfs) >= 1

        for bench, test_data in component_dfs.items():
            if benchmark == bench:
                grapher.graph_dataframes_stacked(test_data, total_dfs[bench])



################################################################################

def path_arg(arg):
    try:
        return Path(arg)
    except:
        raise

def add_common_args(parser):
    parser.add_argument('--input-dir', '-i', default='../results/filtered',
                        type=path_arg, help='Input directory with JSON files.')
    parser.add_argument('--input-files', '-f', default=None, nargs='*',
                        type=path_arg, help='Input JSON file.')
    parser.add_argument('--output_file', '-o',
                        type=path_arg, help='Output graph file.')

def add_arguments(parser):

    subparsers = parser.add_subparsers()

    filebench = subparsers.add_parser('filebench')
    add_common_args(filebench)
    filebench.set_defaults(benchmark='filebench',
                           output_file='filebench.pdf',
                           fn=ResultsParser.do_graph_stacked)

    leveldb = subparsers.add_parser('leveldb')
    add_common_args(leveldb)
    leveldb.set_defaults(benchmark='leveldb',
                         output_file='leveldb.pdf',
                         fn=ResultsParser.do_graph)


def main():
    parser = ArgumentParser(
            description='Graph results from file-indexing experiments')

    add_arguments(parser)
    args = parser.parse_args()

    res = ResultsParser(args.input_dir, args.input_files)
    args.fn(res, args.output_file, args.benchmark)


if __name__ == "__main__":
    main()
