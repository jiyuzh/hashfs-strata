from argparse import ArgumentParser
from collections import defaultdict
import copy
from IPython import embed
import itertools
import json
from enum import Enum
from math import sqrt
import pandas as pd
import numpy as np
from pathlib import Path
from pprint import pprint
from matplotlib.gridspec import GridSpec, GridSpecFromSubplotSpec
import matplotlib.pyplot as plt
import matplotlib.patheffects as PathEffects
from matplotlib.patches import Patch
from matplotlib.gridspec import GridSpec, GridSpecFromSubplotSpec
import matplotlib.ticker as ticker
import re
import yaml

from Graph import Grapher

import pandas as pd

class NDADataObject:
    def __init__(self, file_path):
        if isinstance(file_path, str):
            file_path = Path(file_path)
        assert isinstance(file_path, Path)
        with file_path.open() as f:
            report_data = json.load(f)
            results_data = report_data['results']

            self.dfs = defaultdict(dict)
            for benchmark, config_data in results_data.items():
                print(benchmark)
                for config_name, raw_df in config_data.items():
                    df = None
                    try:
                        df = pd.DataFrame(raw_df)
                    except:
                        df = pd.Series(raw_df)
                    self.dfs[benchmark][config_name.lower()] = df

    def _reorder_data_frames(self):
        new_dict = defaultdict(dict)
        for x, x_data in self.dfs.items():
            for y, y_df in x_data.items():
                new_dict[y][x] = y_df

        return new_dict

    def reorder_data_frames(self, dfs):
        new_dict = defaultdict(dict)
        for x, x_data in dfs.items():
            for y, y_df in x_data.items():
                new_dict[y][x] = y_df

        return new_dict

    def data_by_benchmark(self):
        return self.dfs

    def data_by_config(self):
        return self._reorder_data_frames()

    def filter_benchmarks(self, benchmark_filter, dfs):
        if benchmark_filter is None:
            return dfs
        new_dfs = copy.deepcopy(dfs)
        for bench in [k for k in dfs]:
            matches_any = False
            for f in benchmark_filter:
                if f == bench:
                    matches_any = True
                    break
            if not matches_any:
                new_dfs.pop(bench, None)
        return new_dfs

    def filter_configs(self, config_filter, dfs):
        assert config_filter is not None
        new_dfs = copy.deepcopy(dfs)
        for bench in [k for k in dfs]:
            for config in [x for x in dfs[bench]]:
                matches_any = False
                for c in config_filter:
                    if c == config:
                        matches_any = True
                        break
                if not matches_any:
                    new_dfs[bench].pop(config, None)

        return new_dfs


    def filter_stats(self, stat, dfs):
        new_dfs = {}
        for x, x_data in dfs.items():
            per_x = {}
            for y, y_data in x_data.items():
                if stat in y_data:
                    per_x[y] = y_data[stat]

            new_df = None
            try:
                new_df = pd.DataFrame(per_x).T
            except:
                new_df = pd.Series(per_x).T
            new_dfs[x] = new_df

        return new_dfs

    def filter_stat_field(self, field, dfs):
        new_dfs = {}
        for x, x_df in dfs.items():
            if isinstance(x_df, pd.Series):
                return pd.DataFrame(dfs)
            per_x = {}
            for y, y_data in x_df.T.items():
                if field not in y_data:
                    raise Exception('Not available!')
                per_x[y] = y_data[field]

            new_df = pd.Series(per_x)
            new_dfs[x] = new_df

        return pd.DataFrame(new_dfs)

    def average_stats(self, dfs):
        averages = {}
        for x, df in dfs.items():
            averages[x] = df.mean()

        return pd.DataFrame(averages)

    def _get_stat_attribute(self, stat, attr):
        dataframes = self.data_frames

        stat_data = {}
        for bench, config_dict in dataframes.items():
            stats = {}
            for config, df in config_dict.items():
                stats[config] = df[stat][attr]
            per_bench = pd.Series(stats)
            stat_data[bench] = per_bench

        return pd.DataFrame(stat_data)

    def calculate_cpi_breakdown(self, dfs):
        benchmark_dfs = {}
        for benchmark, config_data in dfs.items():
            baseline_cpi = 1.0 / config_data['hdd']['throughput']

            stat_per_config = defaultdict(dict)
            for config_name, df in config_data.items():

                config_cpi = 1.0 / df['throughput']
                cpi_ratio  = config_cpi / baseline_cpi

                df = df.fillna(0.0)

                components = []
                print(df)
                indexing   = df['indexing']
                read_data  = df['read data']
                components = [indexing, read_data]

                combined_df = pd.Series(components)
                combined_df.index = ['indexing', 'read data']
                all_reasons = combined_df
                sum_all_reasons = combined_df.sum()

                all_reasons /= sum_all_reasons
                #all_reasons *= cpi_ratio

                stat_per_config[config_name] = all_reasons

            benchmark_df = pd.DataFrame(stat_per_config)
            benchmark_dfs[benchmark] = benchmark_df.T

        return pd.concat(dict(benchmark_dfs), axis=0)

