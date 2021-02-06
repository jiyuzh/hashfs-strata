from argparse import ArgumentParser
from collections import defaultdict
from IPython import embed
import copy
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
from IDXDataObject import IDXDataObject

import pandas as pd

class IDXGrapher:

    def __init__(self, args):
        self.args = args
        self.data = IDXDataObject(file_path=Path(args.input_file))
        self.output_dir = Path(args.output_dir)
        assert self.output_dir.exists()

        self._handle_schema_file(args)
        
        with self.schema_file.open() as f:
            self.schema_config = yaml.safe_load(f)
            self.schemas = self.schema_config['schemes']
            assert args.schema_name in self.schemas
            self.schema = self.schemas[args.schema_name]
            self.schema_name = args.schema_name
            self.config_sets = self.schema_config['config_sets']

    def _handle_schema_file(self, args):
        if 'schema_file' in args and args.schema_file is not None:
            self.schema_file = Path(args.schema_file)
        elif 'schema_template' in args and args.schema_template is not None:
            from jinja2 import Template, Environment
            tmpl_file = Path(args.schema_template)
            assert tmpl_file.exists()

            self.schema_file = Path(args.schema_template.replace('.j2', ''))
            assert self.schema_file != tmpl_file

            with tmpl_file.open() as f:
                jinja_env = Environment(extensions=['jinja2.ext.do'],
                        trim_blocks=True, lstrip_blocks=True)
                tmpl = jinja_env.from_string(f.read())
                schema_raw = tmpl.render()

                with self.schema_file.open('w') as f:
                    f.write(schema_raw)
            
        else:
            raise Exception('Bad path!')
        
        assert self.schema_file.exists()

    def _filter_configs(self, df, layout):
        config_set_name = layout['config_set'] if 'config_set' in layout else 'default'
        config_set = [c.upper() for c in self.config_sets[config_set_name]]
        
        return df[df.struct.isin(config_set)]

    def _plot_single_stat(self, gs, layout):
        grapher = Grapher(self.args)

        df = self.data.get_dataframe()
        # options = layout['options']
        # embed()

        df = self._filter_configs(df, layout)

        config = layout['data_config']
        for col, val in config['filter'].items():
            if isinstance(val, list):
                df = df[df[col].isin(val)]
            else:
                df = df[df[col] == val]

        new_index = [*config['axis'], config['groups']] \
                    if isinstance(config['axis'], list) else \
                        [config['axis'], config['groups']]

        # embed()
        df = df.set_index(new_index)
        df = df[~df.index.duplicated(keep='last')]
        
        # embed()
        series = df[config['plot']]
        means_df = series.unstack()
        ci_df = df[f'{config["plot"]}_ci'].unstack().fillna(0)
        # embed()

        if means_df.index.names[0] == 'layout':
            means_df.sort_index(axis=0, ascending=False, inplace=True)
            ci_df.sort_index(axis=0, ascending=False, inplace=True)

        flush = True

        # By this point, means_df should be two dimensional
        return grapher.graph_single_stat(means_df, ci_df, gs,
                                         flush=flush, **layout['options'])

    def _plot_grouped_stacked(self, gs, layout):
        grapher = Grapher(self.args)

        df = self.data.get_dataframe()
        options = layout['options']

        df = self._filter_configs(df, layout)

        config = layout['data_config']
        for col, val in config['filter'].items():
            if isinstance(val, list) or isinstance(val, tuple):
                df = df[df[col].isin(val)]
            else:
                df = df[df[col] == val]

            if 'repetitions' in df:
                df = df[df['repetitions'] != '1']

        new_index = [*config['axis'], config['groups']] \
                    if isinstance(config['axis'], list) else \
                        [config['axis'], config['groups']]

        df = df.set_index(new_index)
        df = df[~df.index.duplicated(keep='last')]
        
        assert isinstance(config['plot'], list)
        # dfs = []
        # for stat in config['plot']:
        #     series = df[stat]
        #     means_df = series.unstack()
        #     # ci_df = df[f'{config["plot"]}_ci'].unstack().fillna(0)

        #     if means_df.index.names[0] == 'layout':
        #         means_df.sort_index(axis=0, ascending=False, inplace=True)
        #         # ci_df.sort_index(axis=0, ascending=False, inplace=True)
            
        #     dfs += [means_df]

        series = df[config['plot']]
        dfs = series.unstack().fillna(0)

        # By this point, means_df should be two dimensional
        return grapher.graph_grouped_stacked_bars(dfs, gs, **options)
        # return grapher.graph_single_stat(means_df, ci_df, gs,
        #                                  flush=flush, **layout['options'])

    def _plot_indexing_breakdown(self, gs, layout):
        grapher = Grapher(self.args)

        dfs = self.data.data_by_benchmark()
        config_set = layout['config_set'] if 'config_set' in layout else 'default'
        dfs = self.data.filter_configs(self.config_sets[config_set], dfs)
        dfs = self.data.filter_stats(['indexing', 'read_data'], dfs)
        if 'benchmarks' in layout:
            if isinstance(layout['benchmarks'], list):
                assert 'layout_score' in layout
                bench_dfs = { b: dfs[b] for b in layout['benchmarks'] }
                compact = {}
                for b, data_dict in bench_dfs.items():
                    compact[b] = pd.concat(data_dict)[str(layout['layout_score'])].T.rename(b)
                #bench_dfs = dfs[layout['benchmarks']]
                #bench_dfs = {b: dfs[b] for b in layout['benchmarks']}
                bench_dfs = { k: v.T for k, v in compact.items()}
                dfs = pd.concat(bench_dfs).swaplevel(0, 1)

                reorg = defaultdict(dict)
                for idx, data in dfs.groupby(level=0):
                    data.index = data.index.droplevel()
                    df = data.unstack()
                    reorg[idx] = df

                dfs = pd.concat(reorg).swaplevel(0, 1)

            else:
                bench_dfs = dfs[layout['benchmarks']]
                bench_dfs = { k: v.T for k, v in bench_dfs.items()}
                dfs = pd.concat(bench_dfs).swaplevel(0, 1)

        options = layout['options']
        if 'average' in options and options['average']:
            dfs = dfs.mean(level=1)
            mi_tuples = [('Average', i) for i in dfs.index]
            dfs.index = pd.MultiIndex.from_tuples(mi_tuples)

        return grapher.graph_grouped_stacked_bars(dfs, gs, **options)

    def _create_table(self, gs, layout):
        grapher = Grapher(self.args)

        df = self.data.get_dataframe()

        config = layout['data_config']
        for col, val in config['filter'].items():
            df = df[df[col] == val]

        new_index = [*config['rows'], config['groups'], *config['columns']]

        df = df.set_index(new_index)

        df = df[~df.index.duplicated(keep='last')]

        series = df[config['values']]
        means_df = series.unstack()

        ci_values = [f'{v}_ci' for v in config['values']]
        series_ci = df[ci_values]

        # if 'baseline' in config:
        #     baseline_value = series.loc[config['baseline']]
        #     for group_name in series.index.get_level_values(0).unique():
        #         if group_name == config['baseline']:
        #             continue

        #         embed()
        #         series.loc[group_name] /= baseline_value
        #         series_ci.loc[group_name] /= baseline_value
     
        # By this point, means_df should be two dimensional
        return grapher.create_single_stat_table(means_df, None, gs, 
                                                **layout['options'])

    def _plot_mtcc(self, gs, layout):
        grapher = Grapher(self.args)

        dfs = self.data.data_by_benchmark()
        options = layout['options']

        config_set = layout['config_set'] if 'config_set' in layout else 'default'
        dfs = self.data.filter_configs(self.config_sets[config_set], dfs)
        dfs = self.data.reorder_data_frames(dfs)
        dfs = self.data.filter_stats(layout['stat'], dfs)

        layout_data = {}
        for idx_struct, idx_res in dfs.items():
            idx_data = {}
            for n_threads, series in idx_res.items():
                thread_num = n_threads.split('_')[0]
                idx_data[thread_num] = series.T.loc[str(layout['layout_score'])][layout['stat']]

            layout_data[idx_struct] = pd.Series(idx_data)

        dfs = pd.DataFrame(layout_data)

        print(layout['stat'])
        print(dfs)

        flush = True
        means_df = dfs
        means_df = means_df.iloc[::-1]
        #ci_df = self.data.filter_stat_field('ci', dfs)
        ci_df = copy.deepcopy(dfs)
        ci_df[:] = 0

        return grapher.graph_single_stat(means_df, ci_df, gs,
                                         flush=flush, **options)

    def plot_schema(self):
        grapher = Grapher(self.args)

        dimensions = self.schema['dimensions']

        artists = []
        for layout_config in self.schema['plots']:
            width = layout_config['size'][0]
            height = layout_config['size'][1]
            axis = plt.subplot2grid(dimensions, layout_config['pos'],
                                    rowspan=width, colspan=height)

            if layout_config['type'] == 'single_stat':
                a = self._plot_single_stat(axis, layout_config)
                artists += [a]
            elif layout_config['type'] == 'grouped_stacked':
                a = self._plot_grouped_stacked(axis, layout_config)
                artists += [a]
            elif layout_config['type'] == 'table':
                a = self._create_table(axis, layout_config)
                artists += [a]
            elif layout_config['type'] == 'mtcc':
                a = self._plot_mtcc(axis, layout_config)
                artists += [a]
            elif layout_config['type'] == 'indexing_breakdown':
                artists += self._plot_indexing_breakdown(axis, layout_config)

        plt.subplots_adjust(wspace=0.05, hspace=0.0)

        fig = plt.gcf()
        print_size = self.schema['print_size']
        fig.set_size_inches(*print_size)
        fig.tight_layout()

        output_file = str(self.output_dir / self.schema['file_name'])
        plt.tight_layout()
        plt.savefig(output_file, dpi=600, bbox_inches='tight', pad_inches=0.02,
                    additional_artists=artists)
        # plt.savefig(output_file, dpi=300, pad_inches=0.02, additional_artists=artists)
        plt.close()

    @classmethod
    def add_parser_args(cls, parser):
        parser.add_argument('--input-file', '-i', default='report.yaml',
                            help='Where the aggregations live')
        parser.add_argument('--output-dir', '-d', default='.',
                            help='Where to output the report')
        parser.add_argument('--config', '-c', default='graph_config.yaml',
                            help='What file to use for this dataset.')

        schema_group = parser.add_mutually_exclusive_group()

        schema_group.add_argument('--schema-file',
                            help='File containing relevant schemas.')
        schema_group.add_argument('--schema-template', 
                                  default='graph_schemes.yaml.j2',
                                  help='File containing schema in jinja2 template.')

        parser.add_argument('--filter', '-f', default=None, nargs='+',
                            help='what benchmarks to filter, all if None')
        parser.add_argument('schema_name', help='Name of the schema to use.')
        graph_schema_fn = lambda args: cls(args).plot_schema()
        parser.set_defaults(fn=graph_schema_fn)

