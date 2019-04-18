from __future__ import print_function
from argparse import ArgumentParser
from collections import defaultdict
import copy
from IPython import embed
import itertools
import json
import enum
from enum import Enum
from math import sqrt, ceil, isnan
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patheffects as PathEffects
from matplotlib.font_manager import FontProperties
from matplotlib.patches import Patch
from matplotlib.gridspec import GridSpec, GridSpecFromSubplotSpec
import matplotlib.ticker as ticker
from pathlib import Path
from pprint import pprint
import re
import numpy as np
import yaml

import pandas as pd
pd.set_option('display.float_format', lambda x: '%.3f' % x)
#pd.set_option('display.max_rows', None)

class Grapher:

    D       = 5
    HATCH   = [D*'//', '', D*'\\\\', D*'..', D*'', D*'xx', D*'*', D*'+', D*'\\']

    def _hex_color_to_tuple(self, color_str):
        r = int(color_str[1:3], 16) / 256.0
        g = int(color_str[3:5], 16) / 256.0
        b = int(color_str[5:7], 16) / 256.0
        return (r, g, b)

    def __init__(self, args):
        self.config_file = Path(args.config)
        assert self.config_file.exists()

        with self.config_file.open() as f:
            self.config = yaml.safe_load(f)

        plt.rcParams['hatch.linewidth'] = 0.5
        plt.rcParams['font.family']     = 'serif'
        plt.rcParams['font.size']       = 6
        plt.rcParams['axes.labelsize']  = 6

        self.barchart_defaults = {
                                    'edgecolor': 'black',
                                    'linewidth': 1.0,
                                 }

    def _get_config_name(self, config_name):
        name = config_name.lower()
        if name in self.config['display_options']['config_names']:
            return self.config['display_options']['config_names'][name]
        return config_name

    def _get_config_color(self, config_name):
        name = self._get_config_name(config_name)
        color = '#000000'
        if name in self.config['display_options']['config_colors']:
            color = self.config['display_options']['config_colors'][name]
        return self._hex_color_to_tuple(color)

    def _get_config_order(self, config_name):
        name = self._get_config_name(config_name)
        order_list = self.config['display_options']['config_order']
        order = len(order_list)
        if name in order_list:
            order = order_list.index(name)
        return order

    @staticmethod
    def _clean_benchmark_names(benchmark_names):
        return benchmark_names
        new_names = []
        for b in benchmark_names:
            try:
                bench = b.split('.')[1].split('_')[0]
                new_names += [bench]
            except:
                new_names += [b]
        return new_names

    @staticmethod
    def _do_grid_lines():
        plt.grid(color='0.5', linestyle='--', axis='y', dashes=(2.5, 2.5))
        plt.grid(which='major', color='0.7', linestyle='-', axis='x', zorder=5.0)

    def _rename_configs(self, df):
        columns = df.columns.unique(0).tolist()
        new_columns = {c: self._get_config_name(c) for c in columns}
        return df.rename(columns=new_columns)

    def _rename_multi_index(self, df):
        index = df.index.unique(1).tolist()
        new_index = [self._get_config_name(c) for c in index]
        df.index = df.index.set_levels(new_index, level=1)
        return df

    def _reorder_configs(self, df):
        columns = df.columns.unique(0).tolist()
        order_fn = lambda s: self._get_config_order(s)
        sorted_columns = sorted(columns, key=order_fn)
        return df[sorted_columns]

        return all_means, all_error, all_perct, labels

    def _kwargs_bool(self, kwargs_dict, field):
        if field not in kwargs_dict or not kwargs_dict[field]:
            return False
        return True

    def _kwargs_default(self, kwargs_dict, field, default_val):
        if field not in kwargs_dict:
            return default_val
        return kwargs_dict[field]

    def _kwargs_has(self, kwargs_dict, field):
        return field in kwargs_dict and kwargs_dict[field] is not None

    def graph_single_stat(self, means_df, ci_df, axis, **kwargs):
        all_means = self._rename_configs(self._reorder_configs(means_df))
        all_error = self._rename_configs(self._reorder_configs(ci_df))
        all_perct = None
        labels = means_df.index

        if self._kwargs_bool(kwargs, 'error_bars'):
            threshold = 0.05
            print('Setting error bar minimum to +/- {} for visibility'.format(threshold))
            #print(all_error)
            all_error = all_error.clip(lower=threshold)
            if 'Average' in all_error.index:
                all_error.loc['Average'][:] = 0.0
            #print(all_error)
        else:
            all_error[:] = 0

        num_configs = len(all_means.columns)
        width = num_configs / (num_configs + 2)
        bar_width = width / num_configs

        max_val = (all_means + all_error + 0.5).max().max()
        cutoff = self._kwargs_default(kwargs, 'cutoff', max_val)
        if cutoff != max_val:
            axis.set_xlim(0.0, cutoff)
        axis.margins(0.0)

        ax = all_means.plot.barh(ax=axis,
                                 xerr=all_error,
                                 width=width,
                                 color='0.75',
                                 **self.barchart_defaults)

        text_df = all_means.T
        min_y_pos = 0.0
        max_y_pos = 0.0
        for i, bench in enumerate(text_df):
            for j, v in enumerate(reversed(text_df[bench])):
                offset = (j * bar_width) - (bar_width * (num_configs / 2)) + \
                        (bar_width / 2)
                y = i - offset
                min_y_pos = min(y - (bar_width / 2), min_y_pos)
                max_y_pos = max(y + (bar_width / 2), max_y_pos)
                precision = self._kwargs_default(kwargs, 'precision', 1)
                s = ('{0:.%df} ' % precision).format(v) if v >= cutoff or \
                        self._kwargs_bool(kwargs, 'add_numbers') else ''
                pos = cutoff if v >= cutoff else v
                if isnan(pos):
                    continue
                txt = ax.text(pos, y, s, va='center', ha='right', color='white',
                              fontweight='bold', fontfamily='sans',
                              fontsize=6)
                txt.set_path_effects([PathEffects.withStroke(linewidth=1, foreground='black')])

                if self._kwargs_bool(kwargs, 'label_bars'):
                    label_txt = ' ' + list(reversed(text_df[bench].index.tolist()))[j]
                    txt2 = ax.text(0, y, label_txt, va='center', ha='left',
                                   color='white', fontweight='bold', fontfamily='sans',
                                   fontsize=6)
                    txt2.set_path_effects([PathEffects.withStroke(linewidth=1, foreground='black')])

        ybounds = axis.get_ylim()
        if self._kwargs_bool(kwargs, 'flush'):
            axis.set_ylim(min_y_pos, max_y_pos)

        artist = []
        labels = self._clean_benchmark_names(labels)

        ax.invert_yaxis()
        major_tick = max(float(int(cutoff / 10.0)), 1.0)
        if major_tick > 10.0:
            major_tick = 5.0 * int(major_tick / 5)
        ax.xaxis.set_major_locator(ticker.MultipleLocator(major_tick))
        if cutoff > 5.0:
            ax.xaxis.set_minor_locator(ticker.MultipleLocator(major_tick / 5.0))
        else:
            ax.xaxis.set_minor_locator(ticker.MultipleLocator(major_tick / 10.0))
        ax.set_axisbelow(True)

        bars = ax.patches

        num_configs = all_means.shape[1]
        num_bench   = all_means.shape[0]
        hatches = (self.__class__.HATCH * 10)[:num_configs]
        all_hatches = sum([ list(itertools.repeat(h, num_bench))
            for h in hatches ], [])

        colors = [self._get_config_color(c) for c in all_means.columns ]
        all_colors = sum([ list(itertools.repeat(c, num_bench))
            for c in colors ], [])

        for bar, hatch, color in zip(bars, all_hatches, all_colors):
            #bar.set_hatch(hatch)
            bar.set_color(color)
            bar.set_edgecolor('black')

        scale = self._kwargs_default(kwargs, 'scale', 1.0)
        if scale < 1.0:
            print('Scaling!')
            box = axis.get_position()
            new_box = [box.x0, box.y0 + box.height * (1.0 - scale),
                       box.width, box.height * scale]
            axis.set_position(new_box)

        plt.sca(axis)
        self.__class__._do_grid_lines()

        plt.xlabel(self._kwargs_default(kwargs, 'label', ''))
        plt.xscale(self._kwargs_default(kwargs, 'xscale', 'linear'))

        if 'xscale' in kwargs and 'symlog' in kwargs['xscale']:
            locmin = ticker.LogLocator(base=10.0, subs=(0.1,0.2,0.4,0.6,0.8,1,2,4,6,8,10 ))
            ax.xaxis.set_minor_locator(locmin)
            ax.xaxis.set_minor_formatter(ticker.NullFormatter())

        if self._kwargs_bool(kwargs, 'exclude_tick_labels'):
            plt.yticks(ticks=range(len(labels)), labels=['']*len(labels))
        else:
            plt.yticks(ticks=range(len(labels)), labels=labels, ha='right')

            minor_ticks = []
            if self._kwargs_has(kwargs, 'per_tick_label'):
                # Required to determine position of ticks.
                plt.gcf().canvas.draw()
                for tick in axis.yaxis.get_major_ticks():
                    tick_label = tick.label.get_text()
                    if tick_label not in kwargs['per_tick_label']:
                        continue

                    opt = kwargs['per_tick_label'][tick_label]

                    if 'font' in opt:
                        tick.label.set_fontproperties(FontProperties(**opt['font']))

                    if self._kwargs_bool(opt, 'line_before'):
                        minor_ticks += [tick.get_loc() - 0.5]

            if len(minor_ticks):
                axis.yaxis.set_minor_locator(ticker.FixedLocator(minor_ticks))
                axis.tick_params(which='minor', axis='y', length=0, width=0)
                plt.grid(which='minor', color='k', linestyle='-', axis='y',
                        zorder=5.0, linewidth=2)


        if self._kwargs_has(kwargs, 'legend'):
            legend = plt.legend(**kwargs['legend'])
            artist += [legend]
        else:
            plt.legend().set_visible(False)

        return artist, ybounds

    def graph_grouped_stacked_bars(self, dataframes, axis, **kwargs):
        dataframes = self._rename_multi_index(dataframes)

        df_list   = [ df for name, df in dataframes.items()   ]
        df_labels = [ name for name, df in dataframes.items() ]

        num_bench = dataframes.index.levshape[0]
        # reversed for top to bottom
        index = np.arange(num_bench)[::-1]
        dfs = dataframes.swaplevel(0,1).sort_index().T
        max_val = ceil(dfs.sum().max() + 0.6)
        cutoff = self._kwargs_default(kwargs, 'cutoff', max_val)
        if cutoff != max_val:
            axis.set_xlim(0, cutoff)
        axis.margins(x=0, y=0)

        dfs = self._reorder_configs(dfs)

        n = 0.0
        num_slots = float(len(dfs.columns.unique(0)) + 1)
        width = 1.0 / num_slots
        print(num_slots, width)

        config_index = 0
        hatches = self.__class__.HATCH
        labels = None

        config_patches = []
        reason_patches = []
        for config in dfs.columns.unique(0):
            bottom = None
            df = dfs[config].T.sort_index(ascending=False)
            print(df)

            config_color = np.array(self._get_config_color(config))
            reason_index = 0

            new_index = index - ((n + 1.0 - (num_slots / 2.0)) * width)

            for reason in df.columns:
                data = df[reason].values

                hatch = hatches[reason_index % len(hatches)]

                axis.barh(new_index,
                          data,
                          height=width,
                          left=bottom,
                          color=config_color,
                          hatch=hatch,
                          label=config,
                          **self.barchart_defaults)


                bottom = data if bottom is None else data + bottom
                reason_index += 1

            for i, d, name in zip(new_index, bottom, df[reason].index):
                #offset = (i * width) - (width * (num_slots / 2)) - (width / 2)
                #y = i - offset
                y = i
                s = '{0:.1f} '.format(d)
                pos = cutoff if cutoff < d else d
                if isnan(pos):
                    continue
                if self._kwargs_bool(kwargs, 'add_numbers'):
                    txt = axis.text(pos, y, s, va='center', ha='right', color='white',
                                    fontweight='bold', fontfamily='sans',
                                    fontsize=6)
                    txt.set_path_effects([PathEffects.withStroke(linewidth=1, foreground='black')])

                if self._kwargs_bool(kwargs, 'label_bars'):
                    txt2 = axis.text(0, y, ' ' + config, va='center', ha='left', color='white',
                                    fontweight='bold', fontfamily='sans',
                                    fontsize=6)
                    txt2.set_path_effects([PathEffects.withStroke(linewidth=1, foreground='black')])

                if labels is None:
                    labels = sorted(df[reason].index, reverse=False)

            config_patches += [Patch(facecolor=config_color, edgecolor='black',
                                     label=config)]

            if len(reason_patches) == 0:
                for reason, hatch, i in zip(df.columns, hatches, itertools.count()):
                    reason_patches += [Patch(facecolor='white',
                                             edgecolor='black',
                                             hatch=hatch,
                                             label=reason)]

            n += 1.0
            config_index += 1

        plt.sca(axis)
        if self._kwargs_bool(kwargs, 'exclude_tick_labels'):
            plt.yticks(ticks=np.arange(num_bench), labels=['']*num_bench)
        else:
            print(labels)
            plt.yticks(ticks=np.arange(num_bench), labels=labels)
        plt.xscale(self._kwargs_default(kwargs, 'xscale', 'linear'))
        #axis.set_ylim(*ybounds)

        config_legend = None
        reason_legend = None
        if self._kwargs_has(kwargs, 'config_legend'):
            config_legend = plt.legend(handles=config_patches,
                    **kwargs['config_legend'])
        if self._kwargs_has(kwargs, 'breakdown_legend'):
            reason_legend = plt.legend(handles=reason_patches,
                    **kwargs['breakdown_legend'])
            if self._kwargs_has(kwargs, 'config_legend'):
                axis.add_artist(config_legend)

        if self._kwargs_default(kwargs, 'xscale', 'linear') != 'linear':
            interval = max(1.0, cutoff / 1.0)
            #axis.tick_params(labelsize=4)
            axis.xaxis.set_major_locator(ticker.MultipleLocator(interval))
            axis.xaxis.set_minor_locator(ticker.MultipleLocator(interval / 10.0))

        axis.set_axisbelow(True)

        plt.xlabel(self._kwargs_default(kwargs, 'label', ''))

        self.__class__._do_grid_lines()

        return [x for x in [config_legend, reason_legend] if x is not None]


    def _get_stat_attribute(self, dataframes, stat, attr):
        stat_data = {}
        for bench, config_dict in dataframes.items():
            stats = {}
            for config, df in config_dict.items():
                stats[config] = df[stat][attr]
            per_bench = pd.Series(stats)
            stat_data[bench] = per_bench

        return pd.DataFrame(stat_data)

