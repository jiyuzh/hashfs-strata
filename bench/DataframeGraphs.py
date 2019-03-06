from __future__ import print_function
from argparse import ArgumentParser
from collections import defaultdict
from IPython import embed
import itertools
from itertools import count
import json
import enum
from enum import Enum
from math import sqrt, ceil
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patheffects as PathEffects
from matplotlib.patches import Patch
from matplotlib.gridspec import GridSpec, GridSpecFromSubplotSpec
import matplotlib.ticker as ticker
from pathlib import Path
from pprint import pprint
import re
import string
import numpy as np
import pandas as pd

class Grapher:

    COLORS  = ['b', 'c', 'g', 'y', 'r', 'm', 'k']
    SHAPES  = ['o', 'v', '8', 's', 'P', '*', 'X']

    D       = 5
    HATCH   = [D*'..', D*'\\\\', D*'//', D*'', D*'xx', D*'*', D*'+', D*'\\']

    COLORS = [ '#91bfdb', '#fc8d59', '#fee090', '#4575b4', '#d73027', '#e0f3f8' ]

    @staticmethod
    def _hex_color_to_tuple(color_str):
        r = int(color_str[1:3], 16) / 256.0
        g = int(color_str[3:5], 16) / 256.0
        b = int(color_str[5:7], 16) / 256.0
        return (r, g, b)

    @staticmethod
    def _set_rcparams():
        plt.rcParams['hatch.linewidth'] = 0.5
        plt.rcParams['font.family']     = 'serif'
        plt.rcParams['font.size']       = 6
        plt.rcParams['axes.labelsize']  = 6


    def __init__(self, output_file: Path):
        self.output_file = output_file

        self._set_rcparams()

        self.barchart_defaults = { 'edgecolor': 'black', 'linewidth': 1.0, }
        self.flip_orientation = False #args.horizontal

    @staticmethod
    def _do_grid_lines():
        plt.grid(color='0.5', linestyle='--', axis='y', dashes=(2.5, 2.5))
        plt.grid(which='major', color='0.7', linestyle='-', axis='x', zorder=5.0)

    @staticmethod
    def _set_xticks(ax):
        _, xmax = ax.get_xlim()
        major_tick = round(float(xmax / 10.0), 1)
        if major_tick > 10.0:
            major_tick = 5.0 * int(major_tick / 5)
        ax.xaxis.set_major_locator(ticker.MultipleLocator(major_tick))
        ax.xaxis.set_minor_locator(ticker.MultipleLocator(major_tick / 5.0))
        ax.set_axisbelow(True)


    def _graph_subplot(self, df, grid_spec, xlabel, no_label=False, error_df=None, error_threshold=0.0, cutoff=None):
        '''
            Grouped bars.
        '''

        if error_df is not None and error_threshold > 0.0:
            print('{} graph: Setting error bar minimum to +/- {} for visibility'.format(
                stat, threshold))
            error_df = error_df.clip(lower=threshold)
        elif error_df is None:
            error_df = df.copy()
            error_df[:] = 0

        num_groups = len(df.columns)
        width = num_groups / (num_groups + 1)

        axis = plt.subplot(grid_spec)
        if cutoff is not None:
            axis.set_xlim(left=0.0, right=cutoff)
        axis.margins(x=0, y=0)
        print(df)
        ax = df.plot.barh(ax=axis,
                          xerr=error_df,
                          width=width,
                          color='0.75',
                          **self.barchart_defaults)

        text_df = df.T
        if cutoff is not None:
            for i, bench in enumerate(text_df):
                for j, v in enumerate(reversed(text_df[bench])):
                    y = i - ((j - 3.0) / (len(text_df) + 1))
                    if v >= cutoff:
                        s = '{0:.1f}'.format(v)
                        txt = ax.text(cutoff * 0.9, y, s, color='white',
                                      fontweight='bold', fontfamily='sans',
                                      fontsize=6)
                        txt.set_path_effects([PathEffects.withStroke(linewidth=1, foreground='black')])

        ybounds = axis.get_ylim()

        artist = []

        #ax.invert_yaxis()
        self._set_xticks(ax)

        bars = ax.patches

        num_bars   = df.shape[0]
        hatches = (self.__class__.HATCH)[:num_groups]
        all_hatches = sum([ list(itertools.repeat(h, num_bars))
            for h in hatches ], [])

        colors = self.__class__.COLORS[:num_groups]
        all_colors = sum([ list(itertools.repeat(c, num_bars))
            for c in colors ], [])

        for bar, hatch, color in zip(bars, all_hatches, all_colors):
            #bar.set_hatch(hatch)
            bar.set_color(color)
            bar.set_edgecolor('black')

        plt.sca(axis)
        self.__class__._do_grid_lines()

        plt.xlabel(xlabel)

        if no_label:
            labels = plt.yticks()
            plt.yticks(ticks=range(len(labels)), labels=['']*len(labels))
            axis.legend().set_visible(False)
            return None
        else:
            legend = plt.legend(loc='best', prop={'size': 6})
            artist += [legend]

            return artist, ybounds

    def _graph_subplot_stacked(self, dataframes, grid_spec, xlabel, cutoff=None, which_legend=None, no_yticks=False, ylabel=str()):
        '''
        '''
        once = False

        axis = plt.subplot(grid_spec)

        if cutoff is not None:
            axis.set_xlim(0, cutoff)
        axis.margins(x=0, y=0)

        hatches = self.__class__.HATCH
        labels = None

        config_patches = []
        reason_patches = []

        hatches = self.__class__.HATCH[:len(dataframes)]

        bottom = defaultdict(lambda: None)
        for component, df, hatch in zip(dataframes.keys(), dataframes.values(), hatches):

            # reversed for top to bottom
            # -- index is number of slots on y axis
            index = np.arange(df.shape[0])
            num_slots = float(df.shape[1] + 1)
            width = 1.0 / num_slots
            colors = self.__class__.COLORS[:len(df.columns)]

            for reason, color, slot in zip(df.columns, colors, count()):
                data = df[reason].values

                config_color = self._hex_color_to_tuple(color)

                new_index = index - ((slot + 1.0 - (num_slots / 2.0)) * width)

                axis.barh(new_index,
                          data,
                          height=width,
                          left=bottom[reason],
                          color=config_color,
                          hatch=hatch,
                          label=component,
                          **self.barchart_defaults)


                bottom[reason] = data if bottom[reason] is None else data + bottom[reason]

                if cutoff is not None:
                    for i, d, name in zip(new_index, bottom, df[reason].index):
                        s = '{0:.1f}'.format(d) if d >= cutoff else ''
                        txt = axis.text(cutoff * 0.9, i - (0.5 * width), s, color='white',
                                        fontweight='bold', fontfamily='sans',
                                        fontsize=6)
                        txt.set_path_effects([PathEffects.withStroke(linewidth=1, foreground='black')])

                if labels is None:
                    labels = df[reason].index

            config_patches += [Patch(facecolor='white',
                                     edgecolor='black',
                                     hatch=hatch,
                                     label=component)]

            if len(reason_patches) == 0:
                for reason, color in zip(df.columns, colors):
                    reason_patches += [Patch(facecolor=color,
                                             edgecolor='black',
                                             label=reason)]


        plt.sca(axis)
        tick_labels = list(dataframes.values())[0].index if not no_yticks else ['']*len(index)
        plt.yticks(ticks=index, labels=tick_labels)
        legend = []
        if which_legend == 0:
            legend = [plt.legend(handles=reason_patches, loc='upper center')]
        elif which_legend == 1:
            legend = [plt.legend(handles=config_patches, loc='upper center')]
        elif which_legend == -1:
            config_legend = plt.legend(handles=config_patches, loc='upper center')
            reason_legend = plt.legend(handles=reason_patches, loc='upper center')
            axis.add_artist(config_legend)
            legend = [config_legend, reason_legend]

        self._set_xticks(axis)

        plt.xlabel(xlabel)
        if not no_yticks:
            plt.ylabel(ylabel)

        self.__class__._do_grid_lines()

        #return [config_legend, reason_legend]
        #return [reason_legend, None]
        return legend


    def graph_dataframes(self, dfs):
        if not isinstance(dfs, list):
            dfs = [dfs]

        num_dfs = len(dfs)
        gridspecs = GridSpec(1, num_dfs)

        artist = None
        for df, gs in zip(dfs, gridspecs):
            a, y = self._graph_subplot(df, gs, 'TODO')
            if artist is None:
                artist = a

        plt.subplots_adjust(wspace=0.05, hspace=0.0)

        fig = plt.gcf()
        fig.set_size_inches(3.5 * num_dfs, 2.5)
        fig.tight_layout()

        output_file = str(self.output_file)
        plt.savefig(output_file, bbox_inches='tight', pad_inches=0.02,
                additional_artists=artist)
        plt.close()

    def graph_dataframes_stacked(self, df_dicts, ylabel=str()):
        if not isinstance(df_dicts, dict):
            df_dicts = {'default': df_dicts}

        gridspecs = GridSpec(1, len(df_dicts))

        artists = []
        for testname, letter, df_dict, gs, legend in zip(df_dicts.keys(), list(string.ascii_uppercase), df_dicts.values(), gridspecs, count()):
            label = '{}) {}'.format(letter, testname)
            legends = self._graph_subplot_stacked(df_dict, gs, label,
                    which_legend=legend, no_yticks=legend > 0 and False, ylabel=ylabel)
            artists += legends

        plt.subplots_adjust(wspace=0.05, hspace=0.0)

        fig = plt.gcf()
        fig.set_size_inches(7.0, 2.5)
        fig.tight_layout()

        output_file = str(self.output_file)
        plt.savefig(output_file, bbox_inches='tight', pad_inches=0.02,
                    additional_artists=artists)
        plt.close()
