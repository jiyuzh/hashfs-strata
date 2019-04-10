#! /usr/bin/env python3
from argparse import ArgumentParser
from collections import defaultdict
from IPython import embed
import itertools
import json
from enum import Enum
from math import sqrt
import pandas as pd
import numpy as np
from pathlib import Path
from pprint import pprint
import re

from Graph import Grapher
from NDAGrapher import NDAGrapher
from NDADataObject import NDADataObject

import pandas as pd
pd.set_option('display.float_format', lambda x: '%.3f' % x)
#pd.set_option('display.max_rows', None)

################################################################################

def add_args(parser):
    subparsers = parser.add_subparsers()

    # For summaries!
    summary_fn = lambda args: Grapher(args).output_text(
                            NDADataObject(args.input_file).data_by_benchmark())
    summary = subparsers.add_parser('summary',
                                    help='Display relavant results')
    summary.add_argument('--input-file', '-i', default='report.json',
                         help='Where the aggregations live')
    summary.add_argument('--output-dir', '-d', default='.',
                         help='Where to output the report')
    summary.add_argument('--config', '-c', default='graph_config.yaml',
                         help='What file to use for this dataset.')
    summary.set_defaults(fn=summary_fn)

    # For graphing!
    graph = subparsers.add_parser('graph',
                                  help='Graph from report.json')
    NDAGrapher.add_parser_args(graph)

################################################################################

def main():
    parser = ArgumentParser(description='Aggregate results from parallel simulation.')
    add_args(parser)

    args = parser.parse_args()
    args.fn(args)

if __name__ == '__main__':
    exit(main())
