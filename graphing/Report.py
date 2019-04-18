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
from IDXGrapher import IDXGrapher
from IDXDataObject import IDXDataObject

import pandas as pd
pd.set_option('display.float_format', lambda x: '%.3f' % x)
#pd.set_option('display.max_rows', None)

################################################################################

def add_args(parser):
    subparsers = parser.add_subparsers()

    # For processing!
    process_fn = lambda args: IDXDataObject(results_dir=args.input_dir
                                            ).save_to_file(Path(args.output_file))
    process = subparsers.add_parser('process',
                                    help='Process all results into a single summary.')
    process.add_argument('--input-dir', '-i', default='./benchout',
                         help='Where the raw results live.')
    process.add_argument('--output-file', '-o', default='report.json',
                         help='Where to output the report')
    process.set_defaults(fn=process_fn)

    # For summaries!
    summary_fn = lambda args: IDXDataObject(file_path=Path(args.input_file)
                                       ).summary(args.output_file, args.final)
    summary = subparsers.add_parser('summary',
                                    help='Display relavant results')
    summary.add_argument('--input-file', '-i', default='report.json',
                         help='Where the aggregations live.')
    summary.add_argument('--output-file', '-o', default=None,
                         help='Dump summary to TeX file.')
    summary.add_argument('--final', '-f', action='store_true',
                         help='If dumping to TeX, remove the tentative tags.')
    summary.set_defaults(fn=summary_fn)

    # For graphing!
    graph = subparsers.add_parser('graph',
                                  help='Graph from report.json')
    IDXGrapher.add_parser_args(graph)

################################################################################

def main():
    parser = ArgumentParser(description='Process results from automate.py.')
    add_args(parser)

    args = parser.parse_args()
    args.fn(args)

if __name__ == '__main__':
    exit(main())
