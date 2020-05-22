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

from FileBenchProcess import FileBenchRunner
from FragTestProcess import FragTestRunner
from MTCCProcess import MTCCRunner
from YCSBCProcess import YCSBCRunner
#from ConcurrencyProcess import ConcurrencyRunner

from Utils import *


################################################################################
# Main execution.
################################################################################
def add_arguments(parser):
    subparsers = parser.add_subparsers()


    mtcc = subparsers.add_parser('mtcc')
    mtcc.set_defaults(cls=MTCCRunner)
    MTCCRunner.add_arguments(mtcc)

    ycsbc = subparsers.add_parser('ycsbc')
    ycsbc.set_defaults(cls=YCSBCRunner)
    YCSBCRunner.add_arguments(ycsbc)

    #concurrency = subparsers.add_parser('concurrency')
    #concurrency.set_defaults(cls=ConcurrencyRunner)
    #ConcurrencyRunner.add_arguments(concurrency)

    #filebench = subparsers.add_parser('filebench')
    #filebench.set_defaults(cls=FileBenchRunner)
    #FileBenchRunner.add_arguments(filebench)

    #fragtest = subparsers.add_parser('fragtest')
    #fragtest.set_defaults(cls=FragTestRunner)
    #FragTestRunner.add_arguments(fragtest)



    '''
    all_cmds = []
    leveldb = subparsers.add_parser('leveldb')
    leveldb.set_defaults(fn=cls._run_leveldb)
    leveldb.add_argument('--db_size', type=int, default=300000,
                         help='number of KV pairs in final DB.')
    leveldb.add_argument('values_sizes', type=int, nargs='+',
                         help='What value sizes to use')
    all_cmds += [leveldb]

    for sub in all_cmds:
        cls._add_common_arguments(sub)
    '''

def main():
    parser = ArgumentParser(description='Automate running benchmarks.')
    add_arguments(parser)

    args = parser.parse_args()

    runner = args.cls(args)
    runner.run()

if __name__ == '__main__':
    main()
