#!env python3

import json
import os
import sys
import itertools
import argparse
import sys

from summarize import summarize

parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--summarize', type=str,
                   help='summarize results')
group.add_argument('--graph-trace', type=str,
                   help='graph trace')
args = parser.parse_args()

if args.summarize:
    summarize(args.summarize, sys.stdout)
