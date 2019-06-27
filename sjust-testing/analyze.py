#!env python3

import json
import os
import sys
import itertools
import argparse
import sys

from summarize import summarize
from traces import open_trace, iterate_structured_trace
from graph import graph
import cProfile

parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--summarize', type=str,
                   help='summarize results')
group.add_argument('--graph-trace', type=str,
                   help='graph trace')
group.add_argument('--iterate-traces', type=str,
                   help='graph trace')
args = parser.parse_args()

def iterate(path):
    for _ in iterate_structured_trace(open_trace(path)):
        pass
    

if args.summarize:
    summarize(args.summarize, sys.stdout)
elif args.graph_trace:
    graph(iterate_structured_trace(open_trace(args.graph_trace)))
elif args.iterate_traces:
    cProfile.run('iterate("' + args.iterate_traces + '")')
