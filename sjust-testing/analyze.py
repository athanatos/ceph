#!env python3

import json
import os
import sys
import itertools
import argparse
import sys

from summarize import dump_target, generate_summary
from traces import open_trace, iterate_structured_trace
from graph import graph

parser = argparse.ArgumentParser()
parser.add_argument('target', metavar='T', type=str, help='target results directory')
parser.add_argument('--match', type=str, help='json for matching', default='{}')
parser.add_argument('--output', type=str, help='output directory')
parser.add_argument('--generate-graphs', type=bool, help='generate graphs')
parser.add_argument('--drop-first', type=float,
                    help='drop', default=10.0)
parser.add_argument('--drop-after', type=float,
                    help='drop')


def get_targets(directory):
    contents = os.listdir(directory)
    if 'ceph.conf' in contents:
        return [directory]
    else:
        return [(x, os.path.join(directory, x)) for x in contents]


args = parser.parse_args()

match = json.loads(args.match)
targets = get_targets(args.target)
projected = [dump_target(name, target) for name, target in targets]

def do_filter(match, input):
    def cond(x):
        return all(x[1]['config'].get(k) == v for k, v in match.items())
    return filter(cond, input)

filtered_targets, filtered = zip(*do_filter(match, zip(targets, projected)))

summary = generate_summary(filtered, match)

if args.generate_graphs:
    for name, path in filtered_targets:
        events = iterate_structured_trace(open_trace(path))
        if args.drop_first:
            events = itertools.dropwhile(lambda x: x.get_start() < args.drop_first, events)
        if args.drop_after:
            events = itertools.takewhile(lambda x: x.get_start() < args.drop_after, events)
        graph(events)

json.dump(summary, sys.stdout, sort_keys=True, indent=2)
