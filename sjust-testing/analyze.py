#!env python3

import json
import os
import sys
import itertools

input = json.load(sys.stdin)

match = {
    'target_device': 'hdd',
    'bs': 4
}

def do_filter(match, input):
    def cond(x):
        return all(x['config'].get(k) == v for k, v in match.items())
    return filter(cond, input)

filtered = do_filter(match, input)

def config_to_frozen(config, match):
    ret = dict(filter(lambda x: x[0] not in match, config.items()))
    if 'run' in ret:
        del ret['run']
    return frozenset(sorted(ret.items()))

def group_by_config(input):
    grouped = {}
    for run in filtered:
        key = config_to_frozen(run['config'], match)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(run)
    return [{'config': dict(list(k)), 'runs': v} for k, v in grouped.items()]

grouped = group_by_config(filtered)

def union_top_n(group):
    ret = set()
    for run in group:
        ret = ret.union(
            [k for v, k in sorted(((a, b) for b, a in run['perf'].items()))][::-1][:5]
        )
    return ret

def project_run(perfs):
    def ret(run):
        return {
            'tp': run['fio']['write']['iops_mean'],
            'lat': run['fio']['write']['clat_mean_ns'] / 1000000000.0,
            'slat': run['fio']['write']['slat_mean_ns'] / 1000000000.0,
            'perf': dict(filter(lambda x: x[0] in perfs, run['perf'].items()))
            }
    return ret

def sort_by(f, input):
    return [v for (k, v) in sorted(map(lambda x: (f(x), x), input))]

def project_group(group):
    perfs = union_top_n(group['runs'])
    return {
        'config': group['config'],
        'runs': sort_by(
            lambda x: x['tp'],
            list(map(project_run(perfs), group['runs'])))
        }
        
output = sort_by(lambda x: x['config']['qd'], list(map(project_group, grouped)))

json.dump(output, sys.stdout, sort_keys=True, indent=2)
