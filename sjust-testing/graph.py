#!env python3

import numpy as np
import matplotlib.pyplot as plt
from traces import get_state_names

FEATURES = {
      'time': (lambda e: e.get_start(), float, 's')
    , 'duration': (lambda e: e.get_duration(), float, 's')
    , 'latency': (lambda e: e.get_latency(), float, 's')
    , 'transaction_bytes': (lambda e: e.get_param('transaction_bytes'), int, 'bytes')
    , 'transaction_ios': (lambda e: e.get_param('transaction_ios'), int, 'ios')
    , 'total_pending_bytes': (lambda e: e.get_param('total_pending_bytes'), int,
                              'bytes')
    , 'total_pending_ios': (lambda e: e.get_param('total_pending_ios'), int, 'ios')
    , 'total_pending_kv': (lambda e: e.get_param('total_pending_kv'), int, 'ios')
}

for state in get_state_names():
    name = 'state_' + state + '_duration'
    FEATURES[name] = ((lambda s: lambda e: e.get_state_duration(s))(state), float, 's')

def generate_throughput(t):
    pass

SECONDARY_FEATURES = {
    'throughput': (('time'), 's', generate_throughput),
    'kv_queued_and_submitted': (
        ('state_kv_submitted_duration', 'state_kv_queued_duration'),
        's',
        float,
        lambda x, y: x + y),
    'total_pending_deferred': (
        ('total_pending_ios', 'total_pending_kv'),
        'ios',
        int,
        lambda s, t: s - t)
}

def get_unit(feat):
    if feat in FEATURES:
        return FEATURES[feat][2]
    elif feat in SECONDARY_FEATURES:
        return SECONDARY_FEATURES[feat][1]
    else:
        assert False, "{} isn't a valid feature".format(feat)

def get_dtype(feat):
    if feat in FEATURES:
        return FEATURES[feat][1]
    elif feat in SECONDARY_FEATURES:
        return SECONDARY_FEATURES[feat][2]
    else:
        assert False, "{} isn't a valid feature".format(feat)


def get_features(to_graph):
    s = set()
    gmap = {}
    for row in to_graph:
        for g in row:
            for ax in g:
                if ax in FEATURES:
                    s.add(ax)
                    gmap[ax] = (lambda name: (lambda x: x[name]))(ax)
                elif ax in SECONDARY_FEATURES:
                    pfeat = list(SECONDARY_FEATURES[ax][0])
                    for f in pfeat:
                        s.add(f)
                    gmap[ax] = (lambda name, pf: lambda x: SECONDARY_FEATURES[name][3](
                        *[x[feat] for feat in pf]))(ax, pfeat)
                else:
                    assert False, "Invalid feature {}".format(ax)
    return s, gmap

def to_arrays(pfeats, events):
    arrays = [(pfeat, FEATURES[pfeat][0], FEATURES[pfeat][1], [])
              for pfeat in pfeats]

    count = 0
    SIZE = 4096

    for event in events:
        if (count % SIZE == 0):
            for pfeat, _, dtype, l in arrays:
                if count == 0: print(pfeat, dtype)
                l.append(np.zeros(SIZE, dtype=dtype))

        offset = count % SIZE
        for _, f, _, l in arrays:
            l[-1][offset] = f(event)

        count += 1

    last_size = count % SIZE
    for _, _, _, l in arrays:
        l[-1] = l[-1][:last_size]
        
    return dict(((feat, np.concatenate(l).ravel()) for feat, _, _, l in arrays))

TO_GRAPH = [
    [('time', 'latency'), ('total_pending_deferred', 'latency')],
    [('total_pending_ios', 'latency'), ('state_kv_queued_duration', 'latency')],
    [('kv_queued_and_submitted', 'latency'), ('state_kv_submitted_duration', 'latency')]
]

def graph(events):
    pfeat, feat_to_array = get_features(TO_GRAPH)

    cols = to_arrays(pfeat, events)

    for feat, arr in cols.items():
        print(feat, arr.dtype)

    arrays = dict(((feat, t(cols)) for feat, t in feat_to_array.items()))

    for feat, arr in arrays.items():
        print(feat, arr.dtype)

    fig, axs = plt.subplots(nrows=len(TO_GRAPH), ncols=len(TO_GRAPH[0]))

    for nrow in range(len(TO_GRAPH)):
        for ncol in range(len(TO_GRAPH[0])):
            ax = axs[nrow][ncol]
            xname, yname = TO_GRAPH[nrow][ncol]
            xunit = get_unit(xname)
            yunit = get_unit(yname)
            ax.set_xlabel("{name} ({unit})".format(name=xname, unit=xunit))
            ax.set_ylabel("{name} ({unit})".format(name=yname, unit=yunit))
            ax.plot(arrays[xname], arrays[yname], '.')

    plt.show()
