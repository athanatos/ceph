#!env python3

import numpy as np
import matplotlib.pyplot as plt
from traces import get_state_names

FEATURES = {
      'time': (lambda e: e.get_start(), float, 's')
    , 'latency': (lambda e: e.get_duration(), float, 's')
    , 'transaction_bytes': (lambda e: e.get_param('transaction_bytes'), int, 'bytes')
    , 'transaction_ios': (lambda e: e.get_param('transaction_ios'), int, 'ios')
    , 'total_pending_bytes': (lambda e: e.get_param('total_pending_bytes'), int,
                              'bytes')
    , 'total_pending_ios': (lambda e: e.get_param('total_pending_ios'), int, 'ios')
    , 'total_pending_kv': (lambda e: e.get_param('total_pending_kv'), int, 'ios')
}

for state in get_state_names():
    name = 'state_' + state + '_duration'
    FEATURES[name] = (name, float, lambda e: e.get_state_duration(state))

def generate_throughput(t):
    pass

SECONDARY_FEATURES = {
    'throughput': (('time'), 's', generate_throughput),
    'total_pending_deferred': (
        ('total_pending_ios', 'total_pending_kv'),
        'ios',
        lambda t, k: t - k)
}

def get_unit(feat):
    if feat in FEATURES:
        return FEATURES[feat][2]
    elif feat in SECONDARY_FEATURES:
        return SECONDARY_FEATURES[feat][1]
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
                    gmap[ax] = (lambda name: lambda x: SECONDARY_FEATURES[name][2](
                        *[x[feat] for feat in pfeat]))(ax)
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
            for _, _, dtype, l in arrays:
                l.append(np.empty(SIZE, dtype=dtype))

        offset = count % SIZE
        for _, f, _, l in arrays:
            l[-1][offset] = f(event)

        count += 1

    last_size = count % SIZE
    for _, _, _, l in arrays:
        l[-1] = l[-1][:last_size]
        
    return dict(((feat, np.ravel(l)) for feat, _, _, l in arrays))

TO_GRAPH = [
    [('time', 'latency'), ('transaction_bytes', 'latency')],
    [('total_pending_deferred', 'latency'), ('transaction_bytes', 'latency')]
]

def graph(events):
    pfeat, feat_to_array = get_features(TO_GRAPH)

    cols = to_arrays(pfeat, events)

    arrays = dict(((feat, t(cols)) for feat, t in feat_to_array.items()))

    fig, axs = plt.subplots(nrows=len(TO_GRAPH), ncols=len(TO_GRAPH[0]))

    for nrow in range(len(TO_GRAPH)):
        for ncol in range(len(TO_GRAPH[0])):
            ax = axs[nrow][ncol]
            xname, yname = TO_GRAPH[nrow][ncol]
            xunit = get_unit(xname)
            yunit = get_unit(yname)
            ax.set_xlabel("{name} ({unit})".format(name=xname, unit=xunit))
            ax.set_ylabel("{name} ({unit})".format(name=yname, unit=yunit))
            ax.scatter(arrays[xname], arrays[yname])

    plt.show()
