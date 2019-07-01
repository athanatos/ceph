#!env python3

import numpy as np
import matplotlib
import matplotlib.figure
from traces import get_state_names
import matplotlib.backends
import matplotlib.backends.backend_pdf as backend

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
    'prepare_kv_queued_and_submitted': (
        ('state_prepare_duration', 'state_kv_submitted_duration', 'state_kv_queued_duration'),
        's',
        float,
        lambda x, y, z: x + y + z),
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
    [('prepare_kv_queued_and_submitted', 'latency'), ('state_kv_submitted_duration', 'latency')],
    [('total_pending_kv', 'latency'), ('state_prepare_duration', 'latency')],
    [('total_pending_deferred', 'state_prepare_duration'), ('total_pending_kv', 'state_prepare_duration')]
]

FONTSIZE=4
matplotlib.rcParams.update({'font.size': FONTSIZE})

def graph(events, name, path):
    pfeat, feat_to_array = get_features(TO_GRAPH)

    cols = to_arrays(pfeat, events)

    for feat, arr in cols.items():
        print(feat, arr.dtype)

    arrays = dict(((feat, t(cols)) for feat, t in feat_to_array.items()))

    for feat, arr in arrays.items():
        print(feat, arr.dtype)

    fig = matplotlib.figure.Figure()
    fig.suptitle(name)
    fig.set_figwidth(8)
    fig.set_figheight(11)

    rows = len(TO_GRAPH)
    cols = len(TO_GRAPH[0])
    for nrow in range(rows):
        for ncol in range(cols):
            index = (nrow * cols) + ncol + 1
            ax = fig.add_subplot(rows, cols, index)
            xname, yname = TO_GRAPH[nrow][ncol]
            xunit = get_unit(xname)
            yunit = get_unit(yname)
            ax.set_xlabel(
                "{name} ({unit})".format(name=xname, unit=xunit),
                fontsize=FONTSIZE
            )
            ax.set_ylabel(
                "{name} ({unit})".format(name=yname, unit=yunit),
                fontsize=FONTSIZE)
            ax.plot(
                arrays[xname], arrays[yname], '.', markersize=0.4,
                rasterized=True)

    fig.subplots_adjust(left=0.05, right=0.98, bottom=0.03, top=0.95)

    if path is not None:
        fig.set_canvas(backend.FigureCanvas(fig))
        fig.savefig(
            path,
            dpi=600,
            orientation='portrait',
            papertype='letter',
            format='pdf')
    else:
        import matplotlib.pyplot as plt
        plt.show()
