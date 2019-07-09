#!env python3

import numpy as np
import matplotlib
import matplotlib.figure
from traces import get_state_names
import matplotlib.backends
import matplotlib.backends.backend_pdf as backend
from scipy import interpolate

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
    , 'weight': (lambda e: e.get_param('weight'), float, 'ratio')
    , 'throughput': (lambda e: e.get_param('throughput'), float, 'iops')
    , 'total_pending_kv': (lambda e: e.get_param('total_pending_kv'), int, 'ios')
    , 'rocksdb_base_level': (
        lambda e: e.get_param('rocksdb_base_level'), int, 'ios')
    , 'rocksdb_estimate_pending_compaction_bytes': (
        lambda e: e.get_param('rocksdb_estimate_pending_compaction_bytes'), int, 'ios')
    , 'rocksdb_cur_size_all_mem_tables': (
        lambda e: e.get_param('rocksdb_cur_size_all_mem_tables'), int, 'ios')
}

for state in get_state_names():
    name = 'state_' + state + '_duration'
    FEATURES[name] = ((lambda s: lambda e: e.get_state_duration(s))(state), float, 's')

def generate_throughput(start, d):
    P = 1.0
    finish = start + d
    tp = np.empty_like(start)
    fi = 0
    for i in range(len(start)):
        while finish[fi] < start[i]:
            fi += 1
        count = 0
        while fi + count < len(finish) and finish[fi + count] < start[i] + P:
            count += 1
        if count >= len(finish) - fi:
            count = len(finish) - fi - 1
        if finish[fi + count] - start[i] == 0:
            tp[i] = 0
        else:
            tp[i] = count / (finish[fi + count] - start[i])
    return tp

SECONDARY_FEATURES = {
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


def get_features(features):
    s = set()
    gmap = {}
    for ax in features:
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
                l.append(np.zeros(SIZE, dtype=dtype))

        offset = count % SIZE
        for _, f, _, l in arrays:
            l[-1][offset] = f(event)

        count += 1

    last_size = count % SIZE
    for name, _, _, l in arrays:
        l[-1] = l[-1][:last_size]
        
    return dict(((feat, np.concatenate(l).ravel()) for feat, _, _, l in arrays))

class Graph(object):
    def sources(self):
        pass

    def graph(self, ax, *sources):
        pass

    def name(self):
        pass

class Scatter(Graph):
    def __init__(self, x, y):
        self.__sources = [x, y]
        self.__xname = x
        self.__yname = y
        self.__xunit = get_unit(x)
        self.__yunit = get_unit(y)

    def sources(self):
        return ['weight', self.__xname, self.__yname]

    def graph(self, ax, w, x, y):
        bins, x_e, y_e = np.histogram2d(x, y, bins=100, weights=w)
        z = interpolate.interpn(
            (0.5*(x_e[1:] + x_e[:-1]) , 0.5*(y_e[1:]+y_e[:-1])),
            bins,
            np.vstack([x,y]).T,
            method = "splinef2d",
            bounds_error = False)

        idx = z.argsort()
            
        ax.set_xlabel(
            "{name} ({unit})".format(name=self.__xname, unit=self.__xunit),
            fontsize=FONTSIZE
        )
        ax.set_ylabel(
            "{name} ({unit})".format(name=self.__yname, unit=self.__yunit),
            fontsize=FONTSIZE)
        ax.scatter(
            x[idx], y[idx], c=z[idx], s=1,
            rasterized=True)

    def name(self):
        return "Scatter({}, {})".format(self.__xname, self.__yname)


class Histogram(Graph):
    def __init__(self, p):
        self.__param = p
        self.__unit = get_unit(p)
        
    def sources(self):
        return ['weight', self.__param]

    def graph(self, ax, w, p):
        ax.set_xlabel(
            "{name} ({unit})".format(name=self.__param, unit=self.__unit),
            fontsize=FONTSIZE
        )
        ax.set_ylabel(
            "N",
            fontsize=FONTSIZE)
        ax.hist(p, weights=w, bins=100)

    def name(self):
        return "Histogram({})".format(self.__param)

TO_GRAPH = [
    [Scatter(*x) for x in [('time', 'latency'), ('time', 'throughput'), ('throughput', 'latency')]],
    [Histogram(x) for x in ['latency', 'throughput', 'total_pending_kv']],
    [Scatter(x, 'latency') for x in ['total_pending_kv', 'total_pending_ios', 'total_pending_deferred']],
    [Scatter(x, 'throughput') for x in ['total_pending_kv', 'total_pending_ios', 'total_pending_deferred']],
]

FONTSIZE=6
matplotlib.rcParams.update({'font.size': FONTSIZE})

def graph(events, name, path, mask_params=None, masker=None):
    if mask_params is None:
        mask_params = []
    features = set([ax for row in TO_GRAPH for g in row for ax in g.sources()]
                   + mask_params)
    pfeat, feat_to_array = get_features(features)

    cols = to_arrays(pfeat, events)

    print("Generated arrays")

    arrays = dict(((feat, t(cols)) for feat, t in feat_to_array.items()))

    if masker is not None:
        mask = masker(*[arrays[x] for x in mask_params])
        arrays = dict(((feat, ar[mask]) for feat, ar in arrays.items()))

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
            grapher = TO_GRAPH[nrow][ncol]
            grapher.graph(
                ax,
                *[arrays.get(n) for n in grapher.sources()])

            print("Generated subplot {}".format(grapher.name()))

    fig.subplots_adjust(left=0.08, right=0.98, bottom=0.03, top=0.95)

    if path is not None:
        fig.set_canvas(backend.FigureCanvas(fig))
        print("Generating image")
        fig.savefig(
            path,
            dpi=1200,
            orientation='portrait',
            papertype='letter',
            format='pdf')
    else:
        import matplotlib.pyplot as plt
        plt.show()
