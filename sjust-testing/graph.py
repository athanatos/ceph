#!env python3

import numpy as np
import matplotlib
import matplotlib.figure
from traces import Write
import matplotlib.backends
import matplotlib.backends.backend_pdf as backend
from scipy import interpolate

FEATURES = Write.get_features()

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
    'kv_sync_size': (
        ('kv_batch_size', 'deferred_done_batch_size', 'deferred_stable_batch_size'),
        'n',
        int,
        lambda x, y, z: x + y + z),
    'deferred_batch_size': (
        ('deferred_done_batch_size', 'deferred_stable_batch_size'),
        'n',
        int,
        lambda x, y: x + y),
    'incomplete_ios': (
        ('total_pending_ios', 'total_pending_deferred_ios'),
        'n',
        int,
        lambda x, y: x + y),
    'committing_state': (
        ('state_prepare_duration', 'state_kv_queued_duration', 'state_kv_submitted_duration'),
        's',
        int,
        lambda x, y, z: x + y + z),
    'commit_latency_no_throttle': (
        ('commit_latency', 'throttle_time'),
        's',
        int,
        lambda x, y: x - y),
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
        for name, f, _, l in arrays:
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
        bins, x_e, y_e = np.histogram2d(x, y, bins=1000, weights=w)
        z = interpolate.interpn(
            (0.5*(x_e[1:] + x_e[:-1]) , 0.5*(y_e[1:]+y_e[:-1])),
            bins,
            np.vstack([x,y]).T,
            method = "nearest",
            fill_value = None,
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
            x[idx], y[idx],
            c=z[idx],
            s=1,
            rasterized=True)

        per_point = 1000
        xsortidx = x.argsort()
        xs = x[xsortidx]
        ys = y[xsortidx]
        lines = [(x, []) for x in [50, 95]]
        limits = []
        idx = 0
        while idx < len(xs):
            limit = max(len(xs), idx + per_point)
            vals = ys[idx:limit]
            limits.append((xs[idx] + xs[limit]) / 2.0)
            for t, d in lines:
                d.append(np.percentile(vals, t))
            idx = limit
        for t, d in lines:
            ax.plot(limits, d, 'go--', linewidth=2, markersize=12)

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
        ax.hist(p, weights=w, bins=50)

    def name(self):
        return "Histogram({})".format(self.__param)

TO_GRAPH2 = [
    [Scatter(*x) for x in
     [('time', 'commit_latency'), ('time', 'throughput'), ('time', 'committing_state')]],
    [Histogram(x) for x in
     ['commit_latency', 'kv_sync_latency', 'committing_state']],
    [Scatter('time', x) for x in
     ['state_prepare_duration', 'state_kv_queued_duration', 'state_kv_submitted_duration']],
    [Histogram(x) for x in
     ['state_prepare_duration', 'state_kv_queued_duration', 'state_kv_submitted_duration']],
    [Scatter('time', x) for x in
     ['kv_submit_latency', 'kv_sync_latency',
      'kv_sync_size']],
    [Scatter(x, 'kv_sync_latency') for x in
     ['kv_sync_size', 'deferred_batch_size', 'kv_batch_size']],
    [Histogram(x) for x in
     ['kv_sync_size', 'deferred_batch_size', 'kv_batch_size']],
]

TO_GRAPH = [
    [Scatter(*x) for x in
     [('current_kv_throttle_cost', 'throughput'),
      ('current_kv_throttle_cost', 'commit_latency_no_throttle')]],
    [Scatter(*x) for x in
     [('current_deferred_throttle_cost', 'throughput'),
      ('current_deferred_throttle_cost', 'commit_latency_no_throttle')]],
    [Scatter(*x) for x in
     [('time', 'throughput'),
      ('time', 'commit_latency_no_throttle')]],
    [Scatter(*x) for x in
     [('throughput', 'commit_latency_no_throttle'),
      ('current_deferred_throttle_cost', 'commit_latency_no_throttle')]]
]


FONTSIZE=4
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
            papertype='legal',
            format='pdf')
    else:
        import matplotlib.pyplot as plt
        plt.show()
