#!env python3

import babeltrace
import sys
import json
import os
import subprocess
import re
import datetime

STATES = {
    19: "prepare",
    20: "aio_wait",
    21: "io_done",
    22: "kv_queued",
    23: "kv_submitted",
    24: "kv_done",
    25: "deferred_queued",
    26: "deferred_cleanup",
    27: "deferred_done",
    28: "finishing",
    29: "done"
}

def get_state_name(state):
    return STATES[state]

def get_state_names():
    return [x[1] for x in STATES.items()]

ROCKSDB_FEATURES = [
    'rocksdb_base_level',
    'rocksdb_estimate_pending_compaction_bytes',
    'rocksdb_cur_size_all_mem_tables',
    'rocksdb_compaction_pending',
    'rocksdb_mem_table_flush_pending',
    'rocksdb_num_running_compactions',
    'rocksdb_num_running_flushes',
    'rocksdb_actual_delayed_write_rate'
]

TYPE_MAP = {
    'sequencer_id': int,
    'tid': int,
    'elapsed': float,
    'state': int,
    'transaction_bytes': int,
    'transaction_ios': int,
    'total_pending_ios': int,
    'total_pending_deferred_ios': int,
    'total_pending_bytes': int,
    'total_pending_kv': int,
    'throughput': float,
    'weight': float
}

for feat in ROCKSDB_FEATURES:
    TYPE_MAP[feat] = int

def get_rocksdb_features():
    return ROCKSDB_FEATURES

def get_type(field):
    return TYPE_MAP.get(field)

def event_id(event):
    #assert 'sequencer_id' in event.keys()
    #assert 'tid' in event.keys()
    return (event['sequencer_id'], event['tid'])

def open_trace(rdir):
    tdir_prefix = os.path.join(rdir, 'trace/ust/uid')
    uid = os.listdir(tdir_prefix)[0]
    ret = babeltrace.TraceCollection()
    ret.add_trace(os.path.join(tdir_prefix, uid, '64-bit'), 'ctf')
    return ret.events

class Event(object):
    def __init__(self, name, timestamp, properties):
        self.name = name
        self.timestamp = timestamp
        self.__properties = properties

    def __getitem__(self, key):
        return self.__properties.get(key)

    def __str__(self):
        return "Event(name: {name}, timestamp: {timestamp}, {properties})".format(
            name=self.name,
            timestamp=self.timestamp,
            properties=self.__properties)

DATE = '(\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d\.\d\d\d\d\d\d\d\d\d)'
OFFSET = '\(\+(\?\.\?+|\d\.\d\d\d\d\d\d\d\d\d)\)'
NAME = 'bluestore:[a-z_]*'
PAIRS = '{((?: [a-z_]+ = [0-9.e+]+[ ,])+)}'
RE = re.compile(
    '\[' + DATE + '\] [a-z0-9]* (?P<name>' + NAME + '): { \d* }, ' + PAIRS
    )
def parse(line):
    res = RE.match(line)
    if not res:
        print(line)
    assert res
    groups = res.groups()
    start = datetime.datetime.strptime(groups[0][:-3], "%Y-%m-%d %H:%M:%S.%f")
    name = groups[1]
    props = {}
    for pair in (x.strip() for x in groups[2].split(',')):
        k, v = pair.split('=')
        k = k.strip()
        props[k] = get_type(k)(v.strip())
    return Event(name, start.timestamp(), props)

def test():
    test = '[20:01:49.773714486] (+?.?????????) incerta05 bluestore:transaction_initial_state: { 22 }, { sequencer_id = 1, tid = 1, transaction_bytes = 399, transaction_ios = 1, total_pending_bytes = 399, total_pending_ios = 1, total_pending_kv = 1 }'
    test2 = '[20:01:49.774633030] (+0.000918544) incerta05 bluestore:transaction_state_duration: { 22 }, { sequencer_id = 1, tid = 1, state = 19, elapsed = 4859 }'
    parse(test)
    parse(test2)

def open_trace(rdir):
    tdir_prefix = os.path.join(rdir, 'trace/')
    CMD = ['babeltrace', '--no-delta', '--clock-date', '-n', 'payload',
           tdir_prefix]
    proc = subprocess.Popen(
        CMD,
        bufsize=524288,
        stdout=subprocess.PIPE,
        stderr=sys.stderr)
    for line in proc.stdout.readlines():
        yield parse(line.decode("utf-8"))

def filter_initial(event):
    initial = [
        'transaction_bytes',
        'transaction_ios',
        'total_pending_deferred_ios',
        'total_pending_ios',
        'total_pending_bytes',
        'total_pending_kv',
        'throughput',
        'weight',
    ]
    return dict(((k, event[k]) for k in initial))

def filter_initial_rocksdb(event):
    return dict(((k, event[k]) for k in ROCKSDB_FEATURES))

class Write(object):
    start = None
    def __init__(self, event_id):
        self.__id = event_id
        self.__state_durations = {}
        self.__start = None
        self.__duration = None
        self.__commit_latency = None
        self.__initial_params = {}

    def consume_event(self, event):
        #assert event_id(event) == self.__id
        if event.name == 'bluestore:transaction_initial_state':
            assert self.__start is None
            self.__initial_params = filter_initial(event)
            if self.__initial_params['transaction_bytes'] > 30000:
                print("Got invalid event {}".format(event))
                
            assert self.__initial_params['transaction_bytes'] < 30000
            if Write.start is None:
                Write.start = event.timestamp
                self.__start = 0
            else:
                self.__start = event.timestamp - Write.start
            return False
        elif event.name == 'bluestore:transaction_initial_state_rocksdb':
            self.__initial_params.update(filter_initial_rocksdb(event))
            return False
        elif event.name == 'bluestore:transaction_total_duration':
            assert self.__duration is None
            self.__duration = event['elapsed'] / 1000000.0
            return True
        elif event.name == 'bluestore:transaction_commit_latency':
            assert self.__commit_latency is None
            self.__commit_latency = event['elapsed'] / 1000000.0
            return False
        elif event.name == 'bluestore:transaction_state_duration':
            self.__state_durations[get_state_name(event['state'])] = \
                event['elapsed'] / 1000000.0
            return False
        else:
            assert False, "{} not a valid event".format(event)
            return True
            
    def to_primitive(self):
        return {
            'id': {
                'sequencer_id': self.__id[0],
                'tid': self.__id[1],
                },
            'state_durations': self.__state_durations,
            'start': self.__start,
            'duration': self.__duration,
            'initial_params': self.__initial_params
        }

    def get_start(self):
        assert self.__start is not None
        return self.__start

    def get_duration(self):
        return self.__duration

    def get_latency(self):
        return self.__commit_latency

    def get_param(self, param):
        if param not in self.__initial_params:
            print("{} not in {}".format(param, self.__initial_params))
        assert param in self.__initial_params
        return self.__initial_params[param]

    def get_state_duration(self, state):
        return self.__state_durations.get(state, 0)

def iterate_structured_trace(trace):
    live = {}
    count = 0
    Write.start = None
    last = 0.0
    for event in trace:
        eid = event_id(event)
        if eid not in live:
            live[eid] = Write(eid)
        if live[eid].consume_event(event):
            count += 1
            y = live[eid]
            del live[eid]
            if y.get_start() > last + 10:
                last = y.get_start()
                print("Trace processed up to {}s".format(int(y.get_start())))
            yield y

def dump_structured_trace(tdir, fd):
    trace = open_trace(tdir)
    for p in iterate_structured_trace(trace):
        json.dump(p.to_primitive(), fd, sort_keys=True, indent=2)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dump-structured-trace', type=str,
                        help='generate json dump of writes')
    args = parser.parse_args()
    
    dump_structured_trace(args.dump_structured_trace, sys.stdout)
