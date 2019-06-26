#!env python3

import babeltrace
import sys
import json
import os

def get_state_name(state):
    return {
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
    }[state]

def open_trace(rdir):
    tdir_prefix = os.path.join(rdir, 'trace/ust/uid')
    uid = os.listdir(tdir_prefix)[0]
    ret = babeltrace.TraceCollection()
    ret.add_trace(os.path.join(tdir_prefix, uid, '64-bit'), 'ctf')
    return ret

def event_id(event):
    assert 'sequencer_id' in event.keys()
    assert 'tid' in event.keys()
    return (event['sequencer_id'], event['tid'])

def filter_initial(event):
    not_initial = frozenset([
        'cpu_id', 'stream_id', 'id', 'events_discarded',
        'timestamp_end', 'packet_size', 'sequencer_id', 'packet_seq_num',
        'uuid', 'content_size', 'stream_instance_id',
        'v', 'timestamp_begin', 'tid', 'magic'])
    return dict((
        (k, event[k]) for k in
        filter(lambda x: x not in not_initial, event.keys())
    ))

class Write(object):
    start = None
    def __init__(self, event_id):
        self.__id = event_id
        self.__state_durations = {}
        self.__start = None
        self.__duration = None
        self.__initial_params = {}

    def consume_event(self, event):
        assert event_id(event) == self.__id
        if event.name == 'bluestore:transaction_initial_state':
            assert self.__start is None
            self.__initial_params = filter_initial(event)
            if Write.start is None:
                Write.start = event.timestamp
                self.__start = 0
            else:
                self.__start = (event.timestamp - Write.start) / (1000000000.0)
            return False
        elif event.name == 'bluestore:transaction_total_duration':
            assert self.__duration is None
            self.__duration = event['elapsed'] / 1000000.0
            return True
        elif event.name == 'bluestore:transaction_state_duration':
            self.__state_durations[get_state_name(event['state'])] = \
                event['elapsed']
            return False
        else:
            assert False, "{} not a valid event".format(event.name)
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

def iterate_structured_trace(trace):
    live = {}
    for event in trace.events:
        eid = event_id(event)
        if eid not in live:
            live[eid] = Write(eid)
        if live[eid].consume_event(event):
            yield live[eid].to_primitive()
            del live[eid]

    for event in events_to_structured(trace):
        yield f(event)

def dump_structured_trace(tdir, fd):
    trace = open_trace(tdir)
    for p in iterate_structured_trace(trace):
        json.dump(p, fd, sort_keys=True, indent=2)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--dump-structured-trace', type=str,
                        help='generate json dump of writes')
    args = parser.parse_args()
    
    dump_structured_trace(args.dump_structured_trace, sys.stdout)
