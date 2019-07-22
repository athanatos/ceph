#!env python3

import os
import subprocess
import json
import time
import sys

BLUESTORE_CONF = """
[global]
	debug bluestore = 0/0
	debug bluefs = 0/0
	debug bdev = 0/0
	debug rocksdb = 5
	osd pool default pg num = 8
	osd op num shards = 5

[osd]
	osd objectstore = bluestore

	# use directory= option from fio job file
	osd data = {target_dir}
	osd journal = {target_dir}/journal
        bluestore block path = {block_device}

        bluestore rocksdb options = compression=kNoCompression,max_write_buffer_number=4,min_write_buffer_number_to_merge=1,recycle_log_file_num=4,write_buffer_size=268435456,writable_file_max_buffer_size=0,compaction_readahead_size=2097152,stats_dump_period_sec=10

	# log inside fio_dir
	log file = {output_dir}/log
        bluestore_tracing = true
        bluestore_throttle_trace_rate = 100.0
        bluestore_throttle_bytes = 0
        bluestore_throttle_deferred_bytes = 0
        bluestore prefer deferred size hdd = {hdd_deferred}
        bluestore prefer deferred size ssd = {hdd_deferred}
        bluestore prefer deferred size = {hdd_deferred}
        rocksdb collect extended stats = true
        rocksdb collect memory stats = true
        rocksdb collect compaction stats = true
        rocksdb perf = true
        bluefs_preextend_wal_files = {preextend}
        bluestore_throttle_cost_per_io = {tcio}
"""

def generate_ceph_conf(conf):
    return BLUESTORE_CONF.format(**conf)

BLUESTORE_FIO_BASE = """
[global]
ioengine={lib}/libfio_ceph_objectstore.so

conf={output_dir}/ceph.conf
directory={target_dir}


oi_attr_len=320 # specifies OI(aka '_') attribute length range to couple
                # writes with. Default: 0 (disabled)

snapset_attr_len=35  # specifies snapset attribute length range to couple
                     # writes with. Default: 0 (disabled)
_fastinfo_omap_len=186 # specifies _fastinfo omap entry length range to
                       # couple writes with. Default: 0 (disabled)
pglog_simulation=1   # couples write and omap generation in OSD PG log manner.
                     # Ceph's osd_min_pg_log_entries, osd_pg_log_trim_min,
                     # osd_pg_log_dups_tracked settings control cyclic
                     # omap keys creation/removal.
                     # Following additional FIO pglog_ settings to apply too:

pglog_omap_len=173   # specifies PG log entry length range to couple
                     # writes with. Default: 0 (disabled)

pglog_dup_omap_len=57 # specifies duplicate PG log entry length range
                      # to couple writes with. Default: 0 (disabled)

perf_output_file={output_dir}/perf_counters.json

bluestore_throttle="{bluestore_throttle}"
bluestore_deferred_throttle="{bluestore_deferred_throttle}"
vary_bluestore_throttle_period={vary_bluestore_throttle_period}

rw=randwrite

nrfiles=128000
filesize=4m
"""

BLUESTORE_FIO_POPULATE = """
[write]
bs=4m
rw=write
time_based=0
size=512g
"""

def generate_fio_populate_conf(conf):
    c = conf.copy()
    assert 'block_device' in c
    return (BLUESTORE_FIO_BASE + BLUESTORE_FIO_POPULATE).format(**c)

BLUESTORE_FIO = """
[write]
iodepth={qd}
iodepth_low={qdl}
time_based=1
runtime={runtime}s
bs={bs}k
rw=randwrite
"""

def generate_fio_job_conf(conf):
    c = conf.copy()
    c['qdl'] = max(c['qd'] - c['qdl'], 0)
    for k in ["deferred_", ""]:
        key = "bluestore_" + k + "throttle"
        c[key] = ','.join([str(x * ((c['bs'] * 1024) + (2 * c['tcio']))) for x in c[key]])
    assert 'block_device' in c
    return (BLUESTORE_FIO_BASE + BLUESTORE_FIO).format(**c)


DEFAULT = {
    'output_dir': os.path.join('./output',time.strftime('%Y-%m-%d-%H:%M:%S')),
    'lib': '../build/lib',
    'target_dir': '/mnt/sjust/run',
    'qd': 16,
    'qdl': 0,
    'runtime': 10,
    'bs': 4,
    'fio_bin': '../build/bin/fio',
    'lttng': True,
    'hdd_deferred': 32768,
    'preextend': 'false',
    'bluestore_throttle': [],
    'bluestore_deferred_throttle': [],
    'vary_bluestore_throttle_period': 0,
    'tcio': 670000,
    'clear_target': False
}

def get_fio_fn(base):
    return os.path.join(base, 'bluestore.fio')

def get_fio_populate_fn(base):
    return os.path.join(base, 'bluestore_populate.fio')

def get_ceph_fn(base):
    return os.path.join(base, 'ceph.conf')

def get_fio_output(base):
    return os.path.join(base, 'fio_output.json')

def get_fio_stdout(base):
    return os.path.join(base, 'fio.stdout')

def write_conf(conf):
    fio_fn = get_fio_fn(conf['output_dir'])
    fio_populate_fn = get_fio_populate_fn(conf['output_dir'])
    ceph_fn = get_ceph_fn(conf['output_dir'])
    for fn, func in [(fio_fn, generate_fio_job_conf),
                     (fio_populate_fn, generate_fio_populate_conf),
                     (ceph_fn, generate_ceph_conf)]:
        with open(fn, 'a') as f:
            f.write(func(conf))
    return fio_fn, fio_populate_fn

def setup_start_lttng(conf):
    if not conf.get('lttng', False):
        return
    tracedir = os.path.join(conf['output_dir'], 'trace')
    subprocess.run(['mkdir', tracedir])
    subprocess.run([
        'lttng', 'create', 'fio-bluestore',
        '--output', tracedir
        ])
    for event in ['state_duration', 'total_duration',
                  'initial_state', 'initial_state_rocksdb',
                  'commit_latency', 'kv_sync_latency', 'kv_submit_latency']:
        subprocess.run([
            'lttng', 'enable-event',
            '--session', 'fio-bluestore',
            '--userspace', 'bluestore:transaction_' + event
        ])
    subprocess.run(['lttng', 'start', 'fio-bluestore'])

def stop_destroy_lttng(conf):
    if not conf.get('lttng', False):
        return
    subprocess.run([
        'lttng', 'stop', 'fio-bluestore'
    ], check=False)
    subprocess.run([
        'lttng', 'destroy', 'fio-bluestore'
    ], check=False)

def run_fio(conf, fn):
    env = {
        'LD_LIBRARY_PATH': conf['lib']
    }
    output_json = get_fio_output(conf['output_dir'])
    cmd = [
        conf['fio_bin'],
        fn,
        '--status-interval', '10s',
        '--output', output_json,
        '--output-format', 'json+']
    with open(get_fio_stdout(conf['output_dir']), 'w') as outf:
        subprocess.run(cmd, env=env, stdout=outf, stderr=outf)

def run_conf(conf):
    to_clear = [conf['output_dir']]
    if conf['clear_target']:
        to_clear.append(conf['target_dir'])

    for d in to_clear:
        subprocess.run(['rm', '-rf', d], check=False)
        subprocess.run(['mkdir', '-p', d])
    fio_conf, fio_populate_conf = write_conf(conf)

    if conf['clear_target']:
        run_fio(conf, fio_populate_conf)

    stop_destroy_lttng(conf)
    setup_start_lttng(conf)
    run_fio(conf, fio_conf)
    stop_destroy_lttng(conf)

def get_all_config_combos(configs):
    if len(configs) == 0:
        yield {}
    else:
        key = list(configs.keys())[0]
        vals = configs[key]
        sub = configs.copy()
        del sub[key]
        for val in vals:
            for subconfig in get_all_config_combos(sub):
                subconfig.update({key: val})
                yield subconfig 

def generate_name_full_config(base, run):
    full_config = {}
    full_config.update(DEFAULT)
    full_config.update(base)
    full_config.update(run)
    if ('devices' in full_config.keys() or
        'target_device' in full_config.keys()):
        assert('devices' in full_config.keys())
        assert('target_device' in full_config.keys())
        full_config['block_device'] = \
            full_config['devices'][full_config['target_device']]['device']
        full_config['target_dir'] = \
            full_config['devices'][full_config['target_device']]['target_dir']
    name = "-".join(
        "{name}({val})".format(name=name, val=val)
        for name, val in run.items())
    return name, run, full_config

def get_base_config(base):
    return os.path.join(base, 'base_config.json')

def get_full_config(base):
    return os.path.join(base, 'full_config.json')

def do_run(base, runs):
    ret = {}
    orig_output_dir = None
    def d(obj, fn):
        with open(fn, 'w') as f:
            json.dump(obj, f, sort_keys=True, indent=2)
        
    for name, base_config, full_config in map(
            lambda x: generate_name_full_config(base, x),
            get_all_config_combos(runs)):
        if orig_output_dir is None:
            orig_output_dir = full_config['output_dir']
        full_config['output_dir'] = os.path.join(full_config['output_dir'], name)
        print("Running {name}".format(name=name))
        run_conf(full_config)
        d(base_config, get_base_config(full_config['output_dir']))
        d(full_config, get_full_config(full_config['output_dir']))
    return orig_output_dir


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--run', type=str,
                       help='path to config file')
    args = parser.parse_args()

    if args.run:
        conf = {}
        with open(args.run) as f:
            conf = json.load(f)
            base = DEFAULT
            base.update(conf.get('base', {}))
            do_run(base, conf['runs'])
