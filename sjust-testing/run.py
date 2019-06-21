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
	debug rocksdb = 0/0
	osd pool default pg num = 8
	osd op num shards = 5

[osd]
	osd objectstore = bluestore

	# use directory= option from fio job file
	osd data = {target_dir}
	osd journal = {target_dir}/journal

	# log inside fio_dir
	log file = {output_dir}/log
        bluestore_tracing = true
        event_tracing = true
"""

BLUESTORE_FIO = """
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

rw=randwrite
iodepth={qd}

time_based=1
runtime={runtime}s

nr_files=256
size=4m
bs={bs}k

[warmup]
rw=randwrite

[write]
rw=randwrite
"""

DEFAULT = {
    'output_dir': os.path.join('./output',time.strftime('%Y-%m-%d-%H:%M:%S')),
    'lib': '../build/lib',
    'target_dir': '/mnt/sjust/run',
    'qd': None,
    'runtime': 10,
    'bs': 4,
    'fio_bin': '../build/bin/fio',
    'lttng': True
}

def get_fio_fn(base):
    return os.path.join(base, 'bluestore.fio')

def get_ceph_fn(base):
    return os.path.join(base, 'ceph.conf')

def get_fio_output(base):
    return os.path.join(base, 'fio_output.json')

def get_fio_stdout(base):
    return os.path.join(base, 'fio.stdout')

def write_conf(conf):
    fio_fn = get_fio_fn(conf['output_dir'])
    ceph_fn = get_ceph_fn(conf['output_dir'])
    for fn, template in [(fio_fn, BLUESTORE_FIO), (ceph_fn, BLUESTORE_CONF)]:
        with open(fn, 'w') as f:
            f.write(template.format(**conf))
    return fio_fn

def setup_start_lttng(conf):
    if not conf.get('lttng', False):
        return
    tracedir = os.path.join(conf['output_dir'], 'trace')
    subprocess.run(['mkdir', tracedir])
    subprocess.run([
        'lttng', 'create', 'fio-bluestore',
        '--output', tracedir
        ])
    subprocess.run([
        'lttng', 'enable-event',
        '--session', 'fio-bluestore',
        '--userspace', 'eventtrace:oid_elapsed'
    ])
    subprocess.run([
        'lttng', 'enable-event',
        '--session', 'fio-bluestore',
        '--userspace', 'bluestore:transaction_state_duration'
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

def run_conf(conf):
    for d in [conf['output_dir'], conf['target_dir']]:
        subprocess.run(['rm', '-rf', d], check=False)
        subprocess.run(['mkdir', '-p', d])
    fio_conf = write_conf(conf)
    env = {
        'LD_LIBRARY_PATH': conf['lib']
    }
    output_json = get_fio_output(conf['output_dir'])
    cmd = [
        conf['fio_bin'],
        fio_conf,
        '--output', output_json,
        '--output-format', 'json+']
    stop_destroy_lttng(conf)
    setup_start_lttng(conf)
    with open(get_fio_stdout(conf['output_dir']), 'w') as outf:
        subprocess.run(cmd, env=env, stdout=outf, stderr=outf)
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
        full_config['target_dir'] = os.path.join(
            full_config['devices'][full_config['target_device']],
            'target')
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

def summarize(directory, p):
    contents = os.listdir(directory)
    if 'ceph.conf' in contents:
        contents = [directory]
    else:
        contents = [(x, os.path.join(directory, x)) for x in contents]

    ret = []
    for name, subdir in contents:
        fio_output = {}
        with open(get_fio_output(subdir)) as f:
            fio_output = json.load(f)
        perf_output = {}
        with open(os.path.join(subdir, 'perf_counters.json')) as f:
            perf_output = json.load(f)
        with open(get_base_config(subdir)) as f:
            base_config = json.load(f)
        ret.append(p(name, base_config, fio_output, perf_output))
    return ret

def project(name, config, fio_stats, perf_stats):
    def f(op):
        return {
            'iops_min': op['iops_min'],
            'iops_max': op['iops_max'],
            'iops_mean': op['iops_mean'],
            'clat_min_ns': op['clat_ns']['min'],
            'clat_max_ns': op['clat_ns']['max'],
            'clat_mean_ns': op['clat_ns']['mean'],
            'clat_median_ns': op['clat_ns']['percentile']['50.000000'],
            'clat_99.9_ns': op['clat_ns']['percentile']['99.900000'],
            'slat_min_ns': op['slat_ns']['min'],
            'slat_max_ns': op['slat_ns']['max'],
            'slat_mean_ns': op['slat_ns']['mean'],
        }
    fio = dict(((op, f(fio_stats['jobs'][1][op])) for op in ['read', 'write']))

    wanted_perf = [
        'commit_lat',
        'kv_commit_lat',
        'kv_final_lat',
        'kv_flush_lat',
        'kv_sync_lat',
        'state_deferred_aio_wait_lat',
        'state_deferred_cleanup_lat',
        'state_deferred_queued_lat',
        'state_kv_committing_lat'
        ]

    perf = {
        k: v['avgtime'] for k, v in
        filter(lambda x: '_lat' in x[0],
               perf_stats['perfcounter_collection']['bluestore'].items())
        }

    return {
        'fio': fio,
        'config': config,
        'name': name,
        'perf': perf,
        }

import argparse
parser = argparse.ArgumentParser()
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--summarize', type=str,
                   help='generate json summary of results')
group.add_argument('--run', type=str,
                   help='path to config file')
args = parser.parse_args()

if args.summarize:
    try:
        json.dump(summarize(args.summarize, project), sys.stdout,
                  sort_keys=True, indent=2)
    except Exception as e:
        print("Unable to summarize {dir}, exception {e}".format(
            dir=args.summarize,
            e=e))
elif args.run:
    try:
        conf = {}
        with open(args.run) as f:
            conf = json.load(f)
        base = DEFAULT
        base.update(conf.get('base', {}))
        do_run(base, conf['runs'])
    except Exception as e:
        print("Unable to run config {conf}, exception {e}".format(
            conf=args.run,
            e=e))
