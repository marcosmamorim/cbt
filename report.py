#!/usr/bin/python

import argparse
import os, fnmatch
from parsing import database
import uuid
from parsing.htmlgenerator import HTMLGenerator
import yaml
import re
import logging
import sys
from texttable import Texttable
from log_support import setup_loggers

logger = logging.getLogger("cbt")

def parse_args():
    parser = argparse.ArgumentParser(description='Import FIO results and Reports.')
    parser.add_argument('-i', help='Import benchmarks from directory.', dest='input_directory')
    parser.add_argument('-l', help='List benchmarks.', dest='list_benchmark', default=False, action='store_true')
    parser.add_argument('-r', help='List benchmark results', dest='benchmark_uuid', default=None)
    parser.add_argument('-m', help='List benchmark result by metric type [write][read]', dest='benchmark_metric', default=None)
    parser.add_argument('-k', help='List benchmark result by result key [iops][runt][bw][io]', dest='benchmark_key', default=None)
    parser.add_argument('-lk', help='List results key', dest='list_keys', default=False, action='store_true')
    # parser.add_argument('-i', default=[], help='Import from directory.')
    args = parser.parse_args()
    return args


def find(pattern, path):
    result = []
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                result.append(os.path.join(root, name))
    return result


def getbw(s):
    if "GB/s" in s:
        return float(s[:-4])*1024
    if "MB/s" in s:
        return float(s[:-4])
    if "KB/s" in s:
        return float(s[:-4])/1024

def replace_metric(metric):
    regex = r"(KB.*|msec)"
    return re.sub(regex, '', metric)


# Read metrics from output files
def import_metrics(filename, params):
    header_results = ['uuid', 'iodepth', 'mode', 'op_size', 'vol_size', 'block_devices']

    file_parse = filename.split("_")
    server_name = file_parse[len(file_parse) -1]
    # logger.info("Parse metrics from output  %s" % filename.split('_'))
    regex = r"(  write|clat \(usec\)|slat|bw|WRITE:|read :|READ:).*"
    metrics = {}
    values = {}
    for i, line in enumerate(open(filename)):
        for match in re.finditer(regex, line):
            results = match.group().split(':')
            metrics[results[0].strip()] = results[1].strip()

    for k,v in metrics.iteritems():
        values[k.strip()] = {}
        v2 = dict(x.strip().split('=') for x in v.split(','))
        values[k].update(v2)

    # print values

    for k, v in values.iteritems():
        if isinstance(v, dict):
            for k1, v1 in v.iteritems():
                db_result = []
                for field in header_results:
                    db_result.append(params[field])
                k = k.split(' ')[0]
                db_result.append(k)
                db_result.append(k1)
                # print "METRIC: %s " % replace_metric(v1)
                db_result.append(replace_metric(v1).strip())
                db_result.append(str(uuid.uuid1()))
                db_result.append(server_name[1:])
                database.insert_results(db_result, params)

def import_results(base_dir, params):
    # logger.info("Begin read output files")
    out_file = "%s/output-" % (base_dir)
    for k, v in sorted(params.iteritems()):
        if k == 'block_devices':
            out_file += "%s_" % os.path.basename(v)
            continue
        out_file += "%s_" % (v)

    out_file += "*"
    files = find('output-*', base_dir)
    for output in files:
        import_metrics(output, params)


def import_benchmarks():
    logger.info("Begin read benchmarks")
    files = find('params-*.yml', ctx.input_directory)

    for inputname in files:

        # print inputname
        base_dir = os.path.dirname(inputname)
        with open(inputname, 'r') as stream:
            try:
                params = yaml.load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        # print "Benchmark Parameters: %s" % params
        benchmark_db = {"uuid": 0, "benchmark": 1, "iodepth": 1, "mode": 'write', "block_devices": '/dev/vdb', "vol_size": 10,
                        "op_size": 10}

        # Parse parameters and insert into benchmark table
        for bench in benchmark_db:
            benchmark_db[bench] = params[bench]

        # print "Insert into benchmark table: %s" % benchmark_db
        if database.exists(benchmark_db, 'benchmark'):
            logger.info("Insert benchmarks from parameters file: %s" % inputname)
            database.insert_benchmark(benchmark_db)
            import_results(base_dir, params)
        else:
            logger.warning("Benchmark already imported to database %s" % inputname)


def list_benchmarks():
    results = database.get_benchmarks(None)
    # print results
    header = ['uuid', 'benchmark', 'iodepth', 'mode', 'op_size', 'vol_size', 'block_devices', 'date']
    t = Texttable()
    t.set_cols_width([36, 10, 7, 9, 8, 8, 14, 26])
    t.set_cols_align(['l', 'l', 'r', 'l', 'r', 'r', 'l', 'c'])
    t.header(header)
    for r in results:
        t.add_row(r)
    print t.draw()


def list_benchmark_results(buuid, metric=None, key=None):
    results = database.get_benchmark_results(buuid, metric, key)
    # print results;
    t = Texttable()
    header = ['benchmark uuid', 'iodepth', 'mode', 'op_size', 'vol_size', 'block_devices', 'type', 'key', 'value', 'server']
    t.set_cols_width([36, 7, 7, 7, 8, 13, 8, 5, 9, 16])
    t.set_cols_align(['l', 'r', 'l', 'r', 'r', 'l', 'l', 'l', 'r', 'l'])
    t.header(header)
    for r in results:
        t.add_row(r)
    print t.draw()

def list_keys():
    t = Texttable()
    t.set_deco(Texttable.HEADER)
    t.header(['Keys'])
    results = database.get_keys()
    print results
    for r in results:
        t.add_row(r)
    print t.draw()

if __name__ == '__main__':
    setup_loggers()
    ctx = parse_args()
    database.create_db()

    if ctx.input_directory is not None:
        logger.info("Starting import from directory to database")
        import_benchmarks()

    if ctx.list_benchmark:
        list_benchmarks()

    if ctx.benchmark_uuid and ctx.benchmark_key is None:
        list_benchmark_results(ctx.benchmark_uuid, ctx.benchmark_metric)

    if ctx.benchmark_uuid and ctx.benchmark_key:
        list_benchmark_results(ctx.benchmark_uuid, None, ctx.benchmark_key)

    if ctx.list_keys:
        list_keys()

    sys.exit(0)

