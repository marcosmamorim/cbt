#!/usr/bin/python

import argparse
import os, fnmatch
from parsing import database
import uuid
from parsing.htmlgenerator import HTMLGenerator
import yaml
import re
import logging

from log_support import setup_loggers

logger = logging.getLogger("cbt")

def parse_args():
    parser = argparse.ArgumentParser(description='Import FIO results and Reports.')
    parser.add_argument('input_directory', help='Directory to search.')
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


def splits(s,d1,d2):
    l, _,r = s.partition(d1)
    m,_,r = r.partition(d2)
    return m


def getbw(s):
    if "GB/s" in s:
        return float(s[:-4])*1024
    if "MB/s" in s:
        return float(s[:-4])
    if "KB/s" in s:
        return float(s[:-4])/1024

# Read metrics from output files
def getmetrics(filename, params):
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
                db_result.append(k)
                db_result.append(k1)
                db_result.append(v1.strip())
                db_result.append(str(uuid.uuid1()))
                db_result.append(server_name)
                database.insert_results(db_result, params)

def get_outputs(base_dir, params):
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
        getmetrics(output, params)


def getbenchmarks():
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
            get_outputs(base_dir, params)
        else:
            logger.warning("Benchmark already imported to database %s" % inputname)



        #
        # # strip off the input directory
        # params = inputname[len(ctx.input_directory):].split("/")
        #
        # # Workaround to get benchmark uuid
        # if not benchmark.has_key('uuid'):
        #     pos = (len(params)-2)
        #     print "UUID: %s" % params
        #     uuid = {'uuid': params[pos]}
        #     benchmark.update(uuid)
        #
        # base_dir = os.path.dirname(inputname)
        #
        # # Start to search output and process metrics
        # findbenchmarks(base_dir, benchmark)



        # # make readahead into an int
        # params[3] = int(params[3][7:])
        #
        # # Make op_size into an int
        # params[4] = int(params[4][8:])
        #
        # # Make cprocs into an int
        # params[5] = int(params[5][17:])
        #
        # # Make io_depth int an int
        # params[6] = int(params[6][9:])
        #
        # params_hash = mkhash(params)
        # params = [params_hash] + params
        # params.extend([0,0])
        # database.insert(params)
        #
        # for line in open(inputname):
        #     if "aggrb" in line:
        #          bw = getbw(splits(line, 'aggrb=', ','))
        #          if "READ" in line:
        #              database.update_readbw(params_hash, bw)
        #          elif "WRITE" in line:
        #              database.update_writebw(params_hash, bw)
        # html = HTMLGenerator()
        # html.add_html(html.read_file('/home/nhm/src/cbt/include/html/table.html'))
        # html.add_style(html.read_file('/home/nhm/src/cbt/include/css/table.css'))
        # html.add_script(html.read_file('/home/nhm/src/cbt/include/js/jsxcompressor.min.js'))
        # html.add_script(html.read_file('/home/nhm/src/cbt/include/js/d3.js'))
        # html.add_script(html.read_file('/home/nhm/src/cbt/include/js/d3var.js'))
        # html.add_script(html.format_data(database.fetch_table(['opsize', 'testtype'])))
        # html.add_script(html.read_file('/home/nhm/src/cbt/include/js/table.js'))
        #
        # print '<meta charset="utf-8">'
        # print '<title>D3 Table Test </title>'
        # print '<html>'
        # print html.to_string()
        # print '</html>'
        #    print database.fetch_table(['opsize', 'testtype'])

        #    get_section(['opsize', 'testtype'])

        #    write_html()
        #    write_data(['opsize', 'testtype'])
        #    write_style()
        #    write_js()


if __name__ == '__main__':
    setup_loggers()
    ctx = parse_args()
    database.create_db()
    getbenchmarks()

