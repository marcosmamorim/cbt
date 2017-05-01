#!/usr/bin/python

import argparse
import os, fnmatch
import numpy
import hashlib
import database
import uuid
from htmlgenerator import HTMLGenerator
import yaml
import re
import logging
import sys
sys.path.append('/home/mamorim/Projects/cbt')

from log_support import setup_loggers

logger = logging.getLogger("cbt")

def mkhash(values):
    value_string = ''.join([str(i) for i in values])
    return hashlib.sha256(value_string).hexdigest()


def parse_args():
    parser = argparse.ArgumentParser(description='get fio averages.')
    parser.add_argument('input_directory', help='Directory to search.')
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
            # print "\t\tGROUP: %s" % match.group()
            results = match.group().split(':')
            # print "\t\tRESULTS: %s" % results
            metrics[results[0].strip()] = results[1].strip()

    for k,v in metrics.iteritems():
        values[k.strip()] = {}
        v2 = dict(x.strip().split('=') for x in v.split(','))
        values[k].update(v2)

    # logger.debug("Insert Values: %s" % values)
    print values
    for k, v in values.iteritems():
        # logger.debug("Metric Type: %s" % k)
        if isinstance(v, dict):
            for k1, v1 in v.iteritems():
                db_result = []
                for field in header_results:
                    db_result.append(params[field])
                # logger.debug("Metric Type: %s - Key: %s - Value: %s" % (k, k1, v1))
                db_result.append(k)
                db_result.append(k1)
                db_result.append(v1.strip())
                db_result.append(str(uuid.uuid1()))
                db_result.append(server_name)
                print "Insert results: %s" % db_result
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
        # logger.info("Read outputs from %s" % output)
        # print "\t" + output
        getmetrics(output, params)
        # print "\t\tResultados: %s" % results
    # print "Find benchmarks on %s" % base_dir
    # bmode = params['mode']
    # uuid = params['uuid']
    # device = os.path.basename(params['block_devices'])
    # results_out = {uuid: {bmode: {device: {}}}}
    # print "Benchmark Type: %s - Disk: %s" % (params['mode'], params['block_devices'])
    # # print "Device name: %s" % device
    # outputs = find("%s-%s*" % (device, bmode), base_dir)
    # for inputname in outputs:
    #     print "Arquivo: %s " % inputname
    #     parse = inputname.split('/')
    #     print "Parse: %s" % parse
    #     results = getmetrics(inputname)
    #     results_out[uuid][bmode][device].update(results)
    # print results_out


def getbenchmarks():
    logger.info("Begin read benchmarks")
    files = find('params-*.yml', ctx.input_directory)

    for inputname in files:
        logger.info("Insert benchmarks from parameters file: %s" % inputname)

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
        database.insert_benchmark(benchmark_db)

        get_outputs(base_dir, params)




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

