import common
import settings
import monitoring
import os
import time
import logging

from benchmark import Benchmark

logger = logging.getLogger("cbt")

class RawFio(Benchmark):

    def __init__(self, cluster, config):
        super(RawFio, self).__init__(cluster, config)
        logger.debug(config)
        self.config = config
        self.block_device_list = config.get('block_devices', '/dev/vdb' )
        self.block_devices = [d.strip() for d in self.block_device_list.split(',')]
        self.concurrent_procs = config.get('concurrent_procs', len(self.block_devices))
        self.total_procs = self.concurrent_procs * len(settings.getnodes('clients').split(','))
        self.time = str(config.get('time', '300'))
        self.ramp = str(config.get('ramp', '0'))
        self.startdelay = config.get('startdelay', None)
        self.rate_iops = config.get('rate_iops', None)
        self.iodepth = config.get('iodepth', 16)
        self.client_ra = config.get('client_ra', 128)
        self.direct = config.get('direct', 1)
        self.numjobs = config.get('numjobs', 1)
        self.mode = config.get('mode', 'write')
        self.rwmixread = config.get('rwmixread', 50)
        self.rwmixwrite = 100 - self.rwmixread
        self.ioengine = config.get('ioengine', 'libaio')
        self.op_size = config.get('op_size', 4194304)
        self.vol_size = config.get('vol_size', 65536) * 0.9
        self.output_format = config.get('output_format', 'terse')
        self.fio_cmd = config.get('fio_cmd', 'sudo /usr/bin/fio')
        self.out_dir = '%s/%s' % (self.archive_dir, self.uuid)
        self.run_dir = "%s/%s/%s" % (settings.cluster.get('tmp_dir'), self.getclass(), self.uuid)

    def exists(self):
        # if os.path.exists(self.out_dir):
        #     logger.info('Skipping existing test in %s.', self.out_dir)
        #     return True
        return False


    def initialize(self): 
        super(RawFio, self).initialize()

        if self.use_sudo:
            self.sudo = 'sudo'
        else:
            self.sudo = ''

        common.pdsh(settings.getnodes('clients'),
                    '%s rm -rf %s' % (self.sudo, self.run_dir),
                    continue_if_error=False).communicate()
        common.make_remote_dir(self.run_dir)

    def run(self):
        super(RawFio, self).run()
        import sys

        common.make_remote_dir(self.run_dir)

        # Set client readahead
        self.set_client_param('read_ahead_kb', self.client_ra)

        clnts = settings.getnodes('clients')

        # We'll always drop caches for rados bench
        self.dropcaches()

        monitoring.start(self.run_dir)

        time.sleep(5)

        logger.info('Starting raw fio %s test.', self.mode)

        fio_process_list = []
        for i in range(self.concurrent_procs):
            b = self.block_devices[i % len(self.block_devices)]
            fiopath = b
            out_file = "%s/output-" % (self.run_dir)
            for k, v in sorted(self.config.iteritems()):
                if k == 'block_devices':
                    out_file += "%s_" % os.path.basename(v)
                    continue
                out_file += "%s_" % (v)
            logger.info("OUTFILE: %s" % out_file)
            # sys.exit(0)
            fio_cmd = '%s %s' % (self.sudo, self.fio_cmd)
            fio_cmd += ' --rw=%s' % self.mode
            if (self.mode == 'readwrite' or self.mode == 'randrw'):
                fio_cmd += ' --rwmixread=%s --rwmixwrite=%s' % (self.rwmixread, self.rwmixwrite)
            fio_cmd += ' --ioengine=%s' % self.ioengine
            if self.time is not None:
                fio_cmd += ' --runtime=%s' % self.time
            if self.ramp is not None:
                fio_cmd += ' --ramp_time=%s' % self.ramp
            if self.startdelay:
                fio_cmd += ' --startdelay=%s' % self.startdelay
            if self.rate_iops:
                fio_cmd += ' --rate_iops=%s' % self.rate_iops
            fio_cmd += ' --numjobs=%s' % self.numjobs
            fio_cmd += ' --direct=%s' % self.direct
            fio_cmd += ' --bs=%dB' % self.op_size
            fio_cmd += ' --iodepth=%d' % self.iodepth
            fio_cmd += ' --size=%dM' % self.vol_size
            if self.output_format == 'normal':
                fio_cmd += ' --write_iops_log=%s' % out_file
                fio_cmd += ' --write_bw_log=%s' % out_file
                fio_cmd += ' --write_lat_log=%s' % out_file
                fio_cmd += ' ----bandwidth-log=1'
            if 'recovery_test' in self.cluster.config:
                fio_cmd += ' --time_based'
            fio_cmd += ' --name=%s --bandwidth-log=1 > %s' % (fiopath, out_file)
            fio_process_list.append(common.pdsh(clnts, fio_cmd, continue_if_error=False))
        for p in fio_process_list:
            p.communicate()
        monitoring.stop(self.run_dir)
        logger.info('Finished raw fio test')

        common.sync_files('%s/*' % self.run_dir, self.out_dir)
        common.create_params_file(self.config, self.out_dir)

    def cleanup(self):
        super(RawFio, self).cleanup()
        clnts = settings.getnodes('clients')

        logger.debug("Kill fio: %s" % clnts)
        common.pdsh(clnts, '%s killall fio' % self.sudo).communicate()
        time.sleep(3)
        common.pdsh(clnts, '%s killall -9 fio' % self.sudo).communicate()

        common.clean_remote_dir(self.run_dir)

    def set_client_param(self, param, value):
        cmd = 'find /sys/block/vd* ! -iname vda -exec %s sh -c "echo %s > {}/queue/%s" \;' % (self.sudo, value, param)
        common.pdsh(settings.getnodes('clients'), cmd).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(RawFio, self).__str__())

    def recovery_callback(self):
        common.pdsh(settings.getnodes('clients'), '%s killall fio' % self.sudo).communicate()

