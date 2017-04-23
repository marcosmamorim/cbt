import common
import settings
import monitoring
import os
import time
import logging

from benchmark import Benchmark

logger = logging.getLogger("cbt")

class KvmRbdFio(Benchmark):

    def __init__(self, cluster, config):
        super(KvmRbdFio, self).__init__(cluster, config)
        self.config = config
        self.iteration = config.get('iteration')
        self.block_device_list = config.get('block_devices', '/dev/vdb' )
        self.block_devices = [ d.strip() for d in self.block_device_list.split(',') ]
        self.concurrent_procs = config.get('concurrent_procs', len(self.block_devices))
        self.total_procs = self.concurrent_procs * len(settings.getnodes('clients').split(','))
        self.fio_out_format = config.get("fio_output", "json")
        self.time = str(config.get('time', '300'))
        self.ramp = str(config.get('ramp', '0'))
        self.startdelay = config.get('startdelay', None)
        self.rate_iops = config.get('rate_iops', None)
        self.iodepth = config.get('iodepth', 16)
        self.numjobs = config.get('numjobs', 1)
        self.mode = config.get('mode', 'write')
        self.rwmixread = config.get('rwmixread', 50)
        self.rwmixwrite = 100 - self.rwmixread
        self.ioengine = config.get('ioengine', 'libaio')
        self.op_size = config.get('op_size', 4194304)
        self.vol_size = config.get('vol_size', 65536) * 0.9
        self.client_ra = config.get('client_ra', '128')
        self.fio_cmd = config.get('fio_cmd', '/usr/bin/fio')
        self.out_dir = '%s/%s' % (self.archive_dir, self.uuid)
        self.run_dir = "%s/%s/%s" % (settings.cluster.get('tmp_dir'), self.getclass(), self.uuid)

    def exists(self):
        if os.path.exists(self.out_dir):
            logger.info('Skipping existing test in %s.', self.out_dir)
            return True
        return False

    def initialize(self): 
        super(KvmRbdFio, self).initialize()

        common.pdsh(settings.getnodes('clients', 'osds', 'mons', 'rgws'),
                    'sudo rm -rf %s' % self.run_dir,
                    continue_if_error=False).communicate()
        common.make_remote_dir(self.run_dir)
        clnts = settings.getnodes('clients')


        logger.info('creating mountpoints...')
        for b in self.block_devices:
            bnm = os.path.basename(b)
            mtpt = '/srv/rbdfio-`%s`-%s' % (common.get_fqdn_cmd(), bnm)
            # TODO: Create parameter to select filesystem and filesystem options
            common.pdsh(clnts, 'sudo mkfs.ext4 %s' % b,
                        continue_if_error=False).communicate()
            common.pdsh(clnts, 'sudo mkdir -p %s' % mtpt,
                        continue_if_error=False).communicate()
            common.pdsh(clnts, 'sudo mount -t ext4 -o noatime %s %s' % (b,mtpt),
                        continue_if_error=False).communicate()
        logger.info('Attempting to initialize fio files...')
        initializer_list = []
        for i in range(self.concurrent_procs):
            b = self.block_devices[i % len(self.block_devices)]
            bnm = os.path.basename(b)
            mtpt = '/srv/rbdfio-`hostname -s`-%s' % bnm
            fiopath = os.path.join(mtpt, 'fio%d.img' % i)
            pre_cmd = 'sudo %s --rw=write -ioengine=sync --bs=%s ' % (self.fio_cmd, self.op_size)
            pre_cmd = '%s --size %dM --name=%s > /dev/null' % \
                      (pre_cmd, self.vol_size, fiopath)
            initializer_list.append(common.pdsh(clnts, pre_cmd,
                                    continue_if_error=False))
        for p in initializer_list:
             p.communicate()

        # Create the run directory
        common.pdsh(clnts, 'rm -rf %s' % self.run_dir, 
                    continue_if_error=False).communicate()
        common.make_remote_dir(self.run_dir)

    def run(self):
        super(KvmRbdFio, self).run()
        # Set client readahead
        self.set_client_param('read_ahead_kb', self.client_ra)

        clnts = settings.getnodes('clients')

        common.make_remote_dir(self.run_dir)

        # We'll always drop caches for rados bench
        self.dropcaches()

        monitoring.start(self.run_dir)

        time.sleep(5)
        # Run the backfill testing thread if requested
        if 'recovery_test' in self.cluster.config:
            recovery_callback = self.recovery_callback
            self.cluster.create_recovery_test(self.run_dir, recovery_callback)

        logger.info('Starting rbd fio %s test.', self.mode)

        fio_process_list = []
        for i in range(self.concurrent_procs):
            b = self.block_devices[i % len(self.block_devices)]
            # out_file
            # [iteration]-[device]-[mode]-[client_ra]-[op_size]-[concurrent_procs]-[iodepth]
            out_file = '%s/%08d-%s-%08d-%08d-%03d-%03d-%s' % \
                       (self.run_dir, self.iteration, os.path.basename(b), int(self.client_ra),
                        int(self.op_size), int(self.total_procs), int(self.iodepth), self.mode)

            bnm = os.path.basename(b)
            mtpt = '/srv/rbdfio-`hostname -s`-%s' % bnm
            fiopath = os.path.join(mtpt, 'fio%d.img' % i)
            fio_cmd = 'sudo %s' % self.fio_cmd
            fio_cmd += ' --rw=%s' % self.mode
            if (self.mode == 'readwrite' or self.mode == 'randrw'):
                fio_cmd += ' --rwmixread=%s --rwmixwrite=%s' % (self.rwmixread, self.rwmixwrite)
            fio_cmd += ' --ioengine=%s' % self.ioengine
            fio_cmd += ' --runtime=%s' % self.time
            fio_cmd += ' --ramp_time=%s' % self.ramp
            if self.startdelay:
                fio_cmd += ' --startdelay=%s' % self.startdelay
            if self.rate_iops:
                fio_cmd += ' --rate_iops=%s' % self.rate_iops
            fio_cmd += ' --numjobs=%s' % self.numjobs
            fio_cmd += ' --direct=1'
            fio_cmd += ' --bs=%dB' % self.op_size
            fio_cmd += ' --iodepth=%d' % self.iodepth
            fio_cmd += ' --size=%dM' % self.vol_size 
            if 'recovery_test' in self.cluster.config:
                fio_cmd += ' --time_based'
            fio_cmd += ' --name=%s > %s.json' % (fiopath, out_file)
            fio_process_list.append(common.pdsh(clnts, fio_cmd, continue_if_error=False))
        for p in fio_process_list:
            p.communicate()
        monitoring.stop(self.run_dir)
        logger.info('Finished rbd fio test')

        common.sync_files('%s/*' % self.run_dir, self.out_dir)
        common.create_params_file(self.config, self.out_dir)
        self.cleanup()

    def cleanup(self):
         super(KvmRbdFio, self).cleanup()
         clnts = settings.getnodes('clients')
         common.pdsh(clnts, 'killall fio').communicate()
         time.sleep(3)
         common.pdsh(clnts, 'killall -9 fio').communicate()
         time.sleep(3)
         common.pdsh(clnts, 'rm -rf /srv/*/*',
                     continue_if_error=True).communicate()
         common.pdsh(clnts, 'sudo umount /srv/* || echo -n').communicate()

    def set_client_param(self, param, value):
         cmd = 'find /sys/block/vd* ! -iname vda -exec sudo sh -c "echo %s > {}/queue/%s" \;' % (value, param)
         common.pdsh(settings.getnodes('clients'), cmd).communicate()

    def __str__(self):
        return "%s\n%s\n%s" % (self.run_dir, self.out_dir, super(KvmRbdFio, self).__str__())

    def recovery_callback(self):
        common.pdsh(settings.getnodes('clients'), 'sudo killall fio').communicate()

