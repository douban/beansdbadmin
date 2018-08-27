#!/usr/bin/env python
# encoding: utf-8

import os
import time
import logging
import subprocess
import psutil
import getpass
import atexit
import beansdbadmin.core.log as log

from collections import defaultdict
from beansdbadmin.lib.bottle import route, run, request, redirect
from beansdbadmin.core.conf import get_server_conf
from beansdbadmin.core.zookeeper import ZK
from beansdbadmin.core.agent_cli import get_server_info as _get_server_info
from beansdbadmin.core.agent_cli import mark_server_disk_state, STATE_UNAVAILABLE, STATE_AVAILABLE, STATE_MIGRATING
from beansdbadmin.core.path import (
    get_disk_info as _get_disk_info,
    get_data_files as _get_data_files,
    du as get_du,
    get_disk_free
)
from beansdbadmin.core.node import (
    Node, ERR_SPACE, ERR_RSYNC_WORKING, ERR_RSYNC_NOT_DONE)


log.basicConfig()

logger = logging.getLogger(__name__)
zk_conf = ''


@atexit.register
def kill_rsync_clients():
    for bucket, st in RsyncState.buckets.items():
        if st.proc:
            logger.warn("kill rsync for bucket %d", bucket)
            st.proc.kill()

    logger.warn("agent killed")


# from config file
confdir = "/etc/gobeansdb"
DB_HOME = '/var/lib/beansdb'
DB_DEPTH = 2
cluster_name = None


# server

def get_disk_info():
    """ Return disk info.
    {
        '/data1/doubandb': {
            'free_size': 77452419072,  # Bytes
            'buckets': [0, 10]
        }
    }
    """
    disk_info = _get_disk_info([DB_HOME], DB_DEPTH)
    disk_free, disk2buckets = disk_info[-2], disk_info[-1]
    rs = defaultdict(dict)
    for d in disk_free:
        rs[d]['free_size'] = disk_free[d]
        rs[d]['buckets'] = list(disk2buckets[d])
    return rs


def get_bucket_info():
    """ Return bucket info.
    {
        bucket: [(data_path, size, mtime)]  # sorted by data_apth
    }
    """
    disk_info = _get_disk_info([DB_HOME], DB_DEPTH)
    buckets = disk_info[0]
    rs = defaultdict(list)
    for b in buckets:
        data_files = _get_data_files([DB_HOME], DB_DEPTH, [b]).next()[1]
        for f in data_files:
            rs[b].append([
                f,
                os.path.getsize(f),
                os.path.getmtime(f)
            ])
    return {k: sorted(v) for (k, v) in rs.iteritems()}


def bucket_path(disk, bucket):
    return os.path.join(disk, bucket)


def rsync_path(disk, bucket=None):
    root = os.path.join(disk, "rsync")
    if bucket is None:
        return root
    else:
        return os.path.join(root, bucket)


class RsyncState(object):
    buckets = dict()

    @classmethod
    def get_bucket(cls, bucket, disk):
        st = cls.buckets.get(bucket)
        if not st:
            st = RsyncState(bucket, disk)
            cls.buckets[bucket] = st
        return st

    def __init__(self, bucket, disk):
        self.bucket = bucket
        if len(bucket) == 1:
            self.bucket_path = bucket
            self.rsync_home = rsync_path(disk)
        else:
            self.bucket_path = bucket[0] + "/" + bucket[1]
            self.rsync_home = rsync_path(disk, bucket[0])

        self.disk = disk
        self.real = bucket_path(disk, self.bucket_path)
        self.tmp = rsync_path(disk, self.bucket_path)
        self.link = os.path.join(DB_HOME, self.bucket_path)

        self.dstate = -1
        self.du = 0

        self.src = ""
        self.src_du = 0

        self.proc = None
        self.rc = -1  # < 0: new, None: running, 0: ok, > 0: err
        self.finish_time = 0

        self.err = None
        self.commited = False

        self.update_state()

    def get_disk_state(self):
        if (not os.path.exists(self.real) and
                not os.path.exists(self.link)):

            if not os.path.exists(self.tmp):
                return 0
            else:
                self.du = get_du(self.tmp)
                return 1

        elif (not os.path.exists(self.tmp) and
              os.path.exists(self.real) and
              os.path.exists(self.link) and
              os.readlink(self.link) == self.real):
            self.du = get_du(self.real)
            return 2

        return -1

    def commit(self):
        if len(self.bucket) == 2:
            for p in [self.real, self.link]:
                p = os.path.dirname(p)
                if not os.path.exists(p):
                    os.mkdir(p)

        logger.info("rename %s -> %s", self.tmp, self.real)
        os.rename(self.tmp, self.real)
        logger.info("link %s -> %s", self.link, self.real)
        os.symlink(self.real, self.link)
        self.finish_time = time.time()

    def summary(self, err=None):
        return {'state': {'bucket': self.bucket,
                          'dstate': self.dstate,
                          'du': self.du,
                          'rc': self.rc,
                          'finish_time': self.finish_time},
                'err': err or self.err}

    def check_and_clear_dir(self):
        try:
            logger.info("check tmp %s", self.tmp)
            if not os.path.exists(self.tmp):
                os.makedirs(self.tmp)
            for p in [self.real, self.link]:
                logger.info("check dir %s", p)
                if not os.path.exists(p):
                    os.makedirs(p)  # check permation
                os.rmdir(p)  # only clear empty dir
            return True
        except OSError as e:
            self.err = "%s" % e
            return False

    def start_rsync(self):
        bwlimit = 60000  # KB
        drop_cache = True
        self.proc = self.src.rsync_client().rsync(self.bucket_path, self.rsync_home, bwlimit, drop_cache=drop_cache)
        self.rc = self.proc.poll()
        time.sleep(1)

        self.update_state()
        logging.info("rsync client start: %s", self.summary())

        if self.rc is None or self.rc == 0:
            RsyncState.buckets[self.bucket] = self

    def update_state(self):
        self.get_disk_state()
        if self.is_running():
            rc = self.rc = self.proc.poll()
            if rc == 0:
                self.finish_time = time.time()
            elif rc > 0:
                self.err = "rsync fail"

    def is_running(self):
        return not self.finish_time and not self.err and self.rc is None


# for multi buckets, e.g. "01,0a,1b"
@route('/rsync/prepare')
def rsync_prepare():
    disk = request.query['disk']
    size = int(request.query['size'])
    buckets = request.query['buckets']
    buckets = buckets.split(',')

    free_size = get_disk_free(disk, True)[1]
    if free_size < size + (50 << 30):
        return {'err': ERR_SPACE}

    for b in buckets:
        st = RsyncState(b, disk)
        if not st.check_and_clear_dir():
            return st.summary()
    return {}


# bucket like "0a"
# 统一返回 RsyncState.summary, 和 rsync_state 一样
@route('/rsync/start/<bucket>')
def rsync_start(bucket):
    disk = request.query['disk']
    size = int(request.query['size'])
    src = Node(request.query['src'])

    st = RsyncState.buckets.get(bucket)
    if st and st.running():
        return st.summary(ERR_RSYNC_WORKING)
    # 如果不是在跑的，会直接覆盖

    st = RsyncState(bucket, disk)
    st.src = src
    st.src_du = size

    if st.dstate == 2:
        return st.summary("not_empty")
    elif st.check_and_clear_dir():
        st.start_rsync()
    return st.summary()


@route('/rsync/state/<bucket>')
def rsync_state(bucket):
    if bucket == "all":
        return [rsync_state(b) for b in RsyncState.buckets]

    st = RsyncState.buckets.get(bucket)
    if not st:
        return {}
    st.update_state()
    return st.summary()


@route('/rsync/commit/<bucket>')
def rsync_commit(bucket):
    st = RsyncState.buckets.get(bucket)
    if not st:
        return {}
    elif not st.finish_time:
        return st.summary(ERR_RSYNC_NOT_DONE)
    else:
        st.commit()
        return st.summary()


@route('/rsync/kill/<bucket>')
def rsync_kill(bucket):
    st = RsyncState.buckets.get(bucket)
    if st:
        if st.proc:
            st.proc.kill()
        RsyncState.buckets.pop(bucket)
        return st.summary()
    else:
        return {}


@route('/disks')
def get_disks():
    global DB_DEPTH
    DB_DEPTH = request.query.get('depth', default=2)
    return {'disks': get_disk_info()}


@route('/buckets')
def get_buckets():
    global DB_DEPTH
    DB_DEPTH = request.query.get('depth', default=2)
    return {'buckets': get_bucket_info()}


@route('/disk_info')
def get_server_info():
    return _get_server_info(zk=zk_client(zk_conf))


@route('/mark_server')
def mark_server():
    disks = request.query.get('disks')
    if disks:
        disks = disks.split(',')
    avail = request.query.get('avail')
    if avail == '0':
        avail = STATE_UNAVAILABLE
    elif avail == '1':
        avail = STATE_AVAILABLE
    elif avail == '2':
        avail = STATE_MIGRATING
    else:
        return 'error avail value'
    if disks:
        mark_server_disk_state(disks=disks, available=avail, host=None, zk=zk_client(zk_conf))
    else:
        mark_server_disk_state(disks=None, available=avail, host=None, zk=zk_client(zk_conf))
    redirect('/disk_info')


def pgrep_rsyncd(port):
    username = getpass.getuser()
    for pid in psutil.pids():
        try:
            p = psutil.Process(pid)
            if p.username() == username:
                cmdline = p.cmdline()
                if len(cmdline) > 1 and cmdline[0] == "rsync" and cmdline[-1] == str(port):
                    return p
        except psutil.Error:
            continue


@route('/rsyncd/restart/<port>')
def restart_rsyncd(port):
    cmd = ["killall", "rsync"]
    subprocess.call(cmd, stderr=subprocess.STDOUT)
    time.sleep(1)
    return "%s" % start_rsyncd(int(port))


def start_rsyncd(port):
    if pgrep_rsyncd(port):
        logger.info("rsyncd at port %d already started", port)
        return True

    cmd = ["rsync", "--daemon", "--config", os.path.join(confdir, "rsyncd.conf"),
           "--port", str(port)]
    return 0 == subprocess.call(cmd, stderr=subprocess.STDOUT)


def check_rsyncd(port):
    try:
        cmd = ['rsync', '--list-only', 'rsync://localhost:%d' % port]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).strip()
        print "check_output: (%s)" % out
        return out == "beansdb"
    except subprocess.CalledProcessError:
        return False


def read_rsynd_config(path):
    d = dict()
    with open(path) as f:
        for line in f:
            kv = line.split("=")
            if len(kv) == 2:
                k = kv[0].strip()
                v = kv[1].strip()
                d[k] = v
    return d


def zk_client(zk_cluster):
    return ZK(zk_cluster)


def main():
    global confdir, DB_HOME, cluster_name, zk_conf

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--confdir')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s:%(lineno)d %(levelname)s %(message)s',
                        datefmt='%Y%m%dT%H:%M:%S')

    if args.confdir:
        confdir = args.confdir
    conf = get_server_conf(confdir)
    DB_HOME = conf['hstore']['local']['home']
    mc_port = conf['server']['port']
    zk_conf = conf['server']['zkpath'].split('/')[-1]
    cluster_name = conf['cluster_name']
    logging.info("agent start, home %s", DB_HOME)

    me = Node("localhost:%d" % mc_port)

    if not os.environ.get('BOTTLE_CHILD'):  # not restart when reload
        logging.info("start rsyncd ok: %s", start_rsyncd(me.rsync_client().port))

    run(host="0.0.0.0", port=me.agent_client().port, reloader=False, debug=True)


if __name__ == '__main__':
    main()
