#!/usr/bin/env python
# encoding: utf-8

import json
import socket
import os
import logging
from collections import Counter
from beansdbadmin.core.zookeeper import ZK
from beansdbadmin.core.server_info import get_bucket_all

STATE_UNAVAILABLE = "0"
STATE_AVAILABLE = "1"
STATE_MIGRATING = "2"

STATE_TYPES = {STATE_UNAVAILABLE: '不可用',
               STATE_AVAILABLE: '可用',
               STATE_MIGRATING: '迁移中'}

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(module)s %(filename)s %(funcName)s:%(lineno)d %(levelname)s %(message)s',
                    datefmt='%Y%m%dT%H:%M:%S')
logger = logging.getLogger(__name__)


def update_server_info(host=None, zk=None):
    if not host:
        host = _get_hostname()
    server_info = zk.disk_info_get(host)
    if not server_info:
        server_info = {}
    disks = server_info.get('disks', None)
    if 'available' not in server_info:
        server_info['available'] = STATE_AVAILABLE  # default value and can be change
    server_info['disks'] = get_disks_info(disks, host)
    zk.disk_info_set(host, server_info)
    return server_info


def get_server_info(host=None, zk=None):
    if not host:
        host = _get_hostname()
    update_server_info(host, zk=zk)
    return zk.disk_info_get(host)


def get_disks_info(server_disks=None, host=None):
    import psutil
    if not host:
        host = _get_hostname()

    disk_info = {}
    partitions = psutil.disk_partitions()

    disks = [p.mountpoint for p in partitions if p.mountpoint != '/']
    disk_buckets = get_disk_buckets(host)
    for d in disks:
        disk_info[d] = {}
        disk_info[d]['free_size'] = psutil.disk_usage(d).free
        disk_info[d]['bucket_num'] = disk_buckets.get(d)
        disk_info[d]['available'] = STATE_AVAILABLE  # default value and can be change
        if server_disks and d in server_disks:
            disk_info[d]['available'] = server_disks[d]['available']
    return disk_info


def get_disk_buckets(host):
    if not host:
        host = _get_hostname()
    buckets = get_bucket_all(host)
    paths = []
    for bkt in buckets:
        try:
            p = os.readlink(bkt.get('Home'))
            paths.append(p)
        except OSError as e:
            logger.error('host: {}, bkt: {}'.format(socket.gethostname(), bkt.get('Home')))
            logger.error(e)
            continue

    disks = ['/'+p.split('/')[1] for p in paths]
    disk_buckets = Counter(disks)
    return disk_buckets


# # for test
# def get_disks_info(server_disks=None, host=None):
#     import requests
#     resp = requests.get('http://%s:7903/du' % host)
#     data = json.loads(resp.text)
#     disks_info = data.get('Disks')
#     disks = disks_info.keys()
#     disk_info = {}
#     for d in disks:
#         d = d.encode('utf-8')
#         disk_info[d] = {}
#         disk_info[d]['free_size'] = disks_info.get(d).get('Free')
#         disk_info[d]['bucket_num'] = len(get_bucket_all(host))
#         disk_info[d]['available'] = STATE_AVAILABLE  # default value and can be change
#         if server_disks and d in server_disks:
#             disk_info[d]['available'] = server_disks[d]['available']
#     return disk_info


def mark_server_disk_state(disks=None, available=STATE_AVAILABLE, host=None, zk=None):
    if not host:
        host = _get_hostname()
    server_info = get_server_info(host, zk=zk)
    if not disks:
        server_aval = server_info.get('available', STATE_AVAILABLE)
        if server_aval == available:
            return
        server_info['available'] = available
        zk.disk_info_set(host, server_info)
        return

    disk_info = server_info['disks']
    if not isinstance(disks, list):
        disks = [disks]
    for d in disks:
        d_info = disk_info.get(d)
        if d_info:
            disk_aval = d_info.get('available', STATE_AVAILABLE)
            if disk_aval != available:
                d_info['available'] = available
        else:
            continue
    server_info['disks'] = disk_info
    zk.disk_info_set(host, server_info)
    return


def _get_hostname():
    return socket.gethostname()


def get_zk(cluster):
    zk = ZK(cluster=cluster)
    return zk


def main():
    """
    usage: python -m beansdbadmin/core/agent_cli --aval 1 --disks /data1 /data2
    """

    disk_list = []

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--aval', choices=['1', '0'])
    parser.add_argument('--disks', nargs='+')
    parser.add_argument('--cluster', choices=['db256', 'fs', 'test'])
    args = parser.parse_args()

    cluster = args.cluster
    zk = get_zk(cluster)

    if args.disks:
        disk_list = args.disks
    if args.aval:
        avaliable = args.aval
        update_server_info(zk=zk)
        mark_server_disk_state(disk_list, avaliable, zk=zk)

    server_info = get_server_info(zk=zk)
    print(json.dumps(server_info, indent=4, sort_keys=True))

if __name__ == '__main__':
    main()