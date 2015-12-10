#!/usr/bin/env python
# encoding: utf-8

import time
import collections

from beansdb_tools.sa.cmdb import get_hosts_by_tag
from beansdb_tools.core.server_info import get_du, get_buffer_stat, get_bucket_stat, get_config, get_lasterr_ts
from beansdb_tools.core.client import DBClient

K = (1 << 10)
M = (1 << 20)
G = (1 << 30)

backup_servers = ["chubb2", "chubb3"]


def get_all_server_stats():
    hosts = get_hosts_by_tag("gobeansdb_servers")
    return [ServerInfo(addr) for addr in hosts if addr not in backup_servers]


def big_num(n):
    n = float(n)
    if n < 1000:
        return str(n)
    elif n < K * 1000:
        return ("%07.2f" % (n/K)) + "K"
    elif n < M * 1000:
        return ("%07.2f" % (n/M)) + "M"
    return ("%07.2f" % (n/G)) + "G"


class ServerInfo(object):

    def __init__(self, addr):
        self.addr = addr
        self.config = get_config(addr)
        self.numbucket = self.config['NumBucket']
        self.buckets_id = [i for (i, v) in enumerate(self.config['Buckets'])
                           if v == 1]

        self.mc = DBClient(addr + ":7900")
        self.du = get_du(addr)
        self.get_server_info()
        self.get_buckets_info()

    def get_server_info(self):
        self.buffer_stat = get_buffer_stat(self.addr)
        self.lasterr_ts = get_lasterr_ts(self.addr)
        self.stats = self.mc.stats()

    def get_buckets_info(self):
        # also need du
        self.buckets = dict([(i, get_bucket_stat(self.addr, i))
                             for i in self.buckets_id])
        self.bucket_tree_root = dict()
        if self.numbucket == 16:
            d = self.mc.get_dir("@")
            for bkt in self.buckets:
                key = "%x/" % bkt
                self.bucket_tree_root[bkt] = d[key]
        else:
            # TODO
            pass

    def summary_server(self):
        start_time = time.localtime(time.time() - int(self.stats['uptime']))
        start_time = time.strftime("%Y-%m-%dT%H:%M:%S", start_time)
        rss = self.stats["rusage_maxrss"]
        total_items = self.stats["total_items"]
        mindisk = min([dinfo['free']
                       for (_, dinfo) in self.du[0]['disks'].items()])
        return [self.addr,
                "%d/%d" % (len(self.buckets), self.numbucket),
                self.stats["version"],
                total_items,
                big_num(rss*1024),
                big_num(mindisk),
                start_time,
                self.lasterr_ts,
                ]

    def summary_bucket(self, bkt_id):
        bkt = self.buckets[bkt_id][0]
        du = self.du[0]['buckets'][bkt_id]
        tree = self.bucket_tree_root[bkt_id]
        return [self.addr,
                bkt_id,
                big_num(du),
                tree[1],
                tree[0],
                bkt["Pos"]["ChunkID"],
                bkt["Pos"]["Offset"],
                bkt["NextGCChunk"],
                ]

    def summary_buckets(self):
        return [self.summary_bucket(i) for i in self.buckets]


class Bucket(object):

    def __init__(self, num_bucket, bucket_id):
        self.num_bucket = num_bucket
        self.bucket_id = bucket_id
        self.bucket_id_str = (
            "%x" % bucket_id) if num_bucket == 16 else ("%2x" % bucket_id)
        self.servers = []  # [(addr, count), ... ]
        self.backups = []
        self.max_diff = 0
        self.rank = 0
        self.cmpkey = None

        # TODO: collect failed severs
        self.config_servers = []

    def compute(self):
        self.servers.sort(key=lambda x: x[1])
        self.max_diff = self.servers[-1][1] - self.servers[0][1]
        max_backup = 0
        if len(self.backups) > 0:
            max_backup = max([x[1] for x in self.backups])
        n = len(self.servers)
        self.cmpkey = (
            abs(n - 3), n, self.max_diff, max_backup, -self.bucket_id)

    def __repr__(self):
        return "(%s %s %s)" % (self.bucket_id, self.servers, self.backups)


def get_key_counts(mc, path, base=0):
    d = mc.get_dir("@" + path)
    l = sorted(list(d.items()))
    counts = [v[1] for (_, v) in l]
    return dict([(base + i, c) for (i, c) in enumerate(counts) if c > 0])


def get_buckets_key_counts(host, n):
    mc = DBClient(host + ":7900")
    d16 = get_key_counts(mc, "")
    # print d16
    if n == 16:
        return d16
    else:
        mc = DBClient(host + ":7900")
        d256 = dict()
        for i, _ in d16.items():
            subd = get_key_counts(mc, "%x" % i, 16*i)
            d256.update(subd)
        return d256


def get_all_buckets(n):
    buckets = [Bucket(n, i) for i in range(n)]
    hosts = get_hosts_by_tag("gobeansdb_servers")
    hosts = [i for i in hosts if i not in backup_servers]
    for h in hosts:
        d = get_buckets_key_counts(h, n)
        for bkt, count in d.items():
            buckets[bkt].servers.append((h, count))
    for h in backup_servers:
        d = get_buckets_key_counts(h, n)
        for bkt, count in d.items():
            buckets[bkt].backups.append((h, count))
    for bkt in buckets:
        bkt.compute()
    buckets.sort(key=lambda x: x.cmpkey, reverse=True)
    for i, bkt in enumerate(buckets):
        bkt.rank = i
    return buckets


def testall():
    servers = get_all_server_stats()
    for server in servers:
        print server.addr
        server.summary_server()


def testone():
    h = "rosa1h"
    si = ServerInfo(h)
    print si.summary_server()
    for bkt in si.summary_buckets():
        print bkt

if __name__ == '__main__':

    # testone()
    # testall()
    print get_all_buckets(256)
