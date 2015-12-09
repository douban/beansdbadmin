#!/usr/bin/env python
# encoding: utf-8

import time

from beansdb_tools.sa.cmdb import get_hosts_by_tag
from beansdb_tools.core.server_info import get_du, get_buffer_stat, get_bucket_stat, get_config, get_lasterr_ts
from beansdb_tools.core.client import DBClient

K = (1 << 10)
M = (1 << 20)
G = (1 << 30)


def get_all_server_stats():
    hosts = get_hosts_by_tag("gobeansdb_servers")
    return [ServerInfo(addr) for addr in hosts]


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
        self.buffer_stat = get_buffer_stat(addr)
        self.lasterr_ts = get_lasterr_ts(addr)
        self.du = get_du(addr)
        self.numbucket = self.config['NumBucket']
        buckets_id = [i for (i, v) in enumerate(self.config['Buckets'])
                      if v == 1]
        self.buckets = dict([(i, get_bucket_stat(addr, i))
                             for i in buckets_id])
        c = DBClient(addr + ":7900")

        self.bucket_tree_root = dict()
        if self.numbucket == 16:
            d = c.get_dir("@")
            print d
            for bkt in self.buckets:
                key = "%x/" % bkt
                self.bucket_tree_root[bkt] = d[key]
        else:
            # TODO
            pass
        self.stats = c.stats()

    def summary_server(self):
        start_time = time.localtime(time.time() - int(self.stats['uptime']))
        start_time = time.strftime("%Y-%m-%dT%H:%M:%S", start_time)
        rss = self.stats["rusage_maxrss"]
        total_items = self.stats["total_items"]
        mindisk = min([dinfo['free']
                       for (d, dinfo) in self.du[0]['disks'].items()])
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

    testone()
    # testall()
