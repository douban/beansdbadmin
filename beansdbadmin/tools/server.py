#!/usr/bin/env python
# encoding: utf-8

import time
import collections

from beansdb_tools.sa.cmdb import get_hosts_by_tag
from beansdb_tools.core.server_info import (
    get_du, get_buffer_stat, get_bucket_all, get_config, get_lasterr_ts
    )
from beansdb_tools.core.client import DBClient

K = (1 << 10)
M = (1 << 20)
G = (1 << 30)

backup_servers = ["chubb2", "chubb3"]

def getprimaries():
    hosts = get_hosts_by_tag("gobeansdb_servers")
    return [host for host in hosts if host not in backup_servers] # + ["rosa4h"]

def get_all_server_stats():
    sis = [ServerInfo(host) for host in getprimaries()]
    sis.sort(key=lambda x: (x.err is None, x.host))
    return sis

def get_all_buckets_stats(digit=2):
    buckets = [get_buckets_info(host, digit) for host in getprimaries()]
    return [b for b in buckets if b is not None]


def big_num(n, before=4, after=2):
    n = float(n)
    fmt = "%%0%d.%df" % (before+after+1, after)
    if n < 1000:
        return str(n)
    elif n < K * 1000:
        return (fmt % (n/K)) + "K"
    elif n < M * 1000:
        return (fmt % (n/M)) + "M"
    return (fmt % (n/G)) + "G"


class ServerInfo(object):

    def __init__(self, host):
        self.host = host
        self.err = None
        try:
            self.config = get_config(host)
            self.numbucket = self.config['NumBucket']
            self.buckets_id = [i for (i, v) in enumerate(self.config['Buckets'])
                               if v == 1]
            self.mc = DBClient(self.host + ":7900")
            self.du = get_du(self.host)
            self.buffer_stat = get_buffer_stat(self.host)
            self.lasterr_ts = get_lasterr_ts(self.host)
            self.stats = self.mc.stats()
        except Exception as e:
            self.err = e

    def summary_server(self):
        if self.err is not None:
            return [self.host,
                    "%s:7903" % (self.host),
                    "%s" % self.err
                   ]

        start_time = time.localtime(time.time() - int(self.stats['uptime']))
        start_time = time.strftime("%Y-%m-%dT%H:%M:%S", start_time)
        rss = self.stats["rusage_maxrss"]
        total_items = self.stats["total_items"]
        mindisk = min([dinfo['Free']
                       for (_, dinfo) in self.du['Disks'].items()])
        return [self.host,
                "%s:7903" % (self.host),
                "%d/%d" % (len(self.buckets_id), self.numbucket),
                self.stats["version"],
                total_items,
                big_num(rss*1024, 2, 2),
                big_num(mindisk, 2, 2),
                start_time,
                self.lasterr_ts,
               ]

def summary_bucket(host, bkt, digit):
    bkt_id = bkt["ID"]
    fmt = "%%0%dx" % digit
    hint_state = bkt["HintState"]
    return [host,
            "%s:7903/bucket/%d" % (host, bkt_id),
            fmt % bkt_id,
            big_num(bkt["DU"], 3, 2),
            bkt["Pos"]["ChunkID"],
            bkt["Pos"]["Offset"],
            bkt["NextGCChunk"],
            hint_state,
           ]

def get_buckets_info(host, digit):
    try:
        buckets = get_bucket_all(host)
        return [summary_bucket(host, bkt, digit) for bkt in buckets]
    except:
        pass


class Bucket(object):

    def __init__(self, num_bucket, bucket_id):
        self.num_bucket = num_bucket
        self.bucket_id = bucket_id
        self.bucket_id_str = (
            "%x" % bucket_id) if num_bucket == 16 else ("%2x" % bucket_id)
        self.servers = []  # [(host, count), ... ]
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


def get_all_buckets_key_counts(n):
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
        print server.host
        server.summary_server()


def testone():
    h = "rosa1h"
    si = ServerInfo(h)
    print si.summary_server()
    get_buckets_info(h)

if __name__ == '__main__':

    testone()
    # testall()
    # print get_all_buckets(256)
