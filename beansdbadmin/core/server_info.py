# coding: utf-8

"""
本文件中的函数是为了从 gobeansdb web 接口中获取 gobeansdb server 的信息。
"""
import json
from collections import defaultdict
from beansdbadmin.core.client import get_url_content


GOBEANSDB_WEB_PORT = 7903


# helper
def get_http(server, query, port=GOBEANSDB_WEB_PORT):
    url = 'http://%s:%d/%s' % (server, port, query)
    content = get_url_content(url)
    return content


# pages
def get_config(server):
    v = json.loads(get_http(server, "config"))
    if isinstance(v, list):
        v = v[0]
    return v


def reload_route(server, port):
    return get_http(server, "reload", port)


def get_lasterr_ts(server):
    "Return last ts of last ERROR OR WARN"
    logs = get_log_last(server)
    logs_ts = [e['TS'][:19] if e is not None else "0" for e in logs]
    return max(logs_ts[2], logs_ts[3])


def get_log_last(server, port=GOBEANSDB_WEB_PORT):
    return json.loads(get_http(server, "loglast", port))


def get_du(server, port=GOBEANSDB_WEB_PORT):
    return json.loads(get_http(server, "du", port))


def get_gc(server, port=GOBEANSDB_WEB_PORT):
    server_gc_status = False
    bucket_gc_status = defaultdict(dict)
    bucket_all = get_bucket_all(server, port=port)
    for bucket in bucket_all:
        gc_status = bucket['LastGC']['Running']
        bucket_gc_status[bucket['ID']] = gc_status
        if gc_status is True:
            server_gc_status = True

    return server_gc_status, bucket_gc_status


def get_buffer_stat(server, port=GOBEANSDB_WEB_PORT):
    return json.loads(get_http(server, "buffers", port))


def get_bucket_stat(server, bucket, port=GOBEANSDB_WEB_PORT):
    return json.loads(get_http(server, "bucket/%x" % bucket, port))


def get_bucket_all(server, port=GOBEANSDB_WEB_PORT):
    return json.loads(get_http(server, "bucket/all", port))


def int2hex(d, depth=1):
    format = '%%0%dx' % depth
    if isinstance(d, int):
        return format % d
    elif isinstance(d, str):
        if d.isdigit():
            return format % (int(d))
    return d


# wappers
def get_buckets(server):
    "Return hex id list of buckets."
    config = get_config(server)
    return [int2hex(i) for (i, v) in enumerate(config['Buckets'])
            if v == 1]


if __name__ == '__main__':
    s = 'rosa3h'
    bkt = 0xf
    print get_config(s)
    print get_buckets(s)
    print get_lasterr_ts(s)
    print get_du(s)
    print get_buffer_stat(s)
    print get_bucket_stat(s, bkt)
    print get_bucket_all(s)
