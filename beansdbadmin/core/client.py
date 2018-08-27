#!/usr/bin/python
# encoding: utf-8
'''a rich client
    1. for one server (instead of multi like in libmc.Client)
    2. encapsulate @, ?, gc ...

use is instead of libmc.Client
'''

import telnetlib
import logging
import libmc
import string
import urllib
import itertools
import warnings
from collections import defaultdict
from beansdbadmin.core.hint import parse_new_hint_body
from beansdbadmin.core.data import parse_records
from beansdbadmin.core.hash import get_khash64


def get_url_content(url):
    return urllib.urlopen(url).read()


def check_bucket(bucket):
    assert 0 <= bucket < 16


def dir_to_dict(dir_str):
    d = dict()
    if dir_str:
        for line in [x for x in dir_str.split('\n') if x]:
            key_or_bucket, _hash, ver_or_count = line.split(' ')
            d[key_or_bucket] = int(_hash) & 0xffff, int(ver_or_count)
    return d


def get_bucket_keys_count(store, bucket, depth=1):
    cmd = "@"
    sub = bucket
    if depth == 2:
        cmd = "@%x" % (bucket/16)
        sub = bucket % 16
    result = store.get(cmd)
    if result:
        lines = result.split('\n')
        for line in lines:
            if len(line) == 0:
                continue
            d, _, c = line.split()
            if d.endswith('/'):
                bucket_ = int(d[0], 16)
                if bucket_ == sub:
                    return int(c)
    raise Exception('get %s from %s, reply = [%s], bucket %x not found' % (cmd, store, result, bucket))


def get_buckets_keys_count(store):
    """ return dict: buckets -> count """
    st = {}
    try:
        for line in (store.get('@') or '').split('\n'):
            if line:
                d, _, c = line.split(' ')
                if not d.endswith('/'):
                    continue
                st[int(d[0], 16)] = int(c)
        return st
    except IOError:
        raise Exception("cannot get @ from %s" % (store))


def get_primary_buckets(store):
    """ return possible primary buckets, might be wrong on temporary nodes,
        result is list of buckets in integer
    """
    ss = get_buckets_keys_count(store)
    bucket_list = ss.items()
    bucket_list = [x for x in bucket_list if x[1] > 0]
    if not bucket_list:
        return None
    bucket_list.sort(lambda a, b: cmp(a[1], b[1]), reverse=True)
    result = [bucket_list[0]]
    for i in bucket_list[1:]:
        if result[-1][1] / i[1] >= 2:
            break
        result.append(i)
    return [x[0] for x in result]


def get_key_info_disk(store, key):
    '''return ver, vhash, flag, vsz, ts, fid, pos'''
    info = store.get('??' + key)
    if info:
        return [int(x) for x in info.split()]


def is_gc_running(ip, port):
    s = get_gc_status(ip, port)
    if s and s.find('running') >= 0:
        return True
    return False


def get_gc_status(ip, port):
    t = telnetlib.Telnet(ip, port)
    t.write('optimize_stat\r\n')
    out = t.read_until('\n')
    t.write('quit\r\n')
    t.close()
    return out.strip("\r\n")


def connect(server, **kwargs):
    comp_threshold = kwargs.pop('comp_threshold', 0)
    prefix = kwargs.pop('prefix', None)
    if prefix is not None:
        warnings.warn('"prefix" is deprecated. '
                      'use douban.wrapper.Prefix instead.')

    c = libmc.Client([server],
                     do_split=0,
                     comp_threshold=comp_threshold,
                     prefix=prefix)
    c.config(libmc.MC_CONNECT_TIMEOUT, 300)  # 0.3s
    c.config(libmc.MC_POLL_TIMEOUT, 3000)  # 3s
    c.config(libmc.MC_RETRY_TIMEOUT, 5)  # 5s
    return c


class MCStore(object):
    IGNORED_LIBMC_RET = frozenset([
        libmc.MC_RETURN_OK,
        libmc.MC_RETURN_INVALID_KEY_ERR
    ])

    def __init__(self, addr):
        self.addr = addr
        self.host, port = addr.split(":")
        self.port = int(port)
        self.mc = connect(addr)

    def __repr__(self):
        return '<MCStore(addr=%s)>' % repr(self.addr)

    def __str__(self):
        return self.addr

    def set(self, key, data, rev=0):
        return bool(self.mc.set(key, data, rev))

    def set_raw(self, key, data, rev=0, flag=0):
        if rev < 0:
            raise Exception(str(rev))
        return self.mc.set_raw(key, data, rev, flag)

    def set_multi(self, values, return_failure=False):
        return self.mc.set_multi(values, return_failure=return_failure)

    def _check_last_error(self):
        last_err = self.mc.get_last_error()
        if last_err not in self.IGNORED_LIBMC_RET:
            raise IOError(last_err, self.mc.get_last_strerror())

    def get(self, key):
        try:
            r = self.mc.get(key)
            if r is None:
                self._check_last_error()
            return r
        except ValueError:
            self.mc.delete(key)

    def get_raw(self, key):
        r, flag = self.mc.get_raw(key)
        if r is None:
            self._check_last_error()
        return r, flag

    def get_multi(self, keys):
        r = self.mc.get_multi(keys)
        self._check_last_error()
        return r

    def delete(self, key):
        return bool(self.mc.delete(key))

    def delete_multi(self, keys, return_failure=False):
        return self.mc.delete_multi(keys, return_failure=return_failure)

    def exists(self, key):
        return bool(self.mc.get('?' + key))

    def incr(self, key, value):
        return self.mc.incr(key, int(value))


class DBClient(MCStore):

    def __init__(self, addr):
        MCStore.__init__(self, addr)
        self._is_old = None

    def stats(self):
        stats = self.mc.stats()
        return stats.values()[0] if stats else None

    def is_old(self):
        if self._is_old is None:
            ver = self.get_server_version()
            self._is_old = (ver.strip().split(".")[0] == "0")
        return self._is_old

    def get_collision_summary(self, bucket):
        check_bucket(bucket)
        raw = self.get("@collision_%x" % bucket)
        if raw is None:
            return None
        count, hcount, khash, data_size = raw.split()
        return (int(count), int(hcount), int(khash, 16), int(data_size))

    def get_collision(self, bucket):
        check_bucket(bucket)
        collisions = defaultdict(dict)
        hint_data = self.get("@collision_all_%x" % bucket)
        if hint_data is None:
            return dict()
        for key, meta, _ in parse_new_hint_body(hint_data):
            khash_str, _, ver, vhash = meta
            collisions[khash_str][key] = (vhash, ver)
        return dict(collisions)

    def get_records_by_khash_raw(self, khash):
        if self.is_old():
            return []
        if not isinstance(khash, str):
            khash = "%016x" % khash
        return self.get("@@" + khash)

    def get_records_by_khash(self, khash_str):
        raw = self.get_records_by_khash_raw(khash_str)
        if raw:
            return parse_records(raw, False)
        else:
            return []

    def start_gc(self, bucket='', start_fid=0, end_fid=None):
        """ bucket must be in 0 or 00 string """
        if bucket:
            assert isinstance(bucket, basestring) and len(bucket) <= 2
        t = telnetlib.Telnet(self.host, self.port)
        tree = '@%s' % bucket
        if end_fid is None:
            gc_cmd = 'gc {} {}\n'.format(tree, start_fid)
        else:
            gc_cmd = 'gc {} {} {}\n'.format(tree, start_fid, end_fid)
        t.write(gc_cmd)
        out = t.read_until('\n').strip('\r\n')
        assert out == 'OK'
        t.write('quit\n')
        t.close()

    def start_gc_all_buckets(self, db_depth):
        hex_digits = string.digits + 'abcdef'
        buckets_iter = itertools.product(*[hex_digits for _ in range(db_depth)])
        buckets = [''.join(i) for i in buckets_iter]
        self.start_gc_buckets(buckets)

    def start_gc_buckets(self, buckets):
        for b in buckets:
            self.start_gc(bucket=b)
            while True:
                status = self.get_gc_status()
                if status.find('running') >= 0:
                    continue
                elif status == 'success':
                    print "bucket %s gc done" % b
                    break
                elif status == 'fail':
                    return self.fail("optimize_stat = fail")
                else:
                    self.fail(status)

    def get_gc_status(self):
        return get_gc_status(self.host, self.port)

    def get_version(self, key):
        meta = self.get("?" + key)
        if meta:
            return int(meta.split()[0])

    def item_count(self):
        s = self.stats()
        if s is None:
            return None
        return int(s['total_items'])

    def get_key_info_mem(self, key, khash64=None):
        ''' return (vhash, ver) or None'''
        if khash64 is None:
            khash64 = get_khash64(key)
        khash32_str = "@%08x" % (khash64 >> 32)
        _dir = self.get_dir(khash32_str)
        if self.is_old():
            return _dir.get(key, None)
        else:
            return _dir.get("%016x" % khash64, None)

    def get_khash_info_mem(self, khash):
        ''' return [(key, (vhash, ver))], key is "" for v2.'''
        khash32 = "@%08x" % (khash >> 32)
        _dir = self.get_dir(khash32)
        ret = []
        if self.is_old():
            for k, (vhash, ver) in _dir.iteritems():
                if get_khash64(k) == khash:
                    ret.append((k, (vhash, ver)))
        else:
            for k, (vhash, ver) in _dir.iteritems():
                if int(k, 16) == khash:
                    return [("", (int(vhash), ver))]
        return ret

    def get_server_version(self):
        try:
            st = self.stats()
            if st:
                return st["version"]
        except IOError:
            logging.error("fail to get version %s", self)
        except KeyError:
            logging.error("fail to get version %s %s", self, st)

    def get_dir(self, path):
        ''' return dict
            case1: map dir(0-f) to (hash, count),
                   like {'0/': (1471, 27784005), ... },
            case2: map key(or khash) to (vhash, version),
                   like {'3000000377e9c2ad': (22212, 1), ... }'''
        try:
            content = self.get(path)
        except IOError:
            content = ''
        return dir_to_dict(content)

    def list_dir(self, d):  # FIXME: d should not need prefix @?
        '''list all KEY in the dir!
        not use it if dir is large!'''
        for path, (vhash, ver) in sorted(self.get_dir(d).items()):
            if path.endswith('/') and len(path) == 2:
                for v in self.list_dir(d + path[:-1]):
                    yield v
            else:
                yield path, int(vhash), int(ver)

    def get_bucket_keys_count(self, bucket, depth=1):
        return get_bucket_keys_count(self, bucket, depth)

    def get_key_info_disk(self, key):
        '''return ver, vhash, flag, vsz, ts, fid, pos'''
        return get_key_info_disk(self, key)

    def prepare(self, data):
        return libmc.encode_value(data, self.mc.comp_threshold)

    def close(self):
        pass


def test_new(addr, bucket):
    b = bucket
    c = DBClient(addr)
    print "stats:", c.stats()
    print 'version:', c.get_server_version()
    print "isold:", c.is_old()
    print "dir root:", c.get_dir("@")
    print "bucket key count:", c.get_bucket_keys_count(int(b))
    print "item_count:", c.item_count()
    print "primary_buckets", get_primary_buckets(c)

    leaf = c.get_dir("@" + b + "000000")
    print "a dir leaf:", leaf
    khash_str = list(leaf)[0]
    print "a khash_str", khash_str
    r = c.get_records_by_khash(khash_str)[0]
    k = r[0]
    print "key, len(value), (flag, tstamp, ver):", k, r[1], r[3:]
    print "key info mem:", c.get_key_info_mem(k)
    print "key info disk(ver, vhash, flag, vsz, ts, fid, pos):", \
          c.get_key_info_disk(k)
    print "key version:", c.get_version(k)
    print "collision_summary", c.get_collision_summary(int(b))
    print "gc status:", c.get_gc_status()

if __name__ == '__main__':
    test_new("rosa3a:7900", '3')
