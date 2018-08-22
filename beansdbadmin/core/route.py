#!/usr/bin/env python
# encoding: utf-8

#!/usr/bin/env python
# encoding: utf-8

import yaml
from collections import defaultdict
import logging

PORT = 7900

logger = logging.getLogger(__name__)


class RouteError(Exception):
    pass


def get_depth(nb):
    if nb == 1:
        return 0
    elif nb == 16:
        return 1
    elif nb == 256:
        return 2


class Route(object):

    def __init__(self, dic):
        self.numbucket = dic['numbucket']
        self.depth = get_depth(self.numbucket)
        self.backup = set(dic.get('backup', []))
        self.main = dict()
        self.buckets = defaultdict(set)
        self.buckets_int = defaultdict(set)
        for addr_buckets in dic['main']:
            addr = addr_buckets['addr']
            buckets = set(addr_buckets['buckets'])
            assert self.main.get(addr) is None
            self.main[addr] = set(buckets)
            for b in buckets:
                self.buckets[b].add(addr)
                self.buckets_int[int(b, 16)].add(addr)
        pairs = [(k, v) for (k, vs) in self.main.items() for v in vs]
        self.main_set = set(pairs)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.main_set, self.backup) == (other.main_set, other.backup)
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def diff(self, other):
        return (self.main_set - other.main_set, other.main_set - self.main_set)

    def to_dict(self):
        return {'numbucket': self.numbucket,
                'backup': list(self.backup),
                'main': [{'addr': addr, 'buckets': list(buckets)}
                         for (addr, buckets) in self.main.items()]}

    # bucket is hex str
    def move_bucket(self, bucket, src, dst):
        assert src != dst
        assert isinstance(bucket, str)
        assert dst not in self.main or bucket not in self.main[dst]
        assert bucket in self.main[src]
        if dst not in self.main:
            self.main[dst] = set()
        self.main[dst].add(bucket)
        self.main[src].remove(bucket)
        if len(self.main[src]) == 0:
            self.main.pop(src)
        self.buckets[bucket].remove(src)
        self.buckets[bucket].add(dst)

    @classmethod
    def from_yaml(cls, raw):
        return Route(yaml.load(raw))

    @classmethod
    def from_zk(cls, zk):
        data = zk.route_get()
        return cls.from_yaml(data)

    def to_yaml(self):
        return yaml.dump(self.to_dict())

    def to_256(self):
        return {
            "numbucket": 256,
            "backup": list(self.backup),
            "main": [{
                "addr": addr,
                "buckets": multiply_hex(bucket)
            } for addr, bucket in self.main.items()]
        }

    def to_256_yaml(self):
        return yaml.dump(self.to_256())


def multiply_hex(buckets):
    buckets_list = []
    for bucket_id in list(buckets):
        bucket_num = int(str(bucket_id), 16)
        buckets_list.extend([format(bucket_num*16+i, '02x') for i in xrange(16)])
    return buckets_list


def dump_file(content, path):
    with open(path, "w") as f:
        f.write(content)


def main():
    import argparse
    #parser = argparse.ArgumentParser(prog='PROG')
    #parser.add_argument('cluster', choices=clusters)

    parser = argparse.ArgumentParser(description="manipulate route.yaml")
    parser.add_argument('path', help="path of yaml file, save to xxx.new")

    subparsers = parser.add_subparsers(dest="cmd", help='all commands')

    parser_mv = subparsers.add_parser('mv', help='move a bucket')

    parser_mv.add_argument('bucket', help="e.g. 01")
    parser_mv.add_argument('src', help="e.g. rosa1a:7900")
    parser_mv.add_argument('dst')

    args = parser.parse_args()
    path = args.path

    with open(path, 'r') as f:
        content = f.read()

    r = Route.from_yaml(content)

    if args.cmd == 'mv':
        r.move_bucket(args.bucket, args.src, args.dst)
        new_path = path + ".new"
        dump_file(r.to_yaml(), new_path)
        print "save to", new_path

    path = args.path


if __name__ == "__main__":
    main()
