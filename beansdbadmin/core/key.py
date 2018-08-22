#!/usr/bin/env python
# encoding: utf-8

from beansdbadmin.core.hash import get_khash


def bkt_atoi(a):
    if isinstance(a, int):
        return a
    return int(a, 16)
    

def get_bucket_from_key(key, db_depth=1):
    assert db_depth <= 2
    hash_ = get_khash(key)
    return hash_ >> (32 - db_depth * 4)


def generate_key(db_depth=1, prefix='', count=16 * 1024, sector=None):
    """ sector express in 0 or (0, 0) """
    if sector is not None:
        assert (isinstance(sector, int) or
                (isinstance(sector, (tuple, list)) and len(sector) == 2))
    i = 0
    j = 0
    while j < count:
        key = prefix + "test%s" % (i)
        if sector is not None:
            if sector == get_bucket_from_key(key, db_depth):
                j += 1
                yield key
        else:
            j += 1
            yield key
        i += 1
