#!/usr/bin/env python
# encoding: utf-8

from beansdbadmin.core.fnv1a import get_hash_beansdb as fnv1a
from mmh3 import hash as murmur3_32
import re

M = 1 << 32


def murmur(k):
    return (murmur3_32(k) + M) % M


KHASH_PREFIX = "__BeansDBv2__0X"
hex_patten = re.compile('[0-9a-fA-F]+')


def get_khash(key):
    return fnv1a(key) & 0xffffffff


get_hash = get_khash


def str_khash64(khash):
    if type(khash) == int:
        return "%016x" % khash
    return khash


def get_khash64(key):
    if key.startswith(KHASH_PREFIX):
        s = len(KHASH_PREFIX)
        m = re.search(hex_patten, key[s:])
        if m is None or m.start() != 0:
            return 0
        return int(key[s: s + m.end()], 16)
    high = fnv1a(key)
    low = murmur(key)
    return (high << 32) | low


def get_khash_order(k):
    return ((k & ((1 << 20) - 1)) << 44) + (k >> 20)


def get_vhash(data):
    data_len = len(data)
    uint32_max = 2 ** 32 - 1
    hash_ = (data_len * 97) & uint32_max
    if len(data) <= 1024:
        hash_ += get_hash(data)
        hash_ &= uint32_max
    else:
        hash_ += get_hash(data[0:512])
        hash_ &= uint32_max
        hash_ *= 97
        hash_ &= uint32_max
        hash_ += get_hash(data[data_len - 512: data_len])
        hash_ &= uint32_max
    hash_ &= 0xffff
    return hash_
