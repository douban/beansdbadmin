# coding: utf-8
import time
import itertools


K = (1 << 10)
M = (1 << 20)
G = (1 << 30)


def big_num(n, before=4, after=2):
    n = float(n)
    fmt = "%%0%d.%df" % (before + after + 1, after)
    if n < 1000:
        return str(n)
    elif n < K * 1000:
        return (fmt % (n / K)) + "K"
    elif n < M * 1000:
        return (fmt % (n / M)) + "M"
    return (fmt % (n / G)) + "G"


def get_start_time(uptime):
    start_time = time.localtime(time.time() - uptime)
    return time.strftime("%Y-%m-%d %H:%M:%S", start_time)


def grouper(n, iterable):
    """
    grouper(3, range(10))
    >>> [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    args = [iter(iterable)] * n
    return ([e for e in t if e != None] for t in itertools.izip_longest(*args))


if __name__ =="__main__":
    print list(grouper(3, range(10)))
