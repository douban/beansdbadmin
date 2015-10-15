# coding: utf-8
import os
import glob
from collections import defaultdict, namedtuple
from beansdbadmin.consts import BEANSDB_BUCKET_PATTERN

DiskUsage = namedtuple('DiskUsage', 'total used free')


def get_buckets_info():
    """ Return bucket info.
    {
        "bucket": [(data_path, mtime),]  # sorted by data_path
    }
    """
    rs = {}
    bucket_paths = glob.glob(BEANSDB_BUCKET_PATTERN)
    for bucket_path in bucket_paths:
        bucket = bucket_path.rsplit(os.path.sep, 1)[-1]
        data_paths = glob.glob(os.path.join(bucket_path, '*.data'))
        rs[bucket] = sorted([(p, os.path.getmtime(p)) for p in data_paths])
    return rs


def get_disks_info():
    """ Return disk info.
    {
        "/data1": {
            'free_size': 123,  # Byte
            'buckets': ['0', '1'],
        }
    }
    """
    rs = defaultdict(dict)
    bucket_paths = glob.glob(BEANSDB_BUCKET_PATTERN)
    for bucket_path in bucket_paths:
        bucket = bucket_path.rsplit(os.path.sep, 1)[-1]
        mountpoint = find_mount_point(bucket_path)
        disk_usage = get_disk_usage(bucket_path)
        rs[mountpoint]["free_size"] = disk_usage.free
        rs[mountpoint].setdefault('buckets', [])
        rs[mountpoint]['buckets'].append(bucket)
    return rs


def find_mount_point(path):
    path = os.path.abspath(path)
    while not os.path.ismount(path):
        path = os.path.dirname(path)
    return path


def get_disk_usage(path):
    """Return disk usage statistics about the given path.

    Will return the namedtuple with attributes: 'total', 'used' and 'free',
    which are the amount of total, used and free space, in bytes.
    """
    st = os.statvfs(path)
    free = st.f_bavail * st.f_frsize
    total = st.f_blocks * st.f_frsize
    used = (st.f_blocks - st.f_bfree) * st.f_frsize
    return DiskUsage(total, used, free)
