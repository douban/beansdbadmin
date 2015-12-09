#!/usr/bin/env python
# encoding: utf-8

import os
import urllib
import json
import time
import sqlite3
import datetime
import telnetlib
from pprint import pprint

from beansdb_tools.tools.backup import get_backup_config
from beansdbadmin.tools.filelock import FileLock
from beansdbadmin.config import IGNORED_SERVERS
from beansdb_tools.sa.cmdb import get_hosts_by_tag

import logging
logger = logging.getLogger('gc')
LOG_FILENAME = '/var/log/beansdb-admin/gc.log'
LOG_FORMAT = '%(asctime)s-%(name)s-%(levelname)s-%(message)s'
logging.basicConfig(filename=LOG_FILENAME,
                    level=logging.INFO,
                    format=LOG_FORMAT)

SQLITE_DB_PATH = '/data/beansdbadmin/beansdb-gc.db'
DISK_URL_PATTERN = 'http://%s:7100/disks'
BUCKET_URL_PATTERN = 'http://%s:7100/buckets'
DISK_FREE_SIZE_THRESHHOLD_MAX = 57000000000
DISK_FREE_SIZE_THRESHHOLD_MIN = 0
GC_DATA_NUMBER_MIN = 5


### gc record database

class GCRecord(object):
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.cursor = self.conn.cursor()

    def create_table(self):
        self.cursor.execute(
        """CREATE TABLE gc_record (
            id INTEGER PRIMARY KEY,
            start_time TEXT,
            stop_time TEXT,
            bucket TEXT,
            server TEXT,
            disk TEXT,
            free_size_before INTEGER,
            free_size_after INTEGER,
            start_id INTEGER,
            stop_id INTEGER,
            status TEXT
           )""")
        self.conn.commit()

    def close(self):
        self.conn.close()

    def add_record(self, start_time, server, disk, free_size, bucket, start_id, stop_id):
        self.cursor.execute(
        """INSERT INTO gc_record (start_time, bucket, server, disk, free_size_before,
                                  start_id, stop_id, status)
           VALUES (:start_time, :bucket, :server, :disk, :free_size_before,
                   :start_id, :stop_id, 'running')
        """,
        {'start_time': start_time, 'server': server, 'bucket': bucket,
         'disk': disk, 'free_size_before': free_size,
         'start_id': start_id, 'stop_id': stop_id}
        )
        self.conn.commit()

    def update_status(self, id, status, stop_time, free_size_after):
        self.cursor.execute(
        """UPDATE gc_record SET status = :status,
                                stop_time = :stop_time,
                                free_size_after = :free_size_after
           WHERE id = :id
        """,
        {'status': status, 'id': id, 'stop_time': stop_time,
         'free_size_after': free_size_after}
        )
        self.conn.commit()

    def get_running_buckets_info(self):
        self.cursor.execute(
        "SELECT id, server, bucket, disk FROM gc_record WHERE status = 'running'"
        )
        return self.cursor.fetchall()

    def get_all_record(self):
        self.cursor.execute(
        "SELECT * FROM gc_record"
        )
        return self.cursor.fetchall()


### gc

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
            '-d', '--debug', action='store_true',
            help="Debug mode, send gc cmd to terminal."
            )
    parser.add_argument(
            '-i', '--init', action='store_true',
            help="Init database."
            )
    parser.add_argument(
            '-q', '--query', action='store_true',
            help="Query the running buckets."
            )
    parser.add_argument(
            '-u', '--update-status', action='store_true',
            help="Update the status of gc."
            )
    parser.add_argument(
            '-m', '--manual-operation',
            help=('Manual specify gc arguments "server bucket start_id stop_id" '
                  '(e.g. `gc.py -m "rosa2e 2 140 145"`).')
            )
    parser.add_argument(
            '-f', '--fail-id', type=int,
            help=('Update the status of failed record.')
            )
    args = parser.parse_args()

    gc_record = GCRecord(SQLITE_DB_PATH)
    server_ports = get_hosts_by_tag("gobeansdb_servers")
    servers = [x.split(':')[0] for x in server_ports]

    if args.init:
        gc_record.create_table()
        return

    if args.query:
        pprint(gc_record.get_all_record())
        return

    with FileLock(SQLITE_DB_PATH, timeout=10):
        if args.fail_id:
            mark_fail(gc_record, args.fail_id)

        if args.update_status:
            update_gc_status(gc_record)
            return

        if gc_record.get_running_buckets_info() and not args.debug:
            # there is another gc running
            return

        if args.manual_operation:
            manual_gc(gc_record, args.manual_operation, servers)
            return

        for disk_info in get_disks_need_gc(servers):
            if gc_disk(gc_record, disk_info, debug=args.debug):
                break


def mark_fail(gc_record, id):
    gc_record.update_status(id, 'fail', 'none', 0)


def update_gc_status(gc_record):
    running_buckets_info = gc_record.get_running_buckets_info()
    for id, server, bucket, disk in running_buckets_info:
        gc_status = beansdb_remote_cmd(server, 'optimize_stat', 2)
        if gc_status == 'success':
            stop_time = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
            free_size_after = get_disks_info(server)[disk]['free_size']
            gc_record.update_status(id, 'done', stop_time, free_size_after)


def manual_gc(gc_record, manual_operation, servers):
    server, bucket_id_hex, start_id, stop_id = manual_operation.split()
    if server not in servers:
        print 'wrong server name: %s' % server
        return
    bucket_id = int(bucket_id_hex, 16)
    disks_info = get_disks_info(server)
    disk = None
    v = None
    for disk, v in disks_info.iteritems():
        if bucket_id in v['buckets']:
            break
    assert disk, "wrong gc arguments: %s" % manual_operation
    time_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    gc_record.add_record(
        time_str, server, disk, v['free_size'],
        bucket_id_hex, start_id, stop_id)
    gc_bucket(server, bucket_id_hex, start_id, stop_id)


def get_disks_info(server):
    url = DISK_URL_PATTERN % server
    try:
        return get_url_data(url)['disks']
    except Exception as e:
        logger.error('%s: %s', url, e)
        return {}


def get_disks_need_gc(beansdb_servers):
    rs = []
    for server in beansdb_servers:
        disks = get_disks_info(server)
        for d, v in disks.iteritems():
            if DISK_FREE_SIZE_THRESHHOLD_MIN < v['free_size'] < DISK_FREE_SIZE_THRESHHOLD_MAX:
                rs.append((server, d, v['buckets'], v['free_size']))
    return sorted(rs, key=lambda x: x[-1])


def gc_disk(gc_record, disk_info, debug=False):
    server, disk, buckets, free_size = disk_info
    if server in IGNORED_SERVERS:
        return False
    url = BUCKET_URL_PATTERN % server
    buckets_info = get_url_data(url)['buckets']
    time_str = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    for bucket_id in buckets:
        bucket_id_hex = '%x' % bucket_id
        bucket_info = buckets_info[str(bucket_id)]
        start_id, stop_id = get_gc_range(server, bucket_info, bucket_id_hex)
        if (stop_id - start_id) >= GC_DATA_NUMBER_MIN:
            if debug:
                print server, disk, free_size, bucket_id_hex, start_id, stop_id
                return False
            else:
                gc_record.add_record(
                    time_str, server, disk, free_size,
                    bucket_id_hex, start_id, stop_id)
                gc_bucket(server, bucket_id_hex, start_id, stop_id)
                return True
    return False


def get_url_data(url):
    content = urllib.urlopen(url).read()
    return json.loads(content)


def get_gc_range(server, bucket_info, bucket):
    bucket_info = sorted(bucket_info, reverse=True)
    start_id = None
    stop_id = None

    for path, size, mtime in bucket_info:
        if is_data_need_gc(server, bucket, size, mtime):
            file_id = get_data_file_id(path)
            stop_id = stop_id or file_id
            start_id = file_id
        else:
            if start_id is None:
                continue
            else:
                break

    return start_id - 1, stop_id


def is_data_need_gc(server, bucket, size, mtime):
    MIN_DATA_SIZE_NOT_GC = 4000000000
    backup_server = get_backup_server(bucket)
    size_flag = size >= MIN_DATA_SIZE_NOT_GC

    today = datetime.date.today()
    days_ago = today - datetime.timedelta(days=60)
    min_ts = time.mktime(days_ago.timetuple())
    today_ts = time.mktime(today.timetuple())
    if backup_server == server:
        mtime_flag = (min_ts <= mtime <= today_ts)
    else:
        mtime_flag = (min_ts <= mtime)

    return (size_flag and mtime_flag)


def get_data_file_id(path):
    return int(os.path.basename(path).split('.')[0])


def get_backup_server(bucket):
    return get_backup_config(cluster='db').get('buckets', bucket)


def gc_bucket(server, bucket, start_id, stop_id):
    cmd = 'gc @%s %s %s' % (bucket, start_id, stop_id)
    rs = beansdb_remote_cmd(server, str(cmd))
    assert rs == 'OK'


def beansdb_remote_cmd(server, cmd, timeout=None):
    port = 7900  # only for doubandb
    logger.info('server=%s, cmd=[%s]', server, cmd)
    t = telnetlib.Telnet(server, port)
    t.write('%s\n' % cmd)
    out = t.read_until('\n', timeout=timeout)
    t.write('quit\n')
    t.close()
    rs = out.strip('\r\n')
    logger.info(rs)
    return rs


if __name__ == '__main__':
    main()

