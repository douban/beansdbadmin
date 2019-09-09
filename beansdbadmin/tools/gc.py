#!/usr/bin/env python
# coding: utf-8

import time
import logging
import sqlite3
import getpass
from pprint import pprint
from beansdbadmin.core.server_info import (get_http, get_bucket_all, get_du)
from beansdbadmin.tools.logreport import send_sms
from beansdbadmin.tools.filelock import FileLock
from beansdbadmin import config

logger = logging.getLogger('gc')
LOG_FORMAT = '%(asctime)s-%(name)s-%(levelname)s-%(message)s'
if getpass.getuser() in ("beansdb", "root"):
    LOG_FILENAME = '/var/log/beansdb-admin/gc.log'
    SQLITE_DB_PATH = '/opt/beansdbadmin/beansdbadmin/gobeansdb-gc.db'
    logging.basicConfig(filename=LOG_FILENAME,
                        level=logging.INFO,
                        format=LOG_FORMAT)
else:
    SQLITE_DB_PATH = './gobeansdb-gc.db'
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

DISK_GC = (400 << 30)

# gc record database


def get_buckets(s):
    try:
        return get_bucket_all(s)
    except Exception as e:
        logging.info("get buckets failed for %s" % s)
        logging.exception(e)
        return []


def get_disks(s):
    try:
        return get_du(s)
    except Exception as e:
        logging.info("get disks failed for %s" % s)
        logging.exception(e)
        return {}


class GCRecord(object):
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.cursor = self.conn.cursor()

    def create_table(self):
        self.cursor.execute("""CREATE TABLE gc_record (
                            id INTEGER PRIMARY KEY,
                            server TEXT,
                            bucket TEXT,
                            start_time TEXT,
                            stop_time TEXT,
                            start_id INTEGER,
                            stop_id INTEGER,
                            curr_id INTEGER,
                            size_released INTEGER,
                            size_broken INTEGER,
                            status TEXT)
                            """)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def add(self, server, bucket, start_time, stop_time, start_id, stop_id,
            curr_id, size_released, size_broken, status):

        logging.debug("insert %s %s %s", server, bucket, start_time)
        self.cursor.execute(
            """INSERT INTO gc_record
                            (server, bucket, start_time, stop_time,
                            start_id, stop_id, curr_id, size_released,
                            size_broken, status)
                            VALUES
                            (:server, :bucket, :start_time, :stop_time,
                            :start_id, :stop_id, :curr_id, :size_released,
                            :size_broken, :status)
                            """, {
                'server': server,
                'bucket': bucket,
                'start_time': start_time,
                'stop_time': stop_time,
                'start_id': start_id,
                'stop_id': stop_id,
                'curr_id': curr_id,
                'size_released': size_released,
                'size_broken': size_broken,
                'status': status
            })
        self.conn.commit()

    def update(self, id, stop_time, curr_id, size_released, size_broken,
               status):
        logging.debug("update_rec %s %s", id, status)
        self.cursor.execute(
            """UPDATE gc_record SET status = :status, stop_time = :stop_time,
            curr_id = :curr_id,
            size_released = :size_released,
            size_broken = :size_broken WHERE id = :id
            """, {
                'status': status,
                'id': id,
                'stop_time': stop_time,
                'curr_id': curr_id,
                'size_released': size_released,
                'size_broken': size_broken
            })
        self.conn.commit()

    def update_status(self, id, status):
        logging.debug("update status %s %s", id, status)
        stop_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
        self.cursor.execute(
            """UPDATE gc_record SET status = :status,
                            stop_time = :stop_time WHERE id = :id
                            """, {
                'status': status,
                'id': id,
                'stop_time': stop_time
            })
        self.conn.commit()

    def get_all(self, num=256 * 3 * 2):
        self.cursor.execute("SELECT * FROM gc_record")
        return self.cursor.fetchall()


def get_servers(exclude):
    exclude.extend(["chubb2", "chubb3"])
    servers = config.get_servers()[0]
    servers = [s for s in servers if s not in exclude]
    return servers


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-c",
                        "--cluster",
                        required=True,
                        choices=['db256', 'test'],
                        help="cluster of beansdb")
    parser.add_argument('-d',
                        '--debug',
                        action='store_true',
                        help="Debug mode, send gc cmd to terminal.")
    parser.add_argument('-i',
                        '--init',
                        action='store_true',
                        help="Init database.")
    parser.add_argument('-q',
                        '--query',
                        action='store_true',
                        help="Query the running buckets.")
    parser.add_argument('-u',
                        '--update-status',
                        action='store_true',
                        help="Update the status of gc.")
    args = parser.parse_args()

    gc_record = GCRecord(SQLITE_DB_PATH)
    if args.init:
        gc_record.create_table()
        return

    if args.query:
        pprint(gc_record.get_all())
        return
    config.cluster = args.cluster  # use for get_servers_from_zk

    with FileLock(SQLITE_DB_PATH, timeout=10):
        if args.update_status:
            update_gc_status(gc_record)
            return
        choose_one_bucket_and_gc_it(args.debug)


def choose_one_bucket_and_gc_it(debug=False):
    servers = get_servers(config.IGNORED_SERVERS)
    disks = []
    buckets = []
    for s in servers:
        for bkt in get_buckets(s):
            bkt_id = bkt["ID"]
            gcing = (bkt["HintState"] >= 4)
            if gcing:
                if not debug:
                    logging.info("%s %s is gcing", s, bkt_id)
                    return

        for disk, disk_info in get_disks(s)["Disks"].iteritems():
            disk_free = disk_info["Free"]
            disk_buckets = disk_info["Buckets"]
            if DISK_GC > disk_free > 0:
                disks.append((s, disk_free, disk_buckets))

    if not disks:
        return
    disks.sort(key=lambda x: x[1])
    gc_disk = disks[0]
    block_buckets = config.gc_block_buckets(gc_disk[0])
    for bucket in gc_disk[-1]:
        bkt = '{:02x}'.format(bucket)
        if block_buckets and bkt in block_buckets:
            continue
        bucket_gc_files = get_gc_files(gc_disk[0], bucket)
        if bucket_gc_files:
            buckets.append((gc_disk[0], bucket, bucket_gc_files))

    if not buckets:
        msg = "server %s:beansdb takes too much disk space \
               and is not cleard when autogc" % gc_disk[0]
        logging.error(msg)
        send_sms(msg)
        return
    buckets.sort(key=lambda x: x[-1], reverse=True)
    server, bucket, _ = buckets[0]
    gc_bucket(server, bucket, debug)


def get_status(gc):
    status = gc["Err"]
    curr_id = gc["Src"]
    stop_id = gc["End"]
    if not status:
        if gc["Running"]:
            status = "running"
        elif curr_id > stop_id:
            status = "success"
        else:
            status = "abort"
    return status


def update_rec(db, bid, gc):
    db.update(
        bid,
        gc["EndTS"][:19],
        gc["Src"],
        gc["SizeReleased"],
        gc["SizeBroken"],
        get_status(gc),
    )


def insert_rec(db, server, bucket, gc):
    db.add(
        server,
        bucket,
        gc["BeginTS"][:19],
        gc["EndTS"][:19],
        gc["Begin"],
        gc["End"],
        gc["Src"],
        gc["SizeReleased"],
        gc["SizeBroken"],
        get_status(gc),
    )


def update_gc_status(db):
    servers = get_servers([])
    indb = get_most_recent_for_buckets(db)
    online = get_gc_stats_online(servers)

    for bkt, old in indb.items():
        if bkt not in online:
            if old[-1] == "running":
                db.update_status(old[0], "lost")

    for bkt, new in online.items():
        old = indb.get(bkt)
        if old is None:
            insert_rec(db, bkt[0], bkt[1], new)
        elif old[3] != new["BeginTS"][:19]:
            if old[-1] == "running":
                db.update_status(old[0], "coverted")
            insert_rec(db, bkt[0], bkt[1], new)
        else:
            if old[-1] == "running":
                update_rec(db, old[0], new)


def get_most_recent_for_buckets(db):
    records = db.get_all()
    records.sort()
    buckets = dict()
    for r in records:
        key = (r[1], int(r[2]))  # (server, bucket id)
        buckets[key] = r
    return buckets


def get_gc_stats_online(servers):
    buckets = dict()
    for s in servers:
        try:
            for bkt in get_buckets(s):
                bkt_id = bkt["ID"]
                lastgc = bkt["LastGC"]
                if lastgc:
                    buckets[(s, bkt_id)] = lastgc
        except Exception:
            pass
    return buckets


def get_gc_files(server, bucket):
    res = get_http(server, "/gc/%02x" % int(bucket))
    start, end, ok = parse_gc_resp(res)
    if ok:
        return end - start + 1
    else:
        return 0


def gc_bucket(server, bucket, debug=True):
    if debug:
        print "pretend gc %s %s" % (server, bucket)
        res = get_http(server, "/gc/%x" % int(bucket))
        print res
        return
    res = get_http(server, "/gc/%x?run=true" % int(bucket))
    _, _, ok = parse_gc_resp(res)
    if not ok:
        logging.error("gc %s %s: %s", server, bucket, res)
    else:
        logging.info("gc %s %s: %s", server, bucket, res)


def get_sentence(s, key):
    left = s.find(key)
    if left < 0:
        return -1
    s = s[left + len(key):]
    right = s.find(",")
    if right < 0:
        return
    try:
        n = int(s[:right])
        return n
    except Exception:
        return -3


def parse_gc_resp(resp):
    err = resp.find("err")
    start = get_sentence(resp, "start")
    end = get_sentence(resp, "end")
    ok = (err < 0 and start >= 0 and end >= start)
    return start, end, ok


def test_parse_gc_resp():
    success = "<a href='/bucket/2'> /bucket/2 </a> <p/><p/> \
               bucket 2, start 197, end 201, merge false, pretend false <p/>"

    err = "<p> err : already running </p><a href='/gc/2'> 2 </a> <p/>"
    r = parse_gc_resp(success)
    if r != (197, 201, True):
        print r
        return
    r = parse_gc_resp(err)
    if r != (-1, -1, False):
        print r
        return
    print "ok"


if __name__ == '__main__':
    # test_parse_gc_resp()
    main()
