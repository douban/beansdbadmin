#!/usr/bin/env python
# encoding: utf-8

import time
import sqlite3
from pprint import pprint

from beansdb_tools.sa.cmdb import get_hosts_by_tag
from beansdb_tools.core.server_info import (
    get_http, get_bucket_all
)

from beansdbadmin.tools.filelock import FileLock
from beansdbadmin.config import IGNORED_SERVERS
import logging

logger = logging.getLogger('gc')
LOG_FORMAT = '%(asctime)s-%(name)s-%(levelname)s-%(message)s'
import getpass
if getpass.getuser() in ("beansdb", "root"):
    LOG_FILENAME = '/var/log/beansdb-admin/gc.log'
    SQLITE_DB_PATH = '/data/beansdbadmin/gobeansdb-gc.db'
    logging.basicConfig(filename=LOG_FILENAME,
                        level=logging.INFO,
                        format=LOG_FORMAT)
else:
    SQLITE_DB_PATH = './gobeansdb-gc.db'
    logging.basicConfig(level=logging.DEBUG,
                        format=LOG_FORMAT)


DISK_FREE_SIZE_THRESHHOLD_MAX = (40<<30)


# gc record database

class GCRecord(object):

    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.cursor = self.conn.cursor()

    def create_table(self):
        self.cursor.execute(
            """CREATE TABLE gc_record (
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
            status TEXT
           )""")
        self.conn.commit()

    def close(self):
        self.conn.close()

    def add(self,
            server, bucket,
            start_time, stop_time,
            start_id, stop_id, curr_id,
            size_released, size_broken,
            status):

        logging.debug("insert %s %s %s", server, bucket, start_time)
        self.cursor.execute(
            """INSERT INTO gc_record (server, bucket,
                                 start_time, stop_time,
                                 start_id, stop_id, curr_id,
                                 size_released, size_broken,
                                 status)
           VALUES (:server, :bucket,
                  :start_time, :stop_time,
                  :start_id, :stop_id, :curr_id,
                  :size_released, :size_broken,
                  :status)
        """,
            {'server': server, 'bucket': bucket,
             'start_time': start_time, 'stop_time': stop_time,
             'start_id': start_id, 'stop_id': stop_id, 'curr_id': curr_id,
             'size_released': size_released, 'size_broken': size_broken,
             'status': status}
        )
        self.conn.commit()

    def update(self, id, stop_time, curr_id, size_released, size_broken, status):
        logging.debug("update_rec %s %s", id, status)
        self.cursor.execute(
            """UPDATE gc_record SET status = :status,
                                stop_time = :stop_time,
                                curr_id = :curr_id,
                                size_released = :size_released,
                                size_broken = :size_broken
           WHERE id = :id
        """,
            {'status': status, 'id': id, 'stop_time': stop_time, 'curr_id': curr_id,
             'size_released': size_released, 'size_broken': size_broken}
        )
        self.conn.commit()

    def update_status(self, id, status):
        logging.debug("update status %s %s", id, status)
        stop_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
        self.cursor.execute(
            """UPDATE gc_record SET status = :status,
                                stop_time = :stop_time
           WHERE id = :id
        """,
            {'status': status, 'id': id, 'stop_time': stop_time}
        )
        self.conn.commit()

    def get_all(self, num=256*3*2):
        self.cursor.execute(
            "SELECT * FROM gc_record"
        )
        return self.cursor.fetchall()

def get_servers(exclude):
    exclude.extend(["chubb2", "chubb3"])
    server_ports = get_hosts_by_tag("gobeansdb_servers")
    servers = [x.split(':')[0] for x in server_ports]
    servers = [s for s in servers if s not in exclude]
    return servers

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
    args = parser.parse_args()

    gc_record = GCRecord(SQLITE_DB_PATH)
    if args.init:
        gc_record.create_table()
        return

    if args.query:
        pprint(gc_record.get_all())
        return

    with FileLock(SQLITE_DB_PATH, timeout=10):
        if args.update_status:
            update_gc_status(gc_record)
            return
        gc_all_buckets(args.debug)


def gc_all_buckets(debug=False):
    servers = get_servers(IGNORED_SERVERS)
    buckets = []
    for s in servers:
        for bkt in get_bucket_all(s):
            bkt_id = bkt["ID"]
            gcing = (bkt["HintState"] >= 4)
            if gcing:
                if not debug:
                    logging.info("%s %s is gcing", s, bkt_id)
                    return
            #lastgc = bkt["LastGC"]
            du = bkt["DU"]
            buckets.append((s, bkt_id, bkt, du))
    buckets.sort(key=lambda x: x[-1], reverse=True)
    bkt = buckets[0]
    # pprint(buckets)
    gc_bucket(bkt[0], bkt[1], debug)


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
    #pprint(indb)
    #pprint(online)

    for bkt, old in indb.items():
        if bkt not in online:
            db.update_status(old[0], "lost")

    for bkt, new in online.items():
        old = indb.get(bkt)
        if old is None:
            insert_rec(db, bkt[0], bkt[1], new)
        elif old[3] != new["BeginTS"][:19]:
            if old[-1] == "running":
                db.update_record_status(old[0], "coverted")
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
            for bkt in get_bucket_all(s):
                bkt_id = bkt["ID"]
                lastgc = bkt["LastGC"]
                if lastgc:
                    buckets[(s, bkt_id)] = lastgc
        except:
            pass
    return buckets


def gc_bucket(server, bucket, debug=True):
    if debug:
        print "pretend gc %s %s" % (server, bucket)
        res = get_http(server, "/gc/%s" % bucket)
        print res
        return
    res = get_http(server, "/gc/%s?run=true" % bucket)
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
    except:
        return -3


def parse_gc_resp(resp):
    err = resp.find("err")
    start = get_sentence(resp, "start")
    end = get_sentence(resp, "end")
    ok = (err < 0 and start >= 0 and end >= start)
    return start, end, ok


def test_parse_gc_resp():
    success = "<a href='/bucket/2'> /bucket/2 </a> <p/><p/> bucket 2, start 197, end 201, merge false, pretend false <p/>"
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
