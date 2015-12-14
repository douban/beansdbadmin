# coding: utf-8
from flask import Flask
from flask import render_template as tmpl

from beansdbadmin.tools.gc import GCRecord, SQLITE_DB_PATH, update_gc_status
from beansdbadmin.tools.server import (
    get_all_server_stats, get_all_buckets_key_counts, get_all_buckets_stats
    )

app = Flask(__name__)


@app.route('/')
@app.route('/index/')
def index():
    return tmpl('index.html')


@app.route('/gc/')
def gc():
    gc_record = GCRecord(SQLITE_DB_PATH)
    update_gc_status(gc_record)
    records = gc_record.get_all()
    return tmpl('gc.html', gc_records=sorted(records, reverse=True))

@app.route('/servers/')
def servers():
    server_infos = get_all_server_stats()
    ss = [s.summary_server() for s in server_infos]
    return tmpl('servers.html', servers=ss)

@app.route('/buckets/')
def buckets():
    server_buckets = get_all_buckets_stats(1)
    return tmpl('buckets.html', server_buckets=server_buckets)

@app.route('/sync/')
def sync():
    bs = get_all_buckets_key_counts(16)
    return tmpl('sync.html', buckets=bs)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p", "--port", type=int, default=5000,
        help="beansdbadmin agent port number."
    )
    args = parser.parse_args()
    app.run(debug=False, host="0.0.0.0", port=args.port)


if __name__ == '__main__':
    main()
