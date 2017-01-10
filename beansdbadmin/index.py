# coding: utf-8
from flask import Flask
from flask import render_template as tmpl

from beansdbadmin.tools.gc import GCRecord, SQLITE_DB_PATH, update_gc_status
from beansdbadmin.models.server import (
    get_all_server_stats, get_all_buckets_key_counts, get_all_buckets_stats)
from beansdbadmin.models.proxy import Proxies
import beansdbadmin.config as config

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
    server_buckets = get_all_buckets_stats(2)
    return tmpl('buckets.html', server_buckets=server_buckets)


@app.route('/sync/')
def sync():
    bs = get_all_buckets_key_counts(256)
    #bs = get_all_buckets_key_counts(256 if config.cluster=="fs" else 16)
    return tmpl('sync.html', buckets=bs)


def generate_proxies(is_online):
    if is_online:
        return Proxies()
    else:
        return Proxies()


def process_proxies(is_online):
    proxies = generate_proxies(is_online)
    stats = proxies.get_stats()
    scores_summary = proxies.get_scores_summary()
    return tmpl('proxies.html',
                stats=stats,
                scores=scores_summary,
                is_online=is_online)


def process_scores(server, is_online):
    proxies = generate_proxies(is_online)
    proxy_list = sorted(proxies.proxies, key=lambda x: x.host)
    scores = proxies.get_scores(server)
    arcs = proxies.get_arcs(server)
    buckets_avg = {}
    for k, hosts in arcs.iteritems():
        avgs = {}
        count = 0
        for key, value in hosts.iteritems():
            count += 1
            for host, arc in value:
                sum = avgs.get(host, 0)
                sum += int(arc)
                avgs[host] = sum
        for host, arc in avgs.iteritems():
            avgs[host] = arc / count
        buckets_avg[k] = avgs
    return tmpl('scores.html',
                server=server,
                proxy_list=proxy_list,
                arcs=arcs,
                buckets_avg=buckets_avg,
                scores=scores)


@app.route('/proxies/')
def db_proxies():
    return process_proxies(is_online=True)


@app.route('/offline_proxies/')
def db_offline_proxies():
    return process_proxies(is_online=False)


@app.route('/score/<server>/')
def server_scores(server):
    return process_scores(server, is_online=True)


@app.route('/offline_score/<server>/')
def offline_server_scores(server):
    return process_scores(server, is_online=False)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-p",
                        "--port",
                        type=int,
                        default=5000,
                        help="beansdbadmin agent port number.")
    parser.add_argument(
        "--cluster",
        required=True,
        choices=['db', 'fs', 'db256', 'test'],
        help="cluster name, will use zk config in /beansdb/<cluster>")
    args = parser.parse_args()
    config.cluster = args.cluster
    app.run(debug=True, host="0.0.0.0", port=args.port)


if __name__ == '__main__':
    main()
