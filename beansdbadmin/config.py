# coding: utf-8
from beansdbadmin.core.zookeeper import ZK
from beansdbadmin.core.route import Route
try:
    from beansdbadmin.local_config import IGNORED_SERVERS
except ImportError:
    IGNORED_SERVERS = []


OFFLINE_PROXIES = ['doubandbofflineproxy1:7905', 'doubandbofflineproxy2:7905']
zk = None
cluster = "test"


def get_servers():
    route = Route.from_zk(get_zk())
    addrs = route.main.keys()
    servers = [x.split(":")[0] for x in addrs]
    backups = [x.split(":")[0] for x in route.backup]
    return servers, backups


def get_proxies():
    return get_zk().proxies_get()


def gc_block_buckets(host):
    buckets = get_zk().gc_get_status(host)
    if buckets:
        buckets = [bucket.encode('utf-8') for bucket in buckets]
    return buckets


def get_zk():
    global zk, cluster
    if zk is None:
        zk = ZK(cluster)
    return zk
