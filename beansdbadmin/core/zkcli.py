
import yaml
import time
import logging
import socket

from beansdbadmin.core.zookeeper import ZK
from beansdbadmin.core.node import Node, ResponsError
from beansdbadmin.core.route import Route, RouteError

RELOAD_INTERVAL_PROXY = 2
RELOAD_INTERVAL_OTHER = 2

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(module)s %(filename)s %(funcName)s:%(lineno)d %(levelname)s %(message)s',
                    datefmt='%Y%m%dT%H:%M:%S')
logger = logging.getLogger(__name__)


def check_route(zk, servers, proxies, new_ver):
    old_ver = zk.route_version_get()
    newest_ver = zk.route_verison_get_newest()
    state = "versions: (old %d new %d newest %d)" % (old_ver, new_ver, newest_ver)
    if newest_ver not in (new_ver, old_ver):
        raise RouteError("bad zk %s" % state)

    for (group, cname) in ((servers, 'servers'), (proxies, 'proxies')):
        news = []
        others = []
        for addr in group:
            ver = Node(addr).web_client().get_route_version()
            if ver != old_ver:
                if ver == new_ver:
                    news.append(addr)
                else:
                    others.append((addr, ver))
        if len(others) > 0:
            raise RouteError("%s  have wront vers %s: %s" %\
                           (cname, state, others))


def update_zk_from_file(cluster, path):
    zk = ZK(cluster)
    with open(path, 'r') as f:
        table = yaml.load(f, Loader=yaml.FullLoader)
        return zk.route_set(yaml.dump(table))


def dump_file(content, path):
    with open(path, "w") as f:
        f.write(content)


def get_servers(zk):
    servers = zk.all_server_get()
    if not servers:
        route = Route.from_zk(zk)
        addrs = route.main.keys()
        servers = [x.split(":")[0] for x in addrs]
    return servers


def force_reload_route(cluster):
    zoo = ZK(cluster)
    router = Route.from_zk(zoo)
    servers = [s + ':7900' for s in get_servers(zoo)]
    proxies = set(zoo.proxies_get())
    new_ver = zoo.route_verison_get_newest()

    logger.info('route newest version: %s' % new_ver)

    def check_new_version(webc):
        curr_ver = webc.get_route_version()
        if curr_ver != new_ver:
            raise Exception("%s route ver %d, not %d", webc, curr_ver, new_ver)

    for s in proxies:
        host = socket.gethostbyname_ex(Node(s).host)[0]
        logger.info("begin reload proxy: %s, host: %s", s, host)
        c = Node(s).web_client()
        c.reload_route(new_ver)
        logger.info("reload proxy %s, host: %s", s, host)
        time.sleep(RELOAD_INTERVAL_PROXY)
        check_new_version(c)

    for i, s in enumerate(sorted(list(servers))):
        logger.info("reload other server %d/%d: %s", i, len(servers), s)
        c = Node(s).web_client()
        try:
            c.reload_route(new_ver)
        except ResponsError as e:
            raise e
        except Exception as e:
            logger.exception("%s", e)
        time.sleep(RELOAD_INTERVAL_OTHER)
        check_new_version(c)


def print_route_state(zk):
    return zk.route_version_get(), zk.route_verison_get_all()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="dump beansdb data content",
                                     prefix_chars="-")

    parser.add_argument('cluster', choices=['db256', 'fs', 'test'])

    subparsers = parser.add_subparsers(dest="cmd", help='all commands')

    # route
    parser_route = subparsers.add_parser('route', help='get/set route')

    parser_route.add_argument('--set-version', type=int, help="set curr version")

    parser_route.add_argument('--set', help="set file $set to zk")

    parser_route.add_argument('--get', help="get config from zk to $get, use with --ver")
    parser_route.add_argument('--ver', type=int, default=-1, help="ver to fetch")

    parser_route.add_argument('--state', action='store_true', help="fetch config from zk to $fetch")
    parser_route.add_argument('--reload', help='reload route')

    # route diff
    parser_diff = subparsers.add_parser('diff', help='')

    parser_diff.add_argument('start', type=int, help="")
    parser_diff.add_argument('end', type=int, help="")
    # parser_diff.add_argument('--seq', action="store_true", help="")

    args = parser.parse_args()

    zk = ZK(args.cluster)

    cmd = args.cmd
    if cmd == "route":
        if args.get:
            data = zk.route_get(args.ver)
            dump_file(data, args.get)
            print "save ver %d to %s" % (args.ver, args.get)
        elif args.set:
            data = open(args.set).read()
            zk.route_set(data)
        elif args.state:
            pass
        elif args.set_version:
            zk.route_version_set(args.set_version)
        elif args.reload:
            force_reload_route(args.cluster)
    elif cmd == "diff":
        # zk.route_verison_get_all()
        vers = [args.start, args.end]
        routes = [((Route.from_yaml(zk.route_get(ver))), ver) for ver in vers]
        for i, (r, v) in enumerate(routes[:-1]):
            print routes[i+1][1], '-', v, ':', routes[i+1][0].diff(r)

    print print_route_state(zk)

if __name__ == "__main__":
    main()
