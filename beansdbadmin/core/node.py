#!/usr/bin/env python
# encoding: utf-8

import os
import json
import logging
import subprocess

from beansdbadmin.core.client import get_url_content

ERR_RSYNC_NOT_FOUND = "rsync client not found"
ERR_RSYNC_WORKING = "rsync client runnnig"
ERR_RSYNC_NOT_DONE = "rsync client not done"
ERR_SPACE = "not enough space"


WEB_PORT_DIFF = 3
AGENT_PORT_DIFF = 2
RSYNC_PORT_DIFF = 4


logger = logging.getLogger(__name__)


class ResponsError(Exception):
    pass


class RsyncException(Exception):
    pass


def get_web_port(mc_port):
    return mc_port + WEB_PORT_DIFF


def get_agent_port(mc_port):
    return mc_port + AGENT_PORT_DIFF


def get_rsync_port(mc_port):
    return mc_port + RSYNC_PORT_DIFF


class Node(object):

    def __init__(self, addr, is_proxy=False):
        self.addr = addr
        self.host, port = addr.split(":")
        self.mc_port = int(port)
        self.is_proxy = is_proxy

    def web_client(self):
        return WebClient(self.host, get_web_port(self.mc_port))

    def rsync_client(self):
        return RsyncClient(self.host, get_rsync_port(self.mc_port))

    def agent_client(self):
        return AgentClient(self.host, get_agent_port(self.mc_port))

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return self.addr


class Client(object):

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def __repr__(self):
        return "%s:%d" % (self.host, self.port)


class HttpClient(Client):

    def get_http(self, query, log=True):
        url = 'http://%s:%d/%s' % (self.host, self.port, query)
        if log:
            logger.info(url)
        else:
            logger.debug(url)
        content = get_url_content(url)
        return content

    def get_http_json(self, query, log=True):
        ret = self.get_http(query, log)
        try:
            return json.loads(ret)
        except ValueError as e:
            logger.error("%s %s not json: %s ", self, query, ret)
            raise e


class WebClient(HttpClient):

    def get_config(self):
        v = self.get_http_json("config")
        if isinstance(v, list):
            v = v[0]
        return v

    def get_route(self):
        return self.get_http("route")

    def get_route_version(self):
        return int(self.get_http("route/version"))

    def reload_route(self, version=-1):
        ret = self.get_http("route/reload?ver=%d" % version)
        logger.info("reroute return: %s", ret)
        if not (ret.startswith('ok') or ret.startswith('warn')):
            raise ResponsError(ret)
        return

    def free_memory(self):
        self.get_http("freememory")
        logging.info("free memory by manual gc")
        return

    def lasterr_ts(self):
        "Return last ts of last ERROR OR WARN"
        logs = self.loglast()
        logs_ts = [e['TS'][:19] if e is not None else "0" for e in logs]
        return max(logs_ts[2], logs_ts[3])

    def loglast(self):
        return self.get_http_json("loglast")

    def du(self):
        return self.get_http_json("du")

    def buffer_stat(self):
        return self.get_http_json("buffers")

    def bucket_stat(self, bucket):
        return self.get_http_json("bucket/%x" % bucket, log=False)

    def bucket_all(self):
        return self.get_http_json("bucket/all")


def dec_rsync(f):
    def _(o, *args):
        v = f(o, *args)
        err = v.get('err')
        if err:
            raise Exception(v)
        return v.get('state')
    return _


class AgentClient(HttpClient):

    def disks(self):
        return self.get_http("disks")

    def buckets_files(self):
        return self.get_http("buckets")

    def buckets(self):
        return self.get_http("buckets")

    @dec_rsync
    def rsync_prepare(self, buckets, disk, size):
        bkts = ",".join(buckets)
        return self.get_http_json("rsync/prepare?buckets=%s&disk=%s&size=%s" %
                                  (bkts, disk, size))

    @dec_rsync
    def rsync_start(self, bucket, disk, size, src):
        return self.get_http_json("rsync/start/%s?disk=%s&size=%s&src=%s" %
                                  (bucket, disk, size, src))

    @dec_rsync
    def rsync_state(self, bucket, disk):
        return self.get_http_json("rsync/state/%s?disk=%s" % (bucket, disk))

    @dec_rsync
    def rsync_commit(self, bucket):
        return self.get_http_json("rsync/commit/%s" % bucket)

    @dec_rsync
    def rsync_kill(self, bucket):
        return self.get_http_json("rsync/kill/%s" % bucket)


class RsyncClient(Client):

    def rsync(self, bucket_str, path, bwlimit, drop_cache=False):
        args = ["rsync",
                "-r", "-a", "-v", "-L",
                "--bwlimit", str(bwlimit),
                "--del",
                "--progress",
                "rsync://%s:%d/beansdb/%s" %
                (self.host, self.port, bucket_str),
                path]

        if drop_cache and os.uname()[0] == "Linux":
            args.insert(1, "--drop-cache")

        log_path = '/var/log/gobeansdb/rsync_%s' % bucket_str.replace('/', '')

        args.append('> %s 2>&1' % log_path)

        logging.info(' '.join(args))
        p = subprocess.Popen(' '.join(args), shell=True)
        return p
