# coding: utf-8

import json
import libmc
import socket
import collections

from douban.utils.config import read_config
from beansdb_tools.utils import get_url_content

from beansdbadmin.models.utils import big_num, get_start_time, grouper


PROXY_SERVER_PORT = 7905
PROXY_WEB_PORT = 7908


class Proxy(object):

    def __init__(self, host_alias):
        self.host_alias = host_alias
        self.host = socket.gethostbyname_ex(self.host_alias)[0]
        self.server_addr = '%s:%s' % (self.host, PROXY_SERVER_PORT)
        self.web_addr = '%s:%s' % (self.host, PROXY_WEB_PORT)
        self.server = libmc.Client([self.server_addr])

    def get_info(self, name):
        url = 'http://%s/stats/%s' % ('rosa5c:7908', name)
        # url = 'http://%s/stats/%s' % (self.web_addr, name)
        data = json.loads(get_url_content(url))
        return data

    def get_score(self):
        return self.get_info('score')

    def get_stats(self):
        stats = self.server.stats()
        rs = stats.values()[0]
        rs['web_addr'] = self.web_addr
        rs['host'] = self.host
        rs['host_alias'] = self.host_alias
        rs['rusage_maxrss'] = big_num(rs['rusage_maxrss'] * 1000, 1, 2)
        rs['start_time'] = get_start_time(rs['uptime'])
        rs['get'] = big_num(rs['cmd_get'], 1, 2)
        rs['set'] = big_num(rs['cmd_set'], 1, 2)
        rs['delete'] = big_num(rs['cmd_delete'], 1, 2)
        rs['read'] = big_num(rs['bytes_written'], 1, 2)
        rs['write'] = big_num(rs['bytes_read'], 1, 2)
        return rs


class Proxies(object):

    def __init__(self, config_name='shire-online'):
        self.proxy_addrs = read_config(config_name, 'beansdb').get('proxies')
        HOST = 0
        self.proxies = [Proxy(x.split(':')[HOST]) for x in self.proxy_addrs]

    def get_stats(self):
        return [p.get_stats() for p in self.proxies]

    def get_proxy_hosts(self):
        return [p.host for p in self.proxies]

    def get_scores(self, server):
        rs = collections.defaultdict(dict)
        for p in self.proxies:
            for bkt, server_scores in p.get_score().iteritems():
                addr = '%s:7900' % (server)
                if addr in server_scores:
                    sorted_server_scores = sorted(server_scores.iteritems(),
                                                  key=lambda x: x[1],
                                                  reverse=True)
                    rs[bkt][p.host] = [
                        (s.split(":")[0], "%02d" % int(score))
                        for (s, score) in sorted_server_scores
                    ]
        return rs

    def get_scores_summary(self):
        rs = collections.defaultdict(int)
        for p in self.proxies:
            for bkt, server_scores in p.get_score().iteritems():
                for server in server_scores:
                    rs[server.split(":")[0]] += int(server_scores[server])
        return grouper(10, sorted(rs.iteritems(), key=lambda x: x[1], reverse=True))


if __name__ == '__main__':
    p = Proxies()
    print p.get_scores('rosa3g')
