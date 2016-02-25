# coding: utf-8

import libmc
from douban.utils.config import read_config

PROXY_SERVER_PORT = 7905
PROXY_WEB_PORT = 7908


class Proxy(object):

    def __init__(self, host):
        self.host = host
        self.server_addr = '%s:%s' % (host, PROXY_SERVER_PORT)
        self.web_addr = '%s:%s' % (host, PROXY_WEB_PORT)
        self.server = libmc.Client([self.server_addr])

    def get_scores(self):
        pass

    def get_stats(self):
        stats = self.server.stats()
        rs = stats[self.server_addr]
        rs['web_addr'] = self.web_addr
        rs['host'] = self.host
        return rs


class Proxies(object):

    def __init__(self, config_name='shire-online'):
        self.proxy_addrs = read_config(config_name, 'beansdb').get('proxies')
        HOST = 0
        self.proxies = [Proxy(x.split(':')[HOST]) for x in self.proxy_addrs]

    def get_stats(self):
        return [p.get_stats() for p in self.proxies]


if __name__ == '__main__':
    p = Proxies()
    print p.get_stats()
