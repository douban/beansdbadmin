#!/usr/bin/env python
# coding:utf-8


# 过程
# 1. 扫描所有节点，确定 角色和状态
#    3个设置的主，但可能有挂的
#    2个设置的backup，也可能有挂的，同步后要被清空
#    迁移过程中可以有4个节点数据都很多
#    c版本不排除临时写到某个节点一些数据（gobeansdb不会），读的时候可以参考，不写，也不清理，也可以不理会（简单）
# 2. 发现冲突 两种方法：
#    2.1 先清理tmp
#    2.2 然后两两mirror（两个主或三个主）
# 3.发现一个冲突，马上解决
#    每次都尽可能拿到所有点数据
#    优先级： 时间戳 > 多数派别 > 正的version > 负的version
#       尽量避免误删
#
# 4. 避免错误：
#    get的过程中发现时间戳很近，留到下一轮
#    对于一个khash，get或set时发现变了，对此khash的同步终止
#       htree中的和record中 version不同（注意后端不要有bug）
#       set用cas，也是检查version
#    set时会提升version，这个不是很好。能不提升不提升（后端要支持）
# 5. log和报错
#    每个loop要做统计。
#    如果判定的比较勉强，要log下
#    严重的情况要发邮件？比如count差很多


# 对于节点挂了:
# 1. 每个loop 会先确定活着的节点， 如果中间访问抛异常，这个loop就被中断，重来
# 2. 主节点少于2个时，不做同步， sleep. 可以设置为要求3个点都在。 LEAST_PRIMARY_NUM 决定。
# 3. 如果在主不全的情况下开始的loop，每次resolve都去检查挂了的，发现活了loop重开始


# 对于迁移
# 设定：迁移bucket B 数据从节点 src 到 dst
# 问题：proxy的流量有个迁移的过程，一开始都写src，中间有的写src，有的写dst。
#      同步脚本看到会有4个节点看着像主，目前的配置可能是新的也可能是旧的，无法判断
#      proxy不写一个点了，同步脚本强行去追不好。
#      迁移会开始于某次loop过程中
# 方案：
#    dst数据载入完，切proxy前，通知或者重启同步脚本，会发现4个。
#    方案1:  sleep, 直到变成3个。 不理想：对于被迁移的点，如果还有proxy还在像它写数据，这些数据就用应该用来支持解决冲突
#    方案2： 同步脚本最好明确知道在迁移，知道src和dst，src中value只作为参考，不向src写。一直到src访问不了了。
#    方案3： proxy4份？
# 第一版：
#    可以先简单有有一个配置文件，写着迁移的bucket，src，dst。手工重启同步脚本。发现4个，但没有找到这个配置，就sleep（fallback到方案1）
#    要增加角色


# TODO:
# 1. 目前，server 列表是外面传进来的，所以除非重启无法用新的配置
# 2. 检查 libmc的返回值？

import time
import sys
import logging
import quicklz
from collections import defaultdict, Counter
from beansdbadmin.core.hash import get_vhash
from beansdbadmin.core.client import DBClient


# 节点数检查
LEAST_PRIMARY_NUM = 3
LEAST_BACKUP_NUM = 2


# 节点key数量检查
MAX_PRIMARY_DIFF = 10 << 20
MAX_BACKUP = 10 << 20
MAX_TMP = 10 << 20


# 同步限制
TS_DIFF_LARGE = 10
MAX_DIR_SIZE = 100000
MAX_DIFF_VER = 100

# log
LOG_NOTHING_HAPPEN = False

# sleep
LOOP_INTERVAL = 10
LOOP_INTERVAL_BIG = 30

# 角色
ROLE_PRIMARY = 0
ROLE_BACKUP = 1
ROLE_TMP = 2
ROLE_OTHER = 3


def is_hot(ts):
    return time.time() - ts < TS_DIFF_LARGE


def is_leaf(d):
    return not (len(d) == 16 and len([k for k in d if k.endswith('/')]) == 16)


class Copy(object):

    def __init__(self, store, ver=0, vhash=None):
        self.store = store
        self.vhash = vhash
        self.ver = ver

        self.ts = 0
        self.key = None
        self.value = None  # decompressed, but not decoded
        self.flag = 0
        self.vhash_count = 0

        if ver == -1:
            logging.info("OLD_BUG -1: %s", self)

    def __str__(self):
        return "(%s, %s, v %s, ts %s, vc %s, ver %s, key %s)" % \
            (not self.store.fail, self.store.addr, self.vhash, self.ts, self.vhash_count, self.ver, self.key)

    def __repr__(self):
        return self.__str__()

    def cmpkey(self):
        return (not self.store.fail, self.ts, self.vhash_count, abs(self.ver), self.ver > 0)

VHASH_DELETE = (1 << 17) # overflow uint16


class Conflict(object):

    def __init__(self, worker, tag, khash_str=None):
        self.worker = worker
        self.tag = tag

        # key
        self.khash_str = khash_str
        self.khash = int(khash_str, 16)
        self.keys = set()

        # values
        self.store_to_copy = dict()
        self.copies = []
        self.mc_vhash = None
        self.vhash_counter = Counter()

        # result
        self.result = None

    def add(self, copy):
        self.copies.append(copy)
        self.store_to_copy[copy.store] = copy
        if copy.ver <= 0:
            copy.vhash = VHASH_DELETE

    def set_vhash_counts(self):
        self.vhash_counter[None] = 0
        for cp in self.copies:
            if cp.store.role == ROLE_BACKUP and cp.ver < 0:
                continue
            cp.vhash_count = self.vhash_counter[cp.vhash]

    def get_vhash_counts(self):
        self.vhash_counter = Counter()
        for cp in self.copies:
            self.vhash_counter[cp.vhash] += 1

    def resolve(self):
        logging.info("begin conflict %s", self)

        if not self.get_all():
            return
        self.get_vhash_counts()
        self.set_vhash_counts()
        self.copies.sort(key=lambda x: x.cmpkey(), reverse=True)

        self.get_mc_copy()
        self.vhash_counter[self.mc_vhash] += 1

        self.set_vhash_counts()
        self.copies.sort(key=lambda x: x.cmpkey(), reverse=True)
        self.sync_all()
        SyncWorker.count += 1
        if SyncWorker.max_count > 0 and SyncWorker.count >= SyncWorker.max_count:
            sys.exit(0)

    def get_copy(self, cp, should_have):
        records = cp.store.get_records_by_khash(self.khash_str)
        if len(records) == 0:
            if should_have and cp.ver > 0:
                logging.warn("can not get record for %s from %s, %s", self.khash_str, cp.store, cp)
                return False
            else:
                return True
        (key, _, value, flag, ts, ver) = records[0]
        if cp.ver != 0 and cp.ver != -1 and abs(cp.ver) < abs(ver) and ver != cp.ver:
            logging.warn("get_all version changed %s -> %s, %s, %s", cp.ver, ver, cp.store, key)
            return

        if is_hot(cp.ts):
            logging.warn("get_all skip hot ts, %s, %s", cp.store, key)
            return

        cp.ts = ts
        cp.key = key
        cp.ver = ver
        if ver < 0:
            cp.vhash = VHASH_DELETE
            cp.value = None
        else:
            if flag & 0x10000:
                value = quicklz.decompress(value)
                flag -= 0x10000
            cp.value = value
            cp.vhash = get_vhash(value)
            cp.flag = flag
        self.keys.add(key)
        #logging.warn("record for %s from %s: %s, %d", self.khash_str, cp.store, key, cp.vhash)
        return True

    def __str__(self):
        return "%s %s %s %s" % (self.tag, self.khash_str, self.copies, self.keys)

    def get_all(self):
        servers = set()
        for cp in self.copies:
            servers.add(cp.store)
            if cp.vhash != None:
                if not self.get_copy(cp, True):
                    return

        for s in self.worker.primary_servers + self.worker.backup_servers:
            if s in servers:
                continue
            cp = Copy(s)
            self.get_copy(cp, s.role == ROLE_PRIMARY)
            self.add(cp)
        return True

    def get_mc_copy(self):
        for key in self.keys:
            mc = self.worker.mc
            if mc is None:
                return
            try:
                value, _ = mc.get_raw(key)
                self.mc_vhash = get_vhash(value)
            except Exception as e:
                logging.error("%s", e)
                self.worker.connect_mc()

    # TODO: use cas, including delete
    def sync_all(self):
        if len(self.copies) <= 1:
            logging.error("BUG: only one copy in conflict %s", self)
            return

        logging.info(" sync conflict %s", self)
        self.result = self.copies[0]
        if self.result.ver > 0:
            logging.info("  end conflict set %d %s", self.result.vhash_count, self)
            self.set_all()
        else:
            if self.result.vhash_count > 1:
                logging.info("  end conflict DELETE %s", self)
                self.delete_all()
            else:
                logging.warn("newest is delete, only one copy, set instead. %s", self)
                self.result = None
                for cp in self.copies[1:]:
                    if cp.ver > 0:
                        self.result = cp
                if self.result is not None:
                    self.set_all()
        self.clear_backups()

    def set_all(self):
        r = self.result
        max_right_ver = 0
        max_wrong_ver = 0
        ver = r.ver
        dsts = set()
        for s in self.worker.primary_servers:
            c = self.store_to_copy[s]
            if c is None:
                logging.error("BUG: %s not collected", s)
            else:
                if c.vhash != r.vhash:
                    dsts.add(s)
                    max_wrong_ver = max(max_wrong_ver, abs(c.ver))
                else:
                    max_right_ver = max(max_right_ver, abs(c.ver))

        if len(dsts) == 0:
            logging.info("all primaries same value %s", self.khash_str)
        else:
            if max_right_ver > 0 and max_wrong_ver >= max_right_ver:
                ver = max_wrong_ver + 1
                for s in self.worker.primary_servers:
                    s.set_raw(r.key, r.value, ver, r.flag, r.vhash)
            else:
                for s in dsts:
                    s.set_raw(r.key, r.value, ver, r.flag, r.vhash)

        if r.vhash != self.mc_vhash:
            try:
                self.worker.mc.set_raw(r.key, r.value, r.flag)
            except:
                pass

    def delete_all(self):
        r = self.result
        for s in self.worker.primary_servers:
            s.delete(r.key)
        try:
            self.worker.mc.delete(r.key)
        except:
            pass

    def clear_backups(self):
        for s in self.worker.backup_servers:
            try:
                for key in self.keys:
                    s.delete(key)
            except:
                pass


# DBClient 作为一个属性而非基类，为了
#   1. 重连时不需要重建 SyncClient对象， SyncClient对象可以一直持有。
#   2. 对接口做更明确的限制和定制。
class SyncClient(object):

    '''use only set_raw and delete for writing'''

    def __init__(self, bucket, addr, depth=1, role=ROLE_OTHER, pretend=True):
        # fix
        self.addr = addr
        self.bucket = bucket
        self.depth = depth
        self.pretend = pretend

        # var
        self.role = role
        self.client = None
        self.count = 0
        self.fail = False
        self.reconnect()

    def __str__(self):
        return "%s[%d]" % (self.addr, self.count)

    def __repr__(self):
        return self.__str__()

    def reconnect(self):
        self.client = DBClient(self.addr)

    def set_raw(self, key, value, ver, flag, vhash):
        logging.info("set %s %s v %d ver %d flag 0x%x", self.addr, key, vhash, ver, flag)
        if self.pretend:
            return
        if not self.client.set_raw(key, value, ver, flag):
            err = "set %s %s v %d ver %d flag 0x%x, err %s" % (self.addr, key, vhash, ver, flag,
                                                               self.client.mc.get_last_strerror())
            logging.info(err)
            raise Exception(err)

    def get_records_by_khash(self, khash_str):
        # logging.info("get khash %s from %s", khash_str, self)
        res = self.client.get_records_by_khash(khash_str)
        return res

    def get_dir(self, path):
        return self.client.get_dir(path)

    def list_dir(self, path):
        return self.client.list_dir(path)

    def delete(self, key):
        if self.role != ROLE_BACKUP:
            logging.info("DELETE %s", (self, key))
        if self.pretend:
            return
        return self.client.delete(key)

    def get_basic_info(self):
        self.fail = False
        self.count = 0
        try:
            self.count = self.client.get_bucket_keys_count(self.bucket, self.depth)
        except Exception as e:
            logging.error("get_basic_info fail %s, %s", self, e)
            self.fail = True


class SyncWorker(object):
    max_count = 0
    count = 0
    def __init__(self, bucket, primary_servers, backup_servers,
                 all_servers, depth=1, pretend=True):
        """ all_servers is from client config
            primary_servers & backup_servers from route table"""

        assert isinstance(all_servers, (set, list))
        assert isinstance(backup_servers, (set, list))
        assert isinstance(primary_servers, (set, list))
        self.formal_primaries = frozenset(primary_servers)
        self.formal_backups = frozenset(backup_servers)
        self.formal_others = frozenset((set(all_servers) - (set(primary_servers) | set(backup_servers))))
        logging.info("bucket %d, formal_primaries: %s, formal_backups: %s", bucket, primary_servers, backup_servers)
        #logging.info("formal_others: %s", self.formal_others)

        self.stores = dict() # name to SyncClient

        # alive primary
        self.primary_servers = set()
        # alive backup
        self.backup_servers = set()
        # should not have data, but does have
        self.tmp_servers = set()


        self.depth = depth
        self.bucket = int(bucket)
        self.pretend = pretend
        self.mc = None
        self.is_running = False

        self.counters = defaultdict(Counter)
        self.mc_connector = None

        self.stats = {}
        self.keys_count = 0


    def init_servers(self):
        for servers, role in zip([self.formal_primaries, self.formal_backups, self.formal_others],
                                 [ROLE_PRIMARY, ROLE_BACKUP, ROLE_OTHER]):
            for server in servers:
                self.stores[server] = SyncClient(self.bucket, server, self.depth, role, pretend=self.pretend)

    def set_mc_connector(self, mc_connector):
        self.mc_connector = mc_connector

    def connect_mc(self):
        '''this is a callbck'''
        if self.mc_connector is not None:
            self.mc = self.mc_connector()

    def incr_counter(self, dst, action):
        self.counters[dst][action] += 1

    def loop(self):
        self.init_servers()
        self.is_running = True
        while self.is_running:
            self.counters.clear()
            self.stats = {
                "time": time.time(),
            }
            try:
                self.scan_all_servers() # 重连

                if self.check_primaries() and self.check_backups() and self.check_tmps():
                    for s in self.backup_servers:
                        self.clear_backup(s)
                    self.mirror_primaries()
                else:
                    logging.info("sleep %ds", LOOP_INTERVAL_BIG)
                    time.sleep(LOOP_INTERVAL_BIG)
            except Exception, e:
                logging.getLogger().exception(e)

            self.log_status()
            if self.is_running:
                t = LOOP_INTERVAL if self.depth == 1 else LOOP_INTERVAL_BIG
                time.sleep(t)

    def check_primaries(self):
        self.primary_servers = list([self.stores.get(addr) for addr in self.formal_primaries])
        self.primary_servers.sort(key=lambda x: x.count, reverse=True)
        max_count = self.primary_servers[0].count
        for store in self.primary_servers:
            if store.fail:
                self.primary_servers.remove(store)
            if max_count - store.count > MAX_PRIMARY_DIFF:
                logging.error('primary %s has too few items %d', store.addr, store.count)
                return False
        if len(self.primary_servers) < LEAST_PRIMARY_NUM:
            logging.error('less than %d formal primary nodes alive, not sync', LEAST_PRIMARY_NUM)
            return False
        return True

    def check_backups(self):
        self.backup_servers = list([self.stores.get(addr) for addr in self.formal_backups])
        for store in self.backup_servers:
            if store.fail:
                self.backup_servers.remove(store)
            c = store.count
            if c > MAX_BACKUP:
                logging.warn("find big backup %s %d, should be migrating", store.addr, c)
                return False
        if len(self.backup_servers) < LEAST_BACKUP_NUM:
            logging.error('less than %d formal backup nodes alive, not sync', LEAST_PRIMARY_NUM)
            return False
        return True

    def check_tmps(self):
        self.tmp_servers = set()
        for server in self.formal_others:
            store = self.stores[server]
            self.tmp_servers.add(server)
            c = store.count
            if c > 0:
                if c > MAX_TMP:
                    logging.warn("find big tmp %s %d, should be migrating", server, c)
                else:
                    logging.error("find small tmp %s %d, abnormal! ", server, c)
                return False # TODO: 如果知道迁移的目标，就可以同步
        return True

    def log_status(self):
        s = self.stats
        s["time"] = "%dms" % ((time.time() - s["time"]) * 1000)
        if "keys_count" in s:
            s["keys_count"] = sorted(s["keys_count"].items(),
                                     key=lambda x: x[1], reverse=True)
        s = "; ".join(["%s:%s" % (k, s[k]) for k in sorted(s.keys())])
        if LOG_NOTHING_HAPPEN or self.counters:
            logging.info("STATS LOOP %x: %s", self.bucket, s)
        for k in sorted(self.counters.keys()):
            logging.info("STATS %s: %s", k, dict(self.counters[k]))

    def scan_all_servers(self):
        for addr, store in self.stores.items():
            if store.fail:  # reconnect
                self.stores[addr] = SyncClient(
                    store.bucket, addr, self.depth, store.role, self.pretend)
            store.get_basic_info()

    def clear_backup(self, store):
        if store.count <= 0:
            return
        logging.info('clear_backup %s count %d', store, store.count)
        _dir_g = store.list_dir("@%01x" % self.bucket)
        for khash_str, vhash, ver in _dir_g:
            logging.info('clear_backup %s %s %d %d', store, khash_str, vhash, ver)
            if not self.is_running:
                return
            if ver < 0:
                continue
            cf = Conflict(self, "TMP", khash_str)
            cf.add(Copy(store, ver, vhash))
            cf.resolve()

    def mirror_primaries(self):
        src = self.primary_servers[0]
        for dst in self.primary_servers[1:]:
            if not self.is_running:
                return
            if dst.count < 0:
                return
            if self.depth == 1:
                self.mirror(src, dst, "@%01x" % (self.bucket), True)
            else:
                logging.info('mirror2 %d %s %s', self.bucket, src, dst)
                self.mirror(src, dst, "@%02x" % (self.bucket), True)

    def mirror(self, src, dst, path, isroot=False):
        if not self.is_running:
            return
        src_dir = src.get_dir(path)
        dst_dir = dst.get_dir(path)

        if isroot:
            #logging.info('mirror1 %d %s %s', self.bucket, src, dst)
            pass
        if src_dir == dst_dir:
            if isroot:
                #logging.info('mirror1 %d %s %s:\n%s\n%s', self.bucket, src, dst, src_dir, dst_dir)
                pass
            return

        if not src_dir or not dst_dir:
            logging.error("%s either dir is empty? %s ", path,
                          (src, bool(src_dir), dst, bool(dst_dir)))
            return
        is_leaf_src = is_leaf(src_dir)
        is_leaf_dst = is_leaf(dst_dir)
        if not is_leaf_src and not is_leaf_dst:
            if (src_dir['0/'][1] - dst_dir['0/'][1]) > MAX_DIR_SIZE:
                logging.error("too many, skiped: %s %s(%s) %s(%s)", path,
                              src, src_dir['0/'][1],
                              dst, dst_dir['0/'][1])
                return
            for k in sorted(src_dir):
                if src_dir[k] != dst_dir.get(k, (0, 0)):
                    self.mirror(src, dst, path + k[0])
        elif is_leaf_src and is_leaf_dst:
            #logging.info("file2file %s, %s => %s", path, src, dst)
            self.mirror_leaf(path, src, dst, src_dir, dst_dir, True)
            self.mirror_leaf(path, dst, src, dst_dir, src_dir, False)
        elif not is_leaf_src and is_leaf_dst:
            self.mirror_nonleaf2leaf(path, src, dst, src_dir, dst_dir)
        else:
            self.mirror_nonleaf2leaf(path, dst, src, dst_dir, src_dir)

    def mirror_nonleaf2leaf(self, path, src, dst, src_dir, dst_dir):
        logging.info("dir2file %s, %s => %s", path, dst, src)
        for k in src_dir.iterkeys():
            subpath = path + k[0]
            sub_src_dir = src.get_dir(subpath)
            if is_leaf(sub_src_dir):
                self.mirror_leaf(subpath, src, dst, sub_src_dir, dst_dir, True)
            else:
                self.mirror_nonleaf2leaf(subpath, src, dst, sub_src_dir, dst_dir)

    def mirror_leaf(self, path, src, dst, src_dir, dst_dir, sync_diff_value=False):
        # logging.debug("mirror_file %s %s %s", path, src.addr, dst.addr)
        for khash_str, (src_vhash, src_ver) in src_dir.iteritems():
            dst_meta = dst_dir.get(khash_str)

            if dst_meta is None:
                if src_ver < 0:
                    continue  # both deleted
                else:
                    dst_ver, dst_vhash = 0, VHASH_DELETE
                tag = "M_MISS"
            else:
                if not sync_diff_value:
                    continue

                dst_vhash, dst_ver = dst_meta
                if ((src_vhash, src_ver) == dst_meta or
                        (src_ver < 0 and dst_ver < 0)):
                    continue

                tag = "M_VALUE"
                if src_vhash == dst_vhash:
                    tag = "M_VER"
                    continue

            cf = Conflict(self, tag, khash_str)
            cf.add(Copy(src, src_ver, src_vhash))
            cf.add(Copy(dst, dst_ver, dst_vhash))
            cf.resolve()
