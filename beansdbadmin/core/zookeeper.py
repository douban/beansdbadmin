

from kazoo.client import KazooClient
import pickle
import json
import logging


logging.getLogger("kazoo").setLevel(logging.WARN)

logger = logging.getLogger(__name__)


def get_zkservers():
    return 'zk1:2181,zk2:2181,zk3:2181,zk4:2181,zk5:2181'


class ZK(object):

    def __init__(self, cluster, zkservers=None):
        if not zkservers:
            zkservers = get_zkservers()
        self.cluster = cluster
        self.zkpath = "/beansdb/%s" % cluster
        self.zk = KazooClient(hosts=zkservers)
        self.zk.start()
        self.zk.ensure_path(self.zkpath)

    def _path_route(self):
        """store curr version.
           children store route.yaml`s in sequence, start with routes_
        """
        return "%s/route" % self.zkpath

    def _path_gcs(self):
        return "%s/gc" % self.zkpath

    def _path_gc_host(self, host):
        return "%s/%s" % (self._path_gcs(), host)

    def _path_gc_bucket(self, host, bucket_str):
        return "%s/%s" % (self._path_gc_host(host), bucket_str)

    def _path_backup(self):
        return "%s/backup" % self.zkpath

    def _path_proxy(self):
        return "%s/proxy" % self.zkpath

    def _path_servers(self):
        return "%s/servers" % self.zkpath

    def _path_disks(self):
        return "%s/disks" % self.zkpath

    def _path_disk(self, host):
        return "%s/%s" % (self._path_disks(), host)

    def _path_jobs(self):
        """store rerouting job
           children store jobs in pickle
        """
        return "%s/jobs" % self.zkpath

    def _path_job(self, key):
        return "%s/%s" % (self._path_jobs(), key)

    def path_jobs(self):
        return self._path_jobs()

    def path_job(self, key):
        return self._path_job(key)

    def _path_migrate(self):
        return "%s/migrate" % self.zkpath

    def path_migrate_status(self, host):
        return "%s/%s" % (self._path_migrate(), host)

    def path_prepared_lock(self):
        return "%s/preparedlock" % self.zkpath

    def path_migrate_lock(self):
        return "%s/migratelock" % self.zkpath

    def path_prepared_jobs(self):
        return "%s/prepared_jobs" % self.zkpath

    def path_prepared_job(self, key):
        return "%s/%s" % (self.path_prepared_jobs(), key)

    def path_err_jobs(self):
        return "%s/error_jobs" % self.zkpath

    def path_err_job(self, key):
        return "%s/%s" % (self.path_err_jobs(), key)

    def reroute_set(self, key):
        path = self._path_jobs()
        self.zk.set(path, key)

    def reroute_get(self):
        path = self._path_jobs()
        curr = self.zk.get(path)[0]
        return curr

    def reroute_clear(self):
        self.reroute_set("None")

    def all_server_set(self, content):
        path = self._path_servers()
        self.zk.ensure_path(path)
        self.zk.set(path, json.dumps(content))

    def all_server_get(self):
        if not self.zk.exists(self._path_servers()):
            return []
        raw = json.loads(self.zk.get(self._path_servers())[0])
        return [host.encode('utf-8') for host in raw]

    def disk_info_set(self, host, content={}):
        """
        store host's disk info
        """
        data, _ = self._ensure_zk_path(self._path_disks())
        disk_info = json.loads(data)
        disk_info[host] = content
        self.zk.set(self._path_disks(), json.dumps(disk_info))
        return disk_info

    def disk_info_get(self, host):
        data, _ = self._ensure_zk_path(self._path_disks())
        disk_info = json.loads(data)
        return disk_info.get(host)

    def _ensure_zk_path(self, path):
        if not self.zk.exists(path):
            self.zk.ensure_path(path)
            self.zk.set(path, json.dumps({}))
        return self.zk.get(path)

    def migrate_status_get(self, host):
        path = self.path_migrate_status(host)
        status, _ = self.zk.get(path)
        return status

    def migrate_status_set(self, host, status):
        path = self.path_migrate_status(host)
        self.zk.ensure_path(path)
        self.zk.set(path, status)

    def route_set(self, content, commit=False):
        path = self._path_route()
        res = self.zk.create(path + '/route_', content, sequence=True)
        ver = int(res[-10:])
        if commit:
            self.route_version_set(ver)
        return ver

    def route_get(self, ver=-1):
        path = self._path_route()
        if ver < 0:
            ver = int(self.zk.get(path)[0])
        return self.zk.get(path + "/route_%010d" % ver)[0]

    def route_version_set(self, ver):
        path = self._path_route()
        self.zk.set(path, str(ver))

    def route_version_get(self):
        path = self._path_route()
        return int(self.zk.get(path)[0])

    def route_verison_get_all(self):
        path = self._path_route()
        vers = self.zk.get_children(path)
        return sorted([int(r[-10:]) for r in vers])

    def route_verison_get_newest(self):
        return max(self.route_verison_get_all())

    def route_watch(self, func):
        path = self._path_route()
        self.zk.DataWatch(path)(func)

    def gc_get(self):
        buckets = self.zk.get_children(self._path_gcs())
        return dict([(b, self.zk.get(self._path_gc(b))[0]) for b in buckets])

    def gc_set(self, buckets, state):
        """ gc cron set busy and idle
            migrate cron set block and idle"""
        assert state in ("busy", "idle", "block")
        paths = [self._path_gc(bucket) for bucket in buckets]
        for p in paths:
            self.zk.ensure_path(p)
        if state == "block":
            busy = [p for p in paths if self.zk.get(p) == "busy"]
            if len(busy) > 0:
                return busy
        for p in paths:
            self.zk.set(p, state)

    def gc_set_bucket(self, host, bucket, state):
        assert state in ("busy", "idle", "block")
        path = self._path_gc_bucket(host, bucket)
        self.zk.ensure_path(path)
        if state == "block":
            busy = path if self.zk.get(path) == "busy" else ""
            if busy:
                return busy
        self.zk.set(path, state)

    def gc_get_bucket(self, host, bucket):
        path = self._path_gc_bucket(host, bucket)
        return self.zk.get(path)[0]

    def gc_get_status(self, host):
        path = self._path_gc_host(host)
        if self.zk.exists(path):
            return self.zk.get_children(path)

    def gc_unblock_bucket(self, host, bucket):
        stats = self.gc_get_bucket(host, bucket)
        if stats == 'block':
            self.gc_set_bucket(host, bucket, 'idle')

    def gc_unblock(self):
        keys = self.job_list()
        buckets = set([key.split("_")[-1] for key in keys])

        to_unblock = []
        stats = self.gc_get()
        for b, s in stats.items():
            if s == 'block' and s not in buckets:
                to_unblock.append(b)
        if len(to_unblock):
            logger.info('unblock gc: %s', to_unblock)
            self.gc_set(to_unblock, 'idle')

    def proxies_get(self):
        data, _ = self.zk.get(self._path_proxy())
        return json.loads(data)

    def proxies_set(self, addrs):
        path = self._path_proxy()
        self.zk.set(path, json.dumps(addrs))

    def backup_get(self):
        data, _ = self.zk.get(self._path_backup())
        return json.loads(data)

    def backup_set(self, dic):
        self.zk.set(self._path_backup(), json.dumps(dic))

    def job_get(self, key):
        return pickle.loads(self.zk.get(self._path_job(key))[0])

    def job_delete(self, key):
        self.zk.delete(self._path_job(key))

    def job_set(self, key, job):
        path = self._path_job(key)
        self.zk.set(path, pickle.dumps(job))

    def job_create(self, key, job):
        path = self._path_job(key)
        self.zk.ensure_path(self.path_jobs())
        self.zk.create(path, pickle.dumps(job))

    def job_exist(self, key):
        return self.zk.exists(self._path_job(key))

    def job_list(self):
        return self.zk.get_children(self._path_jobs())

    def prepared_job_set(self, key, job):
        path = self.path_prepared_job(key)
        self.zk.ensure_path(self.path_prepared_jobs())
        self.zk.create(path, pickle.dumps(job))

    def prepared_job_get(self, key):
        return pickle.loads(self.zk.get(self.path_prepared_job(key))[0])

    def prepared_job_delete(self, key):
        self.zk.delete(self.path_prepared_job(key))

    def prepared_job_exist(self, key):
        return self.zk.exists(self.path_prepared_job(key))

    def err_job_set(self, key, job):
        path = self.path_err_job(key)
        self.zk.ensure_path(self.path_err_jobs())
        self.zk.create(path, pickle.dumps(job))

    def err_job_get(self, key):
        return pickle.loads(self.zk.get(self.path_err_job(key))[0])

    def err_job_delete(self, key):
        self.zk.delete(self.path_err_job(key))

    def err_job_exist(self, key):
        return self.zk.exists(self.path_err_job(key))

