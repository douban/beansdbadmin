
OFFLINE_PROXIES = ['doubandbofflineproxy1:7905', 'doubandbofflineproxy2:7905']

try:
    from beansdbadmin.local_config import IGNORED_SERVERS
except ImportError:
    IGNORED_SERVERS = []
