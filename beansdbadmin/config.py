try:
    from beansdbadmin.local_config import IGNORED_SERVERS
except ImportError:
    IGNORED_SERVERS = []