import os
import profile
import logging
from functools import reduce
from werkzeug.security import generate_password_hash


API_PREFIX = '/api'
DEFAULT_CFG_PATH = "/etc/janus/janus.conf"
DEFAULT_PROFILE_PATH = "/etc/janus/profiles"
DEFAULT_DB_PATH = "/etc/janus/db.json"
#LOG_CFG_PATH = "/etc/janus/logging.conf"
IGNORE_EPS = []
AGENT_PORT = 5050
AGENT_PROTO = "https"
AGENT_SSL_VERIFY = False
AGENT_USERNAME = "admin"
AGENT_PASSWORD = "admin"
AGENT_IMAGE = "dtnaas/agent"
AGENT_AUTO_TUNE = True
log = logging.getLogger(__name__)

try:
    FLASK_DEBUG = True #os.environ['DEBUG']
except:
    FLASK_DEBUG = False

DEFAULT_PROFILE = 'default'
SUPPORTED_FEATURES = ['rdma']
SUPPORTED_IMAGES = ['dtnaas/tools',
                    'dtnaas/ofed']

REGISTRIES = {
    "wharf.es.net": {
        "auth": os.getenv("REGISTRY_AUTH")
    }
}


class JanusConfig():
    def __init__(self):
        self._db = None
        self._pm = None
        self._sm = None
        self._dbpath = None
        self._profile_path = None
        self._dry_run = False
        self._agent = False
        self._controller = False
        self.PORTAINER_URI = None
        self.PORTAINER_USER = None
        self.PORTAINER_PASSWORD = None
        self.PORTAINER_VERIFY_SSL = True

        user = os.getenv("JANUS_USER")
        pwd = os.getenv("JANUS_PASSWORD")
        if user and pwd:
            self._users = {user: generate_password_hash(pwd)}
        else:
            self._users = {
                "admin": generate_password_hash("admin"),
                "kissel": generate_password_hash("kissel")
            }

        self._features = {
            'rdma': {
                'devices': [
                    {
                        'devprefix': '/dev/infiniband',
                        'names': ['rdma_cm', 'uverbs']
                    }
                ],
                'caps': ['IPC_LOCK'],
                'limits': [{"Name": "memlock", "Soft": -1, "Hard": -1}]
            }
        }

        self._volumes = dict()
        self._base_volumes = {
            "type": "bind",
            "driver": None,
            "source": None,
            "target": None,
        }
        self._networks = dict()
        self._base_networks = {
            "driver": "bridge",
            "mode": None,
            "enable_ipv6": False,
            "ipam": dict(),
            "options": dict()
        }
        self._qos = dict()
        self._profiles = dict()
        self._post_starts = dict()

        # base profile is merged with profiles below
        self._base_profile = {
            "privileged": False,
            "systemd": False,
            "pull_image": False,
            "auto_tune": False,
            "cpu": 4,
            "memory": 8589934592,
            "affinity": "network",
            "arguments": None,
            "cpu_set": None,
            "mgmt_net": "bridge",
            "data_net": None,
            "internal_port": None,
            "ctrl_port_range": [30000,30100],
            "data_port_range": None,
            "serv_port_range": None,
            "features": list(),
            "post_starts": list(),
            "volumes": list(),
            "environment": list(),
            "qos": None,
            "tools": {
                "dtnaas/tools": ["iperf3", "iperf3_server", "escp", "xfer_test"],
                "dtnaas/ofed": ["iperf3", "iperf3_server", "ib_write_bw", "xfer_test"]
            }
        }

    @property
    def dryrun(self):
        return self._dry_run

    @property
    def is_agent(self):
        return self._agent

    @property
    def is_controller(self):
        return self._controller

    @property
    def db(self):
        return self._db

    @property
    def pm(self):
        return self._pm

    @property
    def sm(self):
        return self._sm

    def setdb(self, db, pm, sm):
        self._db = db
        self._pm = pm
        self._sm = sm

    def get_dbpath(self):
        return self._dbpath

    def get_users(self):
        return self._users

    def get_volume(self, v):
        return self._volumes.get(v, {})

    def get_qos(self, v):
        return self._qos.get(v, {})

    def get_qos_list(self):
        return self._qos

    def get_feature(self, f):
        return self._features.get(f, {})

    def get_poststart(self, key):
        return self._post_starts.get(key, {})


cfg = JanusConfig()
