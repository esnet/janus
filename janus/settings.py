import os
import yaml
import logging
from werkzeug.security import generate_password_hash


API_PREFIX = '/api'
DEFAULT_CFG_PATH = "/etc/janus/janus.conf"
DEFAULT_PROFILE_PATH = "/etc/janus/profiles"
#LOG_CFG_PATH = "/etc/janus/logging.conf"
DB_FILE = "janus_db.json"
IGNORE_EPS = []
AGENT_PORT = 5050
AGENT_PROTO = "https"
AGENT_SSL_VERIFY = False
AGENT_IMAGE = "dtnaas/agent"
log = logging.getLogger(__name__)

try:
    FLASK_DEBUG = True #os.environ['DEBUG']
except:
    FLASK_DEBUG = False

DEFAULT_PROFILE = 'default'
SUPPORTED_FEATURES = ['rdma']

# Controller will expose the following ENV VARS to containers:
# - HOSTNAME
# - CTRL_PORT
# - DATA_IFACE
# - DATA_PORTS

class JanusConfig():
    def __init__(self):
        self._dbpath = os.path.join(os.getcwd(), DB_FILE)
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
        self._qos = dict()
        self._profiles = dict()

        # base profile is merged with profiles below
        self._base_profile = {
            "privileged": False,
            "systemd": False,
            "cpu": 4,
            "mem": 8589934592,
            "affinity": "network",
            "mgmt_net": "bridge",
            "data_net": None,
            "internal_port": None,
            "ctrl_port_range": [30000,30100],
            "data_port_range": [40000,40032],
            "serv_port_range": [60000,60032],
            "features": list(),
            "volumes": list(),
            "environment": list()
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

    def get_dbpath(self):
        return self._dbpath

    def get_users(self):
        return self._users

    def get_profile(self, p, inline=False):
        if p not in self._profiles:
            raise Exception("Profile not found: {}".format(p))
        if not inline:
            return {**self._base_profile, **self._profiles[p]}
        else:
            prof = {**self._base_profile, **self._profiles[p]}
            prof['volumes'] = [{v: self._volumes[v]} for v in prof['volumes']]
            prof['features'] = [{f: self._features[f]} for f in prof['features']]
            return prof

    def get_profiles(self, inline=False):
        ret = dict()
        for prof in self._profiles:
            ret.update({prof: self.get_profile(prof, inline)})
        return ret

    def get_volume(self, v):
        return self._volumes.get(v, None)

    def get_qos(self, v):
        return self._qos.get(v, None)

    def get_feature(self, f):
        return self._features.get(f, None)

    def read_profiles(self, path=None):
        if not path:
            path = self._profile_path
        if not path:
            raise Exception("Profile path is not set")
        for f in os.listdir(path):
            entry = os.path.join(path, f)
            if os.path.isfile(entry) and (f.endswith(".yml") or f.endswith(".yaml")):
                with open(entry, "r") as yfile:
                    try:
                        data = yaml.safe_load(yfile)
                        log.info("read profile directory: {}".format(data))
                        for k,v in data.items():
                            if isinstance(v, dict):
                                if (k == "volumes"):
                                    self._volumes.update(v)

                                if (k == "qos"):
                                    self._qos.update(v)

                                if (k == "profiles"):
                                    self._profiles.update(v)

                                if (k == "features"):
                                    self._features.update(v)

                    except Exception as e:
                        raise Exception(f"Could not load configuration file: {entry}: {e}")
                    yfile.close()

cfg = JanusConfig()
